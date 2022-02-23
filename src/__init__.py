__version__ = "0.1.0"

from datetime import datetime
from operator import mod
import pathlib, json, time, concurrent.futures
from statistics import linear_regression
import gspread, pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


# Define configuration constants
DIR_PATH = pathlib.Path(__file__).parent.resolve().as_posix()
CREDENTIALS_DIR = DIR_PATH + "/../credentials"
with open(DIR_PATH + "/config.json") as config:
    SHEET_KEY = json.load(config)["spreadsheet_key"]

# Start values.
# these constants define the date at which given expenses started to be recorded.
START_DATE = datetime.strptime("Mar-2020", "%b-%Y")
GROCERY_START_DATE = datetime.strptime("Aug-2021", "%b-%Y")


def parse_datetime_from_title(title: str) -> datetime | None:
    """
    Attempts to parse the date and time from a sheet title name. Titles are expected to have the
    format "%b%Y" or "%B%Y" (e.g., "Apr2020", "April2020")

    Args:
        title: the title of a worksheet

    Returns:
        The datetime if it can be parsed; otherwise `None`.
    """
    try:
        return datetime.strptime(title, "%b%Y")
    except ValueError:
        try:
            return datetime.strptime(title, "%B%Y")
        except ValueError:
            return None


def parse_money(money: str) -> float:
    """ 
    Parses a decimal value from a string representing an amount of money. Note `float` is not ideal
    for representing money values, however, it simplifies operations and is accurate enough for 
    the purpose of the program.
    """
    return float(money.strip('$').replace(',', ''))


def month_df_from_wks(wks: gspread.Worksheet) -> pd.DataFrame | None:
    """
    Gets the monthly rent data from a given worksheet. Worksheets are expected to have titles with 
    the format "%b%Y" or "%B%Y" (e.g., "Apr2020", "April2020").

    Args:
        wks: the google sheets worksheet
    
    Returns:
        A pandas dataframe containing the monthly rent data as a single row if the worksheet title
        is valid; otherwise `None`.
    """
    dt = parse_datetime_from_title(wks.title)
    if dt is None:
        return None
    
    df = pd.DataFrame({"Year": [dt.year], "Month": [dt.month]})
    [data, groceries] = wks.batch_get(["A2:B6", "L2"], major_dimension="COLUMNS")
    expenses = [parse_money(value) for value in data[1]]
    df = pd.concat([df, pd.DataFrame([expenses], columns=data[0])], axis=1)
   
    if dt >= GROCERY_START_DATE:
        df["Groceries"] = [parse_money(groceries[0][0])]

    df.index = [dt] # use datetime as index
    return df


def rent_df_from_spreadsheet() -> pd.DataFrame:
    """
    Gets rent data from the google sheets spreadsheet with a key of `SHEET_KEY` using the 
    credentials found at the directory `CREDENTIALS_DIR`. 
    
    Note: no guarantees are made about the order of the rows in the rent dataframe. To ensure the
    order of the rent data use `pd.DataFrame.sort_index(ascending=False)`, which will sort the data
    from most to least recent.

    Returns:
        A pandas dataframe containing the rent data.

    Raises:
        AssertionError: if `SHEET_KEY` constant does not exist.
        AssertionError: if `CREDENTIALS_DIR` constant does not exist
        FileNotFoundError: if `client_secret.json` is not found.
        gspread.exceptions.SpreadsheetNotFound: if `SHEET_KEY` is not valid
        gspread.exceptions.APIError: if the client receives an error code from the API
    """
    assert SHEET_KEY != None
    assert CREDENTIALS_DIR != None

    client = gspread.oauth(
        credentials_filename=CREDENTIALS_DIR + "/client_secret.json",
        authorized_user_filename=CREDENTIALS_DIR + "/user_session.json"
    )
    rent_sheet = client.open_by_key(SHEET_KEY)
    worksheets = rent_sheet.worksheets()
    
    # get the data from worksheets concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        month_futures = {executor.submit(month_df_from_wks, wks) for wks in worksheets}
        df = pd.DataFrame()
        for future in concurrent.futures.as_completed(month_futures):
            data = future.result()
            df = pd.concat([df, data])

    client.session.close()
    return df


def estimate_rent(df: pd.DataFrame, dt: datetime | None = None) -> pd.DataFrame:
    """
    Estimates the cost of rent, utilities, and other expenses given a set of previous data and a
    datetime representing the year and month. If no datetime is provided, this function uses the 
    current month.

    Args:
        df: a pandas dataframe containing the previous expense data
        dt: the datetime representing the year and month to estimate

    Returns:
        A pandas dataframe containing the estimated rent data as a single row.

    Raises:
        AssertionError: if `dt` is in the past.
    """
    if dt is None:
        dt = datetime.now()
    else:
        assert dt >= datetime.now()
    
    df = remove_outliers(df)
    df.fillna(df.mean(), inplace=True)

    estimate = pd.DataFrame({"Year": [dt.year], "Month": [dt.month]})
    for expense in df.columns[2:]:
        x = df[["Year", "Month"]]
        y = df[expense]
        model = LinearRegression()
        model.fit(x.values, y)
        estimate[expense] = model.predict([[dt.year, dt.month]])
    
    return estimate.round(2)


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes outlier values from a given dataset. Values are considered outliers if they are more
    than 3 standard deviations from the mean.

    Args:
        df: the dataset as a pandas dataframe

    Returns:
        A pandas dataframe containing the data without the outliers. 
    """
    return df[np.abs(df - df.mean()) <= (3 * df.std())]


if __name__ == "__main__":
    # time how long it takes to retrieve the dataframe
    t0 = time.time()
    df = rent_df_from_spreadsheet()
    t1 = time.time()
    print("elapsed:", t1-t0, '\n')

    print("All Rent Data:")
    print(df.sort_index(ascending=False), '\n')

    # this can be used to plot individual expense data
    energy_plt = df.pivot(index="Month", columns="Year", values="Power")
    print("Monthy Energy Costs:")
    print(energy_plt, '\n')

    print("Upcoming Rent Estimate:")
    est = estimate_rent(df)
    print(est)
  