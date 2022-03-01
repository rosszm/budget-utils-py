"""
This module provides functions for retreiving and manipulating rent data from a shared google 
sheets spreadsheet.
"""

__version__ = "0.1.0"


from datetime import datetime
import pathlib, time, sys, json, concurrent.futures
import gspread
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


def main() -> int:
    """ 
    Runs the main program of the budget module. This program prints out the rent data and an 
    estimate for the current month.

    Returns:
        The exit code of the program.
    """
    file_dir_path = pathlib.Path(__file__).parent.resolve().as_posix()
    client_secret = file_dir_path + "/../credentials/client_secret.json"
    with open(file_dir_path + "/config.json") as config:
        sheet_key = json.load(config)["spreadsheet_key"]
    
    # time how long it takes to retrieve the dataframe
    t0 = time.time()
    df = rent_df_from_spreadsheet(sheet_key, client_secret)
    t1 = time.time()
    print("elapsed:", t1-t0, '\n')

    print("All Rent Data:")
    print(df.sort_index(ascending=False), '\n')

    # this can be used to plot individual expense data
    for expense in df[2:]:
        expense_plt = df.pivot(index="Month", columns="Year", values=expense)
        print(f"Monthly {expense} Costs:")
        print(expense_plt, '\n')

    print("Upcoming Rent Estimate:")
    est = estimate_rent(df)
    print(est)

    return 0


def rent_df_from_spreadsheet(sheet_key: str, client_secret: str) -> pd.DataFrame:
    """
    Gets rent data from the google sheets spreadsheet with a key of `sheet_key` using the 
    credentials from the file `client_secret`.
    
    Note: no guarantees are made about the order of the rows in the rent dataframe. To ensure the
    order of the rent data use `pd.DataFrame.sort_index(ascending=False)`, which will sort the data
    from most to least recent.

    Args:
        sheet_key: the key of a google sheets spreadsheet as it appears in the URL
        client_secret: the path of the client secret JSON file

    Returns:
        A pandas dataframe containing the rent data.

    Raises:
        FileNotFoundError: if `client_secret.json` is not found.
        gspread.exceptions.SpreadsheetNotFound: if `SHEET_KEY` is not valid
        gspread.exceptions.APIError: if the client receives an error code from the API
    """
    client = gspread.oauth(credentials_filename=client_secret)
    rent_sheet = client.open_by_key(sheet_key)
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
   
    if len(groceries) > 0:
        df["Groceries"] = [parse_money(groceries[0][0])]

    df.index = [dt] # use datetime as index
    return df


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
    sys.exit(main())
