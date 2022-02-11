__version__ = "0.1.0"

from datetime import datetime
from decimal import Decimal
import pathlib, json, time, concurrent.futures
import gspread, pandas as pd


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
        The datetime if it can be parsed; otherwise None.
    """
    try:
        return datetime.strptime(title, "%b%Y")
    except ValueError:
        try:
            return datetime.strptime(title, "%B%Y")
        except ValueError:
            return None


def parse_money(money: str) -> Decimal:
    """ 
    Parses a decimal value from a string representing an amount of money.
    """
    return Decimal(money.strip('$').replace(',', ''))


def month_df_from_wks(wks: gspread.Worksheet) -> pd.DataFrame | None:
    """
    Gets the monthly rent data from a given worksheet. Worksheets are expected to have titles with 
    the format "%b%Y" or "%B%Y" (e.g., "Apr2020", "April2020").

    Args:
        wks: the google sheets worksheet
    
    Returns:
        A pandas dataframe containing the monthly rent data as a single row if the worksheet tile
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

    df.index = [dt] # datetime as index; this may need to change
    return df


def rent_df_from_spreadsheet() -> pd.DataFrame:
    """
    Gets rent data from the google sheets spreadsheet with a key of `SHEET_KEY` using the 
    credentials found at the directory `CREDENTIALS_DIR`.

    Returns:
        A pandas dataframe containing the rent data.

    Raises:
        AssertionError: if `SHEET_KEY` constant does not exist.
        AssertionError: if `CREDENTIALS_DIR` constant does not exist
        FileNotFoundError: if `client_secret.json` is not found.
    """
    assert SHEET_KEY != None
    assert CREDENTIALS_DIR != None

    client = gspread.oauth(
        credentials_filename=CREDENTIALS_DIR + "/client_secret.json",
        authorized_user_filename=CREDENTIALS_DIR + "/user_session.json"
    )
    rent_sheet = client.open_by_key(SHEET_KEY)
    worksheets = rent_sheet.worksheets()
    
    # get the data from each worksheet asynchronously
    with concurrent.futures.ThreadPoolExecutor() as executor:
        month_futures = {executor.submit(month_df_from_wks, wks) for wks in worksheets}
        df = pd.DataFrame()
        for future in concurrent.futures.as_completed(month_futures):
            data = future.result()
            df = pd.concat([df, data])

    client.session.close()
    return df


if __name__ == "__main__":
    t0 = time.time()
    df = rent_df_from_spreadsheet()
    t1 = time.time()
    print("elapsed:", t1-t0)
    print(df)

