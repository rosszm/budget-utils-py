"""
This module provides functions for retrieving rent data from a shared google sheets spreadsheet.
"""

import json, logging
import concurrent.futures
from typing import Optional, Tuple
import gspread
import pandas as pd
from datetime import datetime
from budget.parsers import parse_month_datetime, parse_money

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MonthRange:
    """
    A month range from a start month (inclusive) to an end month (exclusive).
    """
    def __init__(self, start: Tuple[int, int], end: Tuple[int, int]):
        self.start = datetime(start[0], start[1], 1)
        self.end = datetime(end[0], end[1], 1)

    def __repr__(self) -> str:
        return "({}, {})".format(self.start, self.end)

    def contains(self, dt: datetime) -> bool:
        return self.start <= dt < self.end


_GROCERY_LOC_MAP = {
    MonthRange((2020, 3), (2020, 8)): "B7:D7",
    MonthRange((2020, 9), (2021, 2)): "F7",
    MonthRange((2021, 2), (2021, 3)): "M7",
    MonthRange((2021, 3), (2021, 4)): "F7",
    MonthRange((2021, 4), (2021, 5)): "M2",
    MonthRange((2021, 8), (2022, 5)): "L2",
    MonthRange((2022, 5), (2022, 9)): "D7",
    MonthRange((2022, 10), (2035, 1)): "E7",
}
""" A mapping of cell locations to the date range they are valid for. """

_SPLIT_RENT = {
    2064860783,
    944604678,
    74019485,
}
""" A set of sheet ids where the rent value is total_rent/num_residents. """

_EXCLUDED_SHEETS = {
    "Aug2020",
    "Sept2022"
}
""" A set of sheets that will be excluded from processing. """


def get_spreadsheet_dataframe(spreadsheet_id: str, client_secret: str) -> pd.DataFrame:
    """
    Gets rent data from the google sheets spreadsheet with a key of `sheet_key` using the
    credentials from the file `client_secret`.

    Note: no guarantees are made about the order of the rows in the final dataframe.

    Args:
        spreadsheet_id: the key of a google sheets spreadsheet as it appears in the URL
        client_secret: the path of the client secret JSON file
        sheet_ids: the set of sheets to query

    Returns:
        A pandas dataframe containing the rent data.

    Raises:
        FileNotFoundError: if `client_secret.json` is not found.
        gspread.exceptions.SpreadsheetNotFound: if `SHEET_KEY` is not valid
        gspread.exceptions.APIError: if the client receives an error code from the API
    """
    logger.info(f"Attempting to connect to google")
    client = gspread.oauth(credentials_filename=client_secret)
    logger.info(f"Client connection established, id: {client.auth.client_id}")

    spreadsheet = client.open_by_key(spreadsheet_id)
    sheets = [s for s in spreadsheet.worksheets() if s.title not in _EXCLUDED_SHEETS]

    logger.info(f"Fetching sheets from {spreadsheet_id}")
    # get the data from worksheets concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        month_futures = {executor.submit(get_sheet_dataframe, s) for s in sheets if s.title}
        df = pd.DataFrame()
        for future in concurrent.futures.as_completed(month_futures):
            data = future.result()
            df = pd.concat([df, data])

    client.session.close()
    logger.info(f"Client connection closed, id: {client.auth.client_id}")
    return df

def get_sheet_dataframe(sheet: gspread.Worksheet) -> Optional[pd.DataFrame]:
    """
    Gets the monthly rent data from a given sheet/tab. Sheets are expected to have titles with
    the format "%b%Y" or "%B%Y" (e.g., "Apr2020", "April2020").

    Args:
        sheet: the spreadsheet sheet/tab

    Returns:
        A pandas dataframe containing the monthly rent data as a single row if the worksheet title
        is valid; otherwise `None`.
    """
    EXPENSES_A1 = "A2:B6"
    RESIDENTS_A1 = "B1:E1"

    dt = parse_month_datetime(sheet.title)
    if dt is None:
        return None

    groceries_a1 = get_grocery_a1(dt)
    result = sheet.batch_get([EXPENSES_A1, RESIDENTS_A1, groceries_a1])
    if len(result) == 2:
        [expenses, residents] = result
        groceries = []
    elif len(result) == 3:
        [expenses, residents, groceries] = result
    else:
        return None

    data = {
        "id": sheet.id,
        "year": [dt.year],
        "month": [dt.month],
        "residents": json.dumps(residents[0]),
        "num_residents": len(residents[0]),
    }
    data.update({exp[0].lower(): parse_money(exp[1]) for exp in expenses if len(exp) == 2})

    if sheet.id in _SPLIT_RENT:
        data["rent"] = data["rent"] * data["num_residents"]

    if len(groceries) > 0:
        parsed = map(parse_money, groceries[0])
        data["groceries"] = sum(parsed)

    df = pd.DataFrame(data)
    df.set_index(["id"], inplace=True)

    logger.info(f"Loaded data from sheet {sheet.id}")
    return df

def get_grocery_a1(dt: datetime) -> Optional[str]:
    """
    Returns the A1 notation for the grocery values based on a given datetime.
    """
    for dt_range in _GROCERY_LOC_MAP:
        if dt_range.contains(dt):
            return _GROCERY_LOC_MAP[dt_range]
    return None
