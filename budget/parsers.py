import re
from datetime import datetime
from typing import Optional


def parse_month_datetime(month_str: str) -> Optional[datetime]:
    """
    Attempts to parse the month as a datetime from a string. Arguments are expected to have one of
    the following formats:
    - "%b%Y" ("Apr2020")
    - "%B%Y" ("April2020")
    - "%b%Y" ("Apr 2020")
    - "%B%Y" ("April 2020")
    - "%b" ("Apr")
    - "%B%Y" ("April")

    Note that strings without a year will return a datetime for current year. The day value of the
    datetime is set to 1 for all formats.

    Example:
    ```py
        datetime.datetime.now() # result datetime(2020, 1, 1, 12, 0)
        parse_month_datetime("April") # result datetime(2020, 4, 1, 0, 0)

    ```
    Args:
        title: the title of a worksheet

    Returns:
        The datetime if it can be parsed; otherwise `None`.
    """
    formatted = re.sub(r"\s+", "", month_str, flags=re.UNICODE)
    try:
        return datetime.strptime(formatted, "%b%Y")
    except ValueError:
        pass
    try:
        return datetime.strptime(formatted, "%B%Y")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(formatted, "%b")
        dt = dt.replace(year=datetime.now().year)
        return dt
    except ValueError:
        pass
    try:
        dt = datetime.strptime(formatted, "%B")
        dt = dt.replace(year=datetime.now().year)
        return dt
    except ValueError:
        pass
    return None

def parse_money(money: str) -> Optional[float]:
    """
    Parses a decimal value from a string representing an amount of money. Note `float` is not ideal
    for representing money values, however, it simplifies operations and is accurate enough for
    the purpose of the program.

    Args:
        money: the monetary value as a string

    Returns:
        The monetary value as a float if it can be parsed; otherwise `None`.
    """
    try:
        return float(money.strip('$').replace(',', ''))
    except ValueError:
        return None