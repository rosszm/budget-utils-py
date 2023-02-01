from datetime import datetime
from typing import Optional
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


def estimate_rent(df: pd.DataFrame, residents: int, dt: Optional[datetime]=None) -> pd.DataFrame:
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

    EXCLUDE_COLS = ["id", "year", "month", "residents", "num_residents"]
    expenses_cols = df.columns.difference(EXCLUDE_COLS)

    df = df.fillna(df.mean(numeric_only=True))

    estimate = pd.DataFrame({"year": [dt.year], "month": [dt.month]})
    for expense in expenses_cols:
        x = df[["year", "month", "num_residents"]]
        y = df[expense]
        model = LinearRegression()
        model.fit(x.values, y)
        estimate[expense] = model.predict([[dt.year, dt.month, residents]])

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