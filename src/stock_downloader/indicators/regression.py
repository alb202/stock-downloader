from pandas import DataFrame, to_datetime, Timestamp, Series

# import json
# import time
import numpy as np

from tqdm.auto import tqdm
from scipy.stats import linregress

# from typing import Optional
from scipy.stats._stats_py import LinregressResult

# from dataclasses import dataclass, field
from datetime import timedelta
# from numpy import array
# import plotly.graph_objects as go

from models.data_classes import RegressionResult, RegressionLines, BestCorrelation, BestResults


def back_in_time(date: Timestamp, days: int = 200) -> Timestamp:
    """
    Returns a date that is 'days' days before the given date.
    """
    return date - timedelta(days=days)


def data_between_dates(
    df: DataFrame,
    start_date: Timestamp,
    end_date: Timestamp,
    price_column: str = "Close",
    date_column: str = "Date",
    ordinal_date_column: str = "Date_Ordinal",
) -> DataFrame:
    df.loc[:, date_column] = to_datetime(df[date_column])
    df = df.copy(deep=True).query(f"{date_column} >= @start_date").query(f"{date_column} <= @end_date")
    df.loc[:, ordinal_date_column] = df[date_column].map(Timestamp.toordinal)
    return df.sort_values(date_column).loc[:, [date_column, ordinal_date_column, price_column]].reset_index(drop=True)


def calculate_std(x: Series, y: Series, regression_result: LinregressResult | RegressionResult) -> float:
    line = regression_result.slope * x + regression_result.intercept
    return (y - line).std()


def make_regression_lines(result: LinregressResult, lower_deviation: float = 2.0, upper_deviation: float = 2.0) -> RegressionLines:
    line_start_x = result.start
    line_end_x = result.end
    line_start_ordinal_x = result.start_ordinal
    line_end_ordinal_x = result.end_ordinal
    line_start_y = result.intercept + (result.start_ordinal * result.slope)
    line_end_y = line_start_y + (result.slope * (result.end_ordinal - result.start_ordinal))

    return RegressionLines(
        line_start_ordinal_x=line_start_ordinal_x,
        line_end_ordinal_x=line_end_ordinal_x,
        line_start_x=line_start_x,
        line_end_x=line_end_x,
        line_start_y=line_start_y,
        line_end_y=line_end_y,
        line_plus_start_y=line_start_y + (result.std * upper_deviation),
        line_plus_end_y=line_end_y + (result.std * lower_deviation),
        line_minus_start_y=line_start_y - (result.std * upper_deviation),
        line_minus_end_y=line_end_y - (result.std * lower_deviation),
    )


def get_best_result(
    df: DataFrame,
    symbol: str,
    date: Timestamp,
    max_regression_days: int = 365,
    min_regression_days: int = 20,
    lower_deviation: float = 2.0,
    upper_deviation: float = 2.0,
) -> BestResults:
    start_date = back_in_time(date, days=max_regression_days)
    data = data_between_dates(df=df, start_date=start_date, end_date=date)
    x_ = data["Date"]
    y = data["Close"]
    try:
        best_pearson_r = find_best_pearson_r(x=x_, y=y, use_abs=True, min_days=min_regression_days)
        best_start_date = best_pearson_r.date
        best_result = linear_regression_between_dates(df=data, start=best_start_date, end=date, price_column="Close", date_column="Date")
        best_lines = make_regression_lines(result=best_result, lower_deviation=lower_deviation, upper_deviation=upper_deviation)

        return BestResults(
            max_regression_days=max_regression_days,
            date=date,
            earliest_start_date=start_date,
            symbol=symbol,
            best_regression_date=best_start_date,
            best_result=best_result,
            best_lines=best_lines,
        )
    except ValueError:
        # print(f"Error calculating regression for {symbol} on {date}: {e}")
        return BestResults(
            max_regression_days=max_regression_days,
            date=date,
            earliest_start_date=start_date,
            symbol=symbol,
            best_regression_date=None,
            best_result=None,
            best_lines=None,
        )


def linear_regression_between_dates(
    df: DataFrame,
    start: Timestamp,
    end: Timestamp,
    price_column: str = "Close",
    date_column: str = "Date",
) -> RegressionResult:
    df = data_between_dates(
        df,
        start_date=start,
        end_date=end,
        price_column=price_column,
        date_column=date_column,
        ordinal_date_column="Date_Ordinal",
    )
    x = df["Date_Ordinal"]
    y = df[price_column]

    result = linregress(x, y)
    std = calculate_std(x, y, result)

    return RegressionResult(
        start=start,
        end=end,
        length=(end - start).days + 1,
        start_ordinal=x.values[0],
        end_ordinal=x.values[-1],
        slope=result.slope,
        intercept=result.intercept,
        r_value=result.rvalue,
        p_value=result.pvalue,
        stderr=result.stderr,
        std=std,
    )


def find_best_pearson_r(
    x: Series,
    y: Series,
    use_abs: bool = True,
    min_days: int = 20,
) -> BestCorrelation:
    y = y.to_numpy(dtype=float)
    x_index = np.arange(len(y), dtype=float)  # use index positions
    n = len(y)

    # Reverse cumulative sums (suffix sums)
    Sx = np.cumsum(x_index[::-1])[::-1]
    Sy = np.cumsum(y[::-1])[::-1]
    Sxx = np.cumsum((x_index * x_index)[::-1])[::-1]
    Syy = np.cumsum((y * y)[::-1])[::-1]
    Sxy = np.cumsum((x_index * y)[::-1])[::-1]

    # Candidate starting indices (need at least 2 points, so 0..n-2)
    i_vals = np.arange(0, n - min_days)
    count = n - i_vals

    sum_x = Sx[i_vals]
    sum_y = Sy[i_vals]
    sum_xx = Sxx[i_vals]
    sum_yy = Syy[i_vals]
    sum_xy = Sxy[i_vals]

    num = sum_xy - (sum_x * sum_y) / count
    varx = sum_xx - (sum_x**2) / count
    vary = sum_yy - (sum_y**2) / count
    den = np.sqrt(varx * vary)

    r = np.full_like(den, np.nan, dtype=float)
    valid = den > 0
    r[valid] = num[valid] / den[valid]

    score = np.abs(r) if use_abs else r
    k = np.nanargmax(score)
    best_idx = int(i_vals[k])
    best_r = float(r[k])

    return BestCorrelation(date=x.iloc[best_idx], corr=best_r)


def run_regression_for_symbol(symbol: str, df: DataFrame, date_col: str = "Date", max_regression_days: int = 365, min_regression_days: int = 20) -> None:
    results = []
    for regression_date in tqdm(df[date_col], desc=f"Processing {symbol}"):
        results.append(get_best_result(df=df, symbol=symbol, date=regression_date, max_regression_days=min_regression_days, max_regression_days=max_regression_days))
    return DataFrame(results).sort_values(["symbol", "date"], ascending=[True, False]).dropna(axis=0, subset=["best_regression_date"])
