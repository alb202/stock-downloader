from dataclasses import dataclass
from pandas import Timestamp


@dataclass
class RegressionResult:
    start: Timestamp
    end: Timestamp
    length: int
    start_ordinal: int
    end_ordinal: int
    slope: float = None
    intercept: float = None
    r_value: float = None
    p_value: float = None
    std: float = None
    stderr: float = None


@dataclass
class RegressionLines:
    line_start_ordinal_x: int
    line_end_ordinal_x: int
    line_start_x: Timestamp
    line_end_x: Timestamp
    line_start_y: float
    line_end_y: float
    line_plus_start_y: float
    line_plus_end_y: float
    line_minus_start_y: float
    line_minus_end_y: float


@dataclass
class BestCorrelation:
    date: Timestamp
    corr: float


@dataclass
class BestResults:
    symbol: str
    max_regression_days: int
    date: Timestamp
    earliest_start_date: Timestamp
    best_regression_date: Timestamp
    best_result: RegressionResult
    best_lines: RegressionLines
