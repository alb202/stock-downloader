from pandas import DataFrame  # , to_datetime  # read_csv, read_parquet, DataFrame, to_datetime, concat

# from pathlib import Path, PosixPath, WindowsPath
import yfinance as yf

# from yfinance.exceptions import YFRateLimitError
# import json
# import time
# from tqdm.auto import tqdm
from stock_downloader.utilities import downcast_numeric_columns


class YahooFinancePriceHistory:
    """Fetch historical price data for a stock symbol using Yahoo Finance."""

    RENAME_COLUMNS = {"Stock Splits": "stock_splits"}

    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, symbol: str, interval: str = "1d", period: str = "5y") -> None:
        self.symbol = symbol
        self.interval = interval
        self.period = period

        self.data = self.get_info()

    def __call__(self) -> dict:
        return self.data.to_dict(orient="records")

    def get_info(self) -> DataFrame:
        """Fetch sector and industry information for the given symbols."""

        ticker = yf.Ticker(self.symbol.upper())
        price_data = ticker.history(period=self.period, interval=self.interval, auto_adjust=True, actions=True)
        if price_data.empty:
            return None
        price_data = (
            price_data.dropna(subset=["Open", "High", "Low", "Close", "Volume"], axis=0)
            .rename(columns=self.RENAME_COLUMNS)
            .reset_index(drop=False)
        )
        price_data["Date"] = price_data["Date"].apply(lambda x: x.tz_localize(None).strftime(self.DATE_FORMAT))

        return downcast_numeric_columns(price_data)
