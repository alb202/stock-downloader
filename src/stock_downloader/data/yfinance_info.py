# from pandas import read_csv, read_parquet, DataFrame, to_datetime, concat
# from pathlib import Path, PosixPath, WindowsPath
import yfinance as yf
# from yfinance.exceptions import YFRateLimitError
# import json
# import time
# from tqdm.auto import tqdm


class YahooFinanceTickerInfo:
    COLUMN_RENAME = {"52WeekChange": "_52WeekChange"}

    """Fetch sector and industry information for a stock symbol using Yahoo Finance."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.data = self.get_info()

    def __call__(self) -> dict:
        return [self.data]

    def get_info(self) -> dict:
        """Fetch sector and industry information for the given symbols."""

        ticker = yf.Ticker(self.symbol.upper())
        info = ticker.info
        # sector = info.get('sector', None)
        # industry = info.get('industry', None)
        for k, v in self.COLUMN_RENAME.items():
            info[v] = info[k]
            del info[k]
        return info  # .rename(columns=self.COLUMN_RENAME)
