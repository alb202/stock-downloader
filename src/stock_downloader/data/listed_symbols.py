from pandas import read_csv, read_parquet, DataFrame, to_datetime, concat
from pathlib import Path, PosixPath, WindowsPath
# import yfinance as yf
# from yfinance.exceptions import YFRateLimitError
# import json
# import time
# from tqdm.auto import tqdm


class StockSymbolDownloader:
    URL = "https://datahub.io/core/nyse-other-listings/_r/-/data/other-listed.csv"
    VALID_TEST_ISSUE = ["N"]
    VALID_FINANCIAL_STATUS = ["N"]
    FILE_NAME = "nyse_other_listed_symbols.parquet"

    def __init__(self):
        self.df = self.download_data()

    def __call__(self) -> DataFrame:
        return self.df

    def download_data(self) -> DataFrame:
        df = read_csv(self.URL).dropna(subset=["Company Name", "ACT Symbol"], how="any", axis=0)
        df = (
            df.query("`Test Issue`.isin(@self.VALID_TEST_ISSUE)")
            .query('~`ACT Symbol`.str.contains("\\\\.")', engine="python")
            .query('~`ACT Symbol`.str.contains("\\\\$")', engine="python")
        )
        df = df.rename(
            columns={
                "ACT Symbol": "symbol",
                "Company Name": "company_name",
                # 'Security Name': 'security_name',
                # 'Market Category': 'market_category',
                "ETF": "ETF",
            }
        ).reset_index(drop=True)
        df["symbol"] = df["symbol"].str.upper()
        df["company_name"] = df["company_name"].str.strip()
        return df.loc[:, ["symbol", "company_name", "ETF"]].drop_duplicates().reset_index(drop=True)

    def save_data(self, output_path: str) -> DataFrame:
        output_path = Path(output_path)

        if not output_path.exists():
            raise FileNotFoundError(f"The specified path {output_path} does not exist.")

        self.df.to_parquet(output_path / self.FILE_NAME)
