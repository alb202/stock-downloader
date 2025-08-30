from pandas import read_csv, DataFrame
from pathlib import Path #, PosixPath, WindowsPath
# import yfinance as yf
# from yfinance.exceptions import YFRateLimitError
# import json
# import time
# from tqdm.auto import tqdm


class NasdaqDownloader:

    URL="https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed-symbols.csv"
    VALID_TEST_ISSUE = ["N"]
    VALID_FINANCIAL_STATUS = ["N"]
    FILE_NAME = 'nasdaq_listed_symbols.parquet'
    COLUMN_MAPPING = {
            'Symbol': 'symbol',
            'Company Name': 'company_name',
            # 'Security Name': 'security_name',
            # 'Market Category': 'market_category',
            'ETF': 'ETF',
        }

    def __init__(self):
        self.df = self.download_data()

    def __call__(self) -> DataFrame:
        return self.df

    def download_data(self) -> DataFrame:    
        df = read_csv(self.URL).dropna(subset=['Company Name', 'Symbol'], how='any', axis=0)
        df = df.query('`Test Issue`.isin(@self.VALID_TEST_ISSUE)').query('`Financial Status`.isin(@self.VALID_FINANCIAL_STATUS)')
        df['Symbol'] = df['Symbol'].str.upper()
        df['Company Name'] = df['Company Name'].str.strip()
        df = df.rename(columns=self.COLUMN_MAPPING).reset_index(drop=True)

        return df.loc[:, self.COLUMN_MAPPING.values()].drop_duplicates().reset_index(drop=True)

    def save_data(self, output_path: str) -> DataFrame:
        
        output_path = Path(output_path)

        if not output_path.exists():
            raise FileNotFoundError(f"The specified path {output_path} does not exist.")
            
        self.df.to_parquet(output_path / self.FILE_NAME)