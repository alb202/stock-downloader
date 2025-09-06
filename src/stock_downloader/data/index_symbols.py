from pandas import read_html, DataFrame
from pathlib import Path, PosixPath, WindowsPath


class GetIndexSymbols:
    HTTP_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.google.com/",  # Optional: Mimic a previous page
    }

    COLUMN_RENAME = {
        "sp500": {"Symbol": "symbol", "Company Name": "company_name"},
        "dowjones": {"Symbol": "symbol"},
        "nasdaq100": {"Symbol": "symbol", "Company Name": "company_name", "No.": "number", "% Change": "_%_Change"},
    }

    INDEX_LIST = {
        "sp500": {"url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "table_index": 0},
        "dowjones": {"url": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", "table_index": 2},
        "nasdaq100": {"url": "https://stockanalysis.com/list/nasdaq-100-stocks/", "table_index": 0},
    }

    def __init__(self):
        self.sp500_df = self.get_table(self.INDEX_LIST["sp500"]["url"], self.INDEX_LIST["sp500"]["table_index"]).rename(
            columns=self.COLUMN_RENAME.get("sp500")
        )
        self.dowjones_df = self.get_table(self.INDEX_LIST["dowjones"]["url"], self.INDEX_LIST["dowjones"]["table_index"]).rename(
            columns=self.COLUMN_RENAME.get("dowjones")
        )
        self.nasdaq100_df = self.get_table(self.INDEX_LIST["nasdaq100"]["url"], self.INDEX_LIST["nasdaq100"]["table_index"]).rename(
            columns=self.COLUMN_RENAME.get("nasdaq100")
        )
        self.df = self.merge_tables()

    def merge_tables(self) -> DataFrame:
        return (
            self.dowjones_df.loc[:, ["symbol"]]
            .assign(dowjones=True)
            .merge(self.nasdaq100_df.loc[:, ["symbol"]].assign(nasdaq100=True), how="outer", on="symbol")
            .merge(self.sp500_df.loc[:, ["symbol"]].assign(sp500=True), how="outer", on="symbol")
            .fillna(False)
        )

    def save_data(self, path: str | PosixPath | WindowsPath) -> None:
        """
        Save the DataFrame to a specified output path.
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"The specified path {Path(path)} does not exist.")

        self.sp500_df.to_parquet(Path(path) / "sp500.parquet")
        self.dowjones_df.to_parquet(Path(path) / "dowjones.parquet")
        self.nasdaq100_df.to_parquet(Path(path) / "nasdaq100.parquet")

    def get_table(self, url: str, table: int) -> DataFrame:
        """
        Fetch the first table from the Wikipedia page and return it as a DataFrame.
        """
        try:
            tables = read_html(url, storage_options=self.HTTP_HEADERS)  # Extract all tables
            if not tables:
                raise ValueError("No tables found on the page.")
            return tables[table]  # First table
        except Exception as e:
            print(f"Error fetching table: {e}")
            return DataFrame()  # Return empty DataFrame on error
