from pandas import DataFrame, to_datetime  # , #read_csv, read_parquet, , to_datetime
from pathlib import Path, PosixPath, WindowsPath

# import yfinance as yf
from yfinance.exceptions import YFRateLimitError
import json
import time
from tqdm.auto import tqdm
import os
import uuid

from stock_downloader.utilities import downcast_numeric_columns


class YahooFinanceBatchDownloader:
    RETRY_TIME: int = 20  # seconds
    TEMP_FILE_SUFFIX: str = "txt"
    # PARQUET_FILE: str = 'yahoo_info.parquet'

    def __init__(
        self,
        cls,
        symbols: list,
        path: str | PosixPath | WindowsPath,
        #  filename: str,
        delete_temp: bool = True,
    ) -> None:
        self.symbols = symbols
        self.temp_file = Path(path) / self.make_temp_filename()
        # self.filename = filename
        self.cls = cls

        if not self.temp_file.exists():
            self.temp_file.touch()

        self.data = self.run()
        for col in ["Date", "date"]:
            if col in self.data:
                self.data[col] = to_datetime(self.data[col])
        # self._save_parquet(output_path=Path(path))

        if delete_temp:
            self._delete_temp_file()

    def make_temp_filename(self) -> str:
        return f"{str(uuid.uuid4())}.{self.TEMP_FILE_SUFFIX}"

    def __call__(self) -> DataFrame:
        return self.data

    def _load_completed_symbols(self) -> list:
        if not self.temp_file.exists():
            return list()
        completed = list()
        with self.temp_file.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    completed.add(record["symbol"])
                except Exception as e:
                    print(e)
                    continue
        return sorted(set(completed))

    def _append_to_temp(self, symbol, info) -> None:
        with self.temp_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"symbol": symbol, "info": info}) + "\n")

    # def _save_parquet(self, output_path: WindowsPath) -> None:
    #     """Save the collected data to a Parquet file."""
    #     self.data.to_parquet(output_path / self.filename, index=False)

    def run(self) -> DataFrame:
        completed: list = self._load_completed_symbols()
        for symbol in tqdm(self.symbols):
            if symbol in completed:
                continue
            while True:
                try:
                    info = self.cls(symbol)
                    for record in info():
                        self._append_to_temp(symbol, record)
                    break  # Exit the while loop if successful
                except YFRateLimitError:
                    print(f"Rate limit exceeded for {symbol}. Retrying in {self.RETRY_TIME} seconds.")
                    time.sleep(self.RETRY_TIME)
                except Exception as e:
                    print(f"Other error for symbol {symbol}: {e}. Retrying in {self.RETRY_TIME} seconds.")
                    time.sleep(self.RETRY_TIME)

        return self._temp_to_parquet(output_path=self.temp_file)

    def _delete_temp_file(self) -> None:
        try:
            os.remove(self.temp_file)
            print(f"File '{self.temp_file}' deleted successfully.")
        except FileNotFoundError:
            print(f"File '{self.temp_file}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def _temp_to_parquet(self, output_path: str) -> DataFrame:
        """Save the collected data to a Parquet file."""
        records: list = []
        # Load all data from temp file
        with open(output_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    flat_info = {"symbol": record["symbol"]}
                    flat_info.update(record["info"])
                    records.append(flat_info)
                except Exception:
                    continue
        return downcast_numeric_columns(
            DataFrame(records)
            # .drop_duplicates(subset='symbol', keep='first')
            # .drop_duplicates()
            # .sort_values(["symbol"])
            .reset_index(drop=True)
            .replace("Infinity", None)
        )
