#!/usr/bin/env python3

from stock_downloader.data.nasdaq import NasdaqDownloader
from stock_downloader.data.index_symbols import GetIndexSymbols
from stock_downloader.data.listed_symbols import StockSymbolDownloader
from stock_downloader.data.yfinance_batch import YahooFinanceBatchDownloader
from stock_downloader.data.yfinance_info import YahooFinanceTickerInfo
from stock_downloader.data.yfinance_price import YahooFinancePriceHistory
from stock_downloader.indicators.ta import run_talib_functions, run_custom_ta, concatenate_ta_results
from stock_downloader.indicators.ta_definitions import (
    talib_functions,
    pattern_columns,
    custom_ta_sets__ma_ratio,
    custom_ta_sets__change_ratio,
    custom_ta_sets__future,
    custom_ta_sets__regression_channel,
    custom_ta_sets__regression_channel_ma,
)
from stock_downloader.indicators.regression import (
    run_regression_for_symbol,
)  # , get_best_result, RegressionResult, RegressionLines, BestResults
from stock_downloader.database.db import write_table

# from stock_downloader.schemas.dowjones import dowjones_schema
# from stock_downloader.schemas.nasdaq100 import nasdaq100_schema
# from stock_downloader.schemas.sp500 import sp500_schema

from stock_downloader.schemas.indicies import indicies_schema
from stock_downloader.schemas.info import info_schema
from stock_downloader.schemas.price import price_schema
from stock_downloader.schemas.nasdaq_symbols import nasdaq_symbols_schema
from stock_downloader.schemas.other_symbols import other_symbols_schema
from stock_downloader.schemas.talib import talib_schema
from stock_downloader.schemas.ta__change import ta__change_schema
from stock_downloader.schemas.ta__ma_ratio import ta__ma_ratio_schema
from stock_downloader.schemas.regression import regression_schema
from stock_downloader.schemas.regression_indicators import regression_indicators_schema
from stock_downloader.schemas.regression_indicators_ma import regression_indicators_ma_schema
from stock_downloader.schemas.ma_future import ma_future_schema

from stock_downloader.data.loaders import load_column_mappings, load_config
from pathlib import Path  # , PosixPath, WindowsPath
# from loguru import logging

import duckdb
# import toml

# from models.data_classes import BestCorrelation
from pandas import read_parquet, concat, to_datetime, Series
from tqdm.auto import tqdm

INDICIES = ["sp500", "dowjones", "nasdaq100"]


def main():
    print("Welcome to Stock Downloader!")

    print(Path(".").cwd())

    # config = toml.load(open("src/stock_downloader/config/config.toml", "r"))
    config = load_config()
    # output_folder = Path("D:/stocks/output/raw_data/")
    output_folder = Path(config.get("data").get("output_folder"))
    output_folder.mkdir(exist_ok=True, parents=True)

    db_folder = Path(config.get("database").get("database_folder"))
    db_folder.mkdir(exist_ok=True, parents=True)

    # column_mappings = toml.load(open("C:/Users/ab/OneDrive/projects/stocks/src/stock_downloader/models/column_mappings.toml", "r"))
    # column_mappings = toml.load(open("src/stock_downloader/models/column_mappings.toml", "r"))
    column_mappings = load_column_mappings()
    nasdaq = NasdaqDownloader()
    nasdaq_symbols_df = nasdaq.df

    other_symbols = StockSymbolDownloader()
    other_symbols_df = other_symbols.df

    index_symbols = GetIndexSymbols()
    # sp500_df = index_symbols.sp500_df
    # dowjones_df = index_symbols.dowjones_df
    # nasdaq100_df = index_symbols.nasdaq100_df

    nasdaq_symbols_df.to_parquet(output_folder / "nasdaq_symbols.parquet")
    other_symbols_df.to_parquet(output_folder / "other_symbols.parquet")
    index_symbols.df.to_parquet(output_folder / "index_symbols.parquet")
    # sp500_df.to_parquet(output_folder / "sp500.parquet")
    # dowjones_df.to_parquet(output_folder / "dowjones.parquet")
    # nasdaq100_df.to_parquet(output_folder / "nasdaq100.parquet")
    # index_df.to_parquet
    symbols = sorted(
        set(nasdaq_symbols_df.query('ETF == "N"')["symbol"].to_list() + other_symbols_df.query('ETF == "N"')["symbol"].to_list())
    )
    # print("symbols_", symbols_)
    # Only keep symbols that are in S&P 500
    # symbols = [symbol for symbol in symbols if symbol in sp500_df.Symbol.to_list()]

    # Only keep symbols that are in the Dow Jones
    symbols = [
        symbol
        for symbol in symbols
        if symbol in index_symbols.df.query("sp500 == True | dowjones == True | nasdaq100 == True")["symbol"].to_list()
    ][:5]

    # Only keep symbols that are in the Nasdaq 100
    # symbols = [symbol for symbol in symbols if symbol in nasdaq100_df.Symbol.to_list()]

    print(f"Total symbols to download: {len(symbols)}")
    all_info = YahooFinanceBatchDownloader(symbols=symbols, path=output_folder, cls=YahooFinanceTickerInfo)
    all_price = YahooFinanceBatchDownloader(symbols=symbols, path=output_folder, cls=YahooFinancePriceHistory)

    all_info.data.to_parquet(output_folder / "yahoo_info.parquet", index=False)
    all_price_data = all_price.data

    all_price_data["Date"] = to_datetime(all_price_data["Date"])
    all_price_data.to_parquet(output_folder / "yahoo_price.parquet", index=False)

    # Load price data
    price_df = read_parquet(output_folder / "yahoo_price.parquet")

    # Compute regression channels
    symbol_data = list(price_df.copy(deep=True).groupby("symbol"))
    regression_results_list = [run_regression_for_symbol(symbol=symbol, df=data) for symbol, data in tqdm(symbol_data, desc="Regression")]
    regression_df = concat([df for df in regression_results_list if df is not None])
    regression_df = concat(
        [
            regression_df.loc[:, ["symbol", "max_regression_days", "date", "earliest_start_date", "best_regression_date"]],
            regression_df.best_result.apply(Series),
            regression_df.best_lines.apply(Series),
        ],
        axis=1,
    )
    regression_df.to_parquet(output_folder / "regression_data.parquet", index=False)
    regression_df = price_df.merge(regression_df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner")

    # Compute TA-LIB indicators
    talib_results = [
        run_talib_functions(df=df[1], talib_functions=talib_functions, pattern_columns=pattern_columns)
        for df in tqdm(price_df.groupby("symbol"))
    ]
    talib__df = concatenate_ta_results(talib_results)
    talib__df.to_parquet(output_folder / "ta_talib.parquet")

    # Compute Moving Average Ratios and Price Change Ratios
    ma_ratio_results = [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__ma_ratio) for df in tqdm(talib__df.groupby("symbol"))]
    ta__ma_ratio__df = concatenate_ta_results(ma_ratio_results)
    ta__ma_ratio__df.to_parquet(output_folder / "ta__ma_ratio.parquet", index=False)

    # Compute Price Change Ratios
    ma_change_results = [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__change_ratio) for df in tqdm(price_df.groupby("symbol"))]
    ta__change__df = concatenate_ta_results(ma_change_results)
    ta__change__df.to_parquet(output_folder / "ta__change.parquet", index=False)

    # Compute Future Price Ratios
    ma_future_results = [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__future) for df in tqdm(price_df.groupby("symbol"))]
    ma_future_df = concatenate_ta_results(ma_future_results)
    ma_future_df.to_parquet(output_folder / "ta__future.parquet", index=False)

    # Compute 1st regression channel indicators
    regression_indicators_results = [
        run_custom_ta(df=df[1], custom_ta=custom_ta_sets__regression_channel) for df in tqdm(regression_df.groupby("symbol"))
    ]
    regression_indicators_df = concatenate_ta_results(regression_indicators_results)
    regression_indicators_df.to_parquet(output_folder / "regression_indicators.parquet")

    # Compute TA-LIB indicators
    regression_indicators_ma_results = [
        run_custom_ta(df=df[1], custom_ta=custom_ta_sets__regression_channel_ma) for df in tqdm(regression_indicators_df.groupby("symbol"))
    ]
    regression_indicators_ma_df = concatenate_ta_results(regression_indicators_ma_results)
    regression_indicators_ma_df.to_parquet(output_folder / "regression_indicators_ma.parquet")

    # Rename columns before saving
    nasdaq_symbols_df = nasdaq_symbols_df.loc[:, column_mappings.get("nasdaq_symbols").keys()].rename(
        columns=column_mappings.get("nasdaq_symbols")
    )
    other_symbols_df = other_symbols_df.loc[:, column_mappings.get("other_symbols").keys()].rename(
        columns=column_mappings.get("other_symbols")
    )
    indicies_df = index_symbols.df.loc[:, column_mappings.get("indicies").keys()].rename(columns=column_mappings.get("indicies"))

    # sp500_df = sp500_df.loc[:, column_mappings.get("sp500").keys()].rename(columns=column_mappings.get("sp500"))
    # dowjones_df = dowjones_df.loc[:, column_mappings.get("dowjones").keys()].rename(columns=column_mappings.get("dowjones"))
    # nasdaq100_df = nasdaq100_df.loc[:, column_mappings.get("nasdaq100").keys()].rename(columns=column_mappings.get("nasdaq100"))
    regression_indicators_ma_df = regression_indicators_ma_df.loc[:, column_mappings.get("regression_indicators_ma").keys()].rename(
        columns=column_mappings.get("regression_indicators_ma")
    )
    regression_indicators_df = regression_indicators_df.loc[:, column_mappings.get("regression_indicators").keys()].rename(
        columns=column_mappings.get("regression_indicators")
    )
    ma_future_df = ma_future_df.loc[:, column_mappings.get("ma_future").keys()].rename(columns=column_mappings.get("ma_future"))
    ta__change__df = ta__change__df.loc[:, column_mappings.get("ta__change").keys()].rename(columns=column_mappings.get("ta__change"))
    ta__ma_ratio__df = ta__ma_ratio__df.loc[:, column_mappings.get("ta__ma_ratio").keys()].rename(
        columns=column_mappings.get("ta__ma_ratio")
    )
    talib__df = talib__df.loc[:, column_mappings.get("ta__talib").keys()].rename(columns=column_mappings.get("ta__talib"))
    regression_df = regression_df.loc[:, column_mappings.get("ta__regression").keys()].rename(columns=column_mappings.get("ta__regression"))
    price_df = price_df.loc[:, column_mappings.get("price").keys()].rename(columns=column_mappings.get("price"))
    all_info_df = all_info.data.loc[:, column_mappings.get("symbol_info").keys()].rename(columns=column_mappings.get("symbol_info"))

    # Write to database
    db = duckdb.connect(db_folder / f"{config.get('database').get('database_name')}.db")
    write_table(db=db, df=nasdaq_symbols_schema.validate(nasdaq_symbols_df), table="nasdaq_symbols")
    write_table(db=db, df=other_symbols_schema.validate(other_symbols_df), table="other_symbols")
    write_table(db=db, df=info_schema.validate(all_info_df), table="info")
    write_table(db=db, df=indicies_schema.validate(indicies_df), table="indicies")
    # write_table(db=db, df=dowjones_schema.validate(dowjones_df), table="dowjones")
    # write_table(db=db, df=sp500_schema.validate(sp500_df), table="sp500")
    # write_table(db=db, df=nasdaq100_schema.validate(nasdaq100_df), table="sp500")
    write_table(db=db, df=price_schema.validate(price_df), table="price")
    write_table(db=db, df=talib_schema.validate(talib__df), table="talib")
    write_table(db=db, df=ta__change_schema.validate(ta__change__df), table="ta__change")
    write_table(db=db, df=ta__ma_ratio_schema.validate(ta__ma_ratio__df), table="ta__ma_ratio")
    write_table(db=db, df=regression_schema.validate(regression_df), table="regression")
    write_table(db=db, df=regression_indicators_schema.validate(regression_indicators_df), table="regression_indicators")
    write_table(db=db, df=regression_indicators_ma_schema.validate(regression_indicators_ma_df), table="regression_indicators_ma")
    write_table(db=db, df=ma_future_schema.validate(ma_future_df), table="ma_future")

    db.close()


if __name__ == "__main__":
    main()
