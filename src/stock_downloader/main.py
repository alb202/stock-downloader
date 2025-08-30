#!/usr/bin/env python3
from pathlib import Path  # , PosixPath, WindowsPath

from data.nasdaq import NasdaqDownloader
from data.index_symbols import GetIndexSymbols
from data.listed_symbols import StockSymbolDownloader
from data.yfinance_batch import YahooFinanceBatchDownloader
from data.yfinance_info import YahooFinanceTickerInfo
from data.yfinance_price import YahooFinancePriceHistory
from indicators.ta import run_talib_functions, run_custom_ta, concatenate_ta_results
from indicators.ta_definitions import (
    talib_functions,
    pattern_columns,
    custom_ta_sets__ma_ratio,
    custom_ta_sets__change_ratio,
    custom_ta_sets__future,
    custom_ta_sets__regression_channel,
    custom_ta_sets__regression_channel_ma,
)
from indicators.regression import (
    run_regression_for_symbol,
)  # , get_best_result, RegressionResult, RegressionLines, BestResults
from db.db import write_table

from schemas.dowjones import dowjones_schema
from schemas.info import info_schema
from schemas.ma_future import ma_future_schema
from schemas.nasdaq_symbols import nasdaq_symbols_schema
from schemas.nasdaq100 import nasdaq100_schema
from schemas.other_symbols import other_symbols_schema
from schemas.price import price_schema
from schemas.regression_indicators import regression_indicators_schema
from schemas.regression_indicators_ma import regression_indicators_ma_schema
from schemas.regression import regression_schema
from schemas.sp500 import sp500_schema
from schemas.ta__change import ta__change_schema
from schemas.ta__ma_ratio import ta__ma_ratio_schema
from schemas.talib import talib_schema


import duckdb
import toml

# from models.data_classes import BestCorrelation
from pandas import read_parquet, concat, to_datetime, Series
from tqdm.auto import tqdm


def main():
    print("Welcome to Stock Downloader!")

    output_folder = Path("D:/stocks/output/raw_data/")
    output_folder.mkdir(exist_ok=True, parents=True)
    db_folder = Path("D:/stocks/output/database/")
    db_folder.mkdir(exist_ok=True, parents=True)

    column_mappings = toml.load(open("C:/Users/ab/OneDrive/projects/stocks/src/stock_downloader/models/column_mappings.toml", "r"))

    nasdaq = NasdaqDownloader()
    nasdaq_symbols_df = nasdaq.df

    other_symbols = StockSymbolDownloader()
    other_symbols_df = other_symbols.df

    index_symbols = GetIndexSymbols()
    sp500_df = index_symbols.sp500_df
    dowjones_df = index_symbols.dowjones_df
    nasdaq100_df = index_symbols.nasdaq100_df

    nasdaq_symbols_df.to_parquet(output_folder / "nasdaq_symbols.parquet")
    other_symbols_df.to_parquet(output_folder / "other_symbols.parquet")

    sp500_df.to_parquet(output_folder / "sp500.parquet")
    dowjones_df.to_parquet(output_folder / "dowjones.parquet")
    nasdaq100_df.to_parquet(output_folder / "nasdaq100.parquet")

    # print(sp500_df.info())
    # print(sp500_df)
    # print(sp500_df.Symbol)
    # print(dowjones_df.info())
    # print(dowjones_df)

    # print(nasdaq100_df.info())
    # print(nasdaq100_df)

    symbols = sorted(
        set(nasdaq_symbols_df.query('ETF == "N"')["symbol"].to_list() + other_symbols_df.query('ETF == "N"')["symbol"].to_list())
    )
    # print("symbols_", symbols_)
    # Only keep symbols that are in S&P 500
    # symbols = [symbol for symbol in symbols if symbol in sp500_df.Symbol.to_list()]

    # Only keep symbols that are in the Dow Jones
    symbols = [symbol for symbol in symbols if symbol in dowjones_df.Symbol.to_list()][:]

    # Only keep symbols that are in the Nasdaq 100
    # symbols = [symbol for symbol in symbols if symbol in nasdaq100_df.Symbol.to_list()]

    print(f"Total symbols to download: {len(symbols)}")
    # print(symbols)
    all_info = YahooFinanceBatchDownloader(symbols=symbols, path=output_folder, cls=YahooFinanceTickerInfo)
    all_price = YahooFinanceBatchDownloader(symbols=symbols, path=output_folder, cls=YahooFinancePriceHistory)

    all_info.data.to_parquet(output_folder / "yahoo_info.parquet", index=False)
    all_price_data = all_price.data

    # print(all_price_data.info())

    all_price_data["Date"] = to_datetime(all_price_data["Date"])
    all_price_data.to_parquet(output_folder / "yahoo_price.parquet", index=False)

    # Load price data
    price_df = read_parquet(output_folder / "yahoo_price.parquet")
    # price_df["Date"] = to_datetime(price_df["Date"])

    # Compute regression channels
    symbol_data = list(price_df.copy(deep=True).groupby("symbol"))
    regression_results_list = [run_regression_for_symbol(symbol=symbol, df=data) for symbol, data in symbol_data]
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

    # print("regression_df", regression_df.columns)

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
    sp500_df = sp500_df.loc[:, column_mappings.get("sp500").keys()].rename(columns=column_mappings.get("sp500"))
    dowjones_df = dowjones_df.loc[:, column_mappings.get("dowjones").keys()].rename(columns=column_mappings.get("dowjones"))
    nasdaq100_df = nasdaq100_df.loc[:, column_mappings.get("nasdaq100").keys()].rename(columns=column_mappings.get("nasdaq100"))
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

    db = duckdb.connect(db_folder / "stocks.db")

    write_table(db=db, df=dowjones_schema.validate(dowjones_df), table="dowjones")
    write_table(db=db, df=info_schema.validate(all_info_df), table="info")
    write_table(db=db, df=ma_future_schema.validate(ma_future_df), table="ma_future")
    write_table(db=db, df=nasdaq_symbols_schema.validate(nasdaq_symbols_df), table="nasdaq_symbols")
    write_table(db=db, df=other_symbols_schema.validate(other_symbols_df), table="other_symbols")
    write_table(db=db, df=price_schema.validate(price_df), table="price")
    write_table(db=db, df=regression_schema.validate(regression_df), table="regression")
    write_table(db=db, df=regression_indicators_schema.validate(regression_indicators_df), table="regression_indicators")
    write_table(db=db, df=regression_indicators_ma_schema.validate(regression_indicators_ma_df), table="regression_indicators_ma")
    write_table(db=db, df=sp500_schema.validate(sp500_df), table="sp500")
    write_table(db=db, df=ta__change_schema.validate(ta__change__df), table="ta__change")
    write_table(db=db, df=ta__ma_ratio_schema.validate(ta__ma_ratio__df), table="ta__ma_ratio")
    write_table(db=db, df=talib_schema.validate(talib__df), table="talib")

    db.close()
    # dowjones_schema.validate(dowjones_df)
    # info_schema.validate(all_info)
    # ma_future_schema.validate(ma_future_df)
    # nasdaq_symbols_schema.validate(nasdaq_symbols_df)
    # other_symbols_schema.validate(other_symbols_df)
    # price_schema.validate(price_df)
    # regression_schema.validate(regression_df)
    # regression_indicators_schema.validate(regression_indicators_df)
    # regression_indicators_ma_schema.validate(regression_indicators_ma_df)
    # sp500_schema.validate(sp500_df)
    # ta__change_schema.validate(ta__change__df)
    # ta__ma_ratio_schema.validate(ta__ma_ratio__df)
    # talib_schema.validate(talib__df)

    # # Load all this data into a Duckdb database
    # nasdaq_symbols_df.to_parquet(output_folder / "nasdaq_symbols.parquet")
    # other_symbols_df.to_parquet(output_folder / "other_symbols.parquet")

    # sp500_df.to_parquet(output_folder / "sp500.parquet")
    # dowjones_df.to_parquet(output_folder / "dowjones.parquet")
    # nasdaq100_df.to_parquet(output_folder / "nasdaq100.parquet")

    # regression_indicators_ma_df = read_parquet(output_folder / "regression_indicators_ma.parquet")
    # regression_indicators_df.to_parquet(output_folder / "regression_indicators.parquet")
    # ma_future_df.to_parquet(output_folder / "ta__future.parquet", index=False)
    # ta__change__df.to_parquet(output_folder / "ta__change.parquet", index=False)
    # ta__ma_ratio__df.to_parquet(output_folder / "ta__ma_ratio.parquet", index=False)
    # talib__df.to_parquet(output_folder / "ta_talib.parquet")
    # regression_df.to_parquet(output_folder / "regression_data.parquet", index=False)
    # price_df = read_parquet(output_folder / "yahoo_price.parquet")
    # all_info.data.to_parquet(output_folder / "yahoo_info.parquet", index=False)


if __name__ == "__main__":
    main()
