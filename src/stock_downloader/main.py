#!/usr/bin/env python3

from stock_downloader.utilities import validate_folder, rename_and_select_columns
from stock_downloader.data.nasdaq import NasdaqDownloader
from stock_downloader.data.index_symbols import GetIndexSymbols
from stock_downloader.data.listed_symbols import StockSymbolDownloader
from stock_downloader.data.yfinance_batch import YahooFinanceBatchDownloader
from stock_downloader.data.yfinance_info import YahooFinanceTickerInfo
from stock_downloader.data.yfinance_price import YahooFinancePriceHistory
from stock_downloader.technical_analysis.talib import (
    # run_talib_functions,
    # run_custom_ta,
    # concatenate_ta_results,
    run_all_talib,
    run_all_custom_ta,
)
from stock_downloader.technical_analysis.ta_definitions import (
    talib_functions,
    pattern_columns,
    custom_ta_sets__ma_ratio,
    custom_ta_sets__change_ratio,
    custom_ta_sets__future,
    custom_ta_sets__regression_channel,
    custom_ta_sets__regression_channel_ma,
)
from stock_downloader.technical_analysis.regression import run_all_regression
from stock_downloader.database.db import write_table

# from stock_downloader.schemas.dowjones import dowjones_schema
# from stock_downloader.schemas.nasdaq100 import nasdaq100_schema
# from stock_downloader.schemas.sp500 import sp500_schema

from stock_downloader.schemas.indicies import indicies_schema
from stock_downloader.schemas.equity_info import equity_info_schema
from stock_downloader.schemas.etf_info import etf_info_schema
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

from stock_downloader.data.loaders import load_mappings, load_config
from stock_downloader.data.select_symbols import select_symbols

# from pathlib import Path
import duckdb
from pandas import read_parquet, concat, to_datetime, Series
from tqdm.auto import tqdm
# from loguru import logging


def main():
    print("Running Stock Downloader!")

    # Load config and column mappings
    config = load_config()
    column_mappings = load_mappings(name="columns")

    # Create folders
    output_folder = validate_folder(path=config.get("data").get("output_folder"))
    db_folder = validate_folder(path=config.get("database").get("database_folder"))

    # Download symbols
    nasdaq_symbols_df = NasdaqDownloader().df
    other_symbols_df = StockSymbolDownloader().df
    index_symbols_df = GetIndexSymbols().df

    # Rename columns before saving
    # nasdaq_symbols_df = nasdaq_symbols_df.loc[:, column_mappings.get("nasdaq_symbols").keys()].rename(
    #     columns=column_mappings.get("nasdaq_symbols")
    # )
    nasdaq_symbols_df = rename_and_select_columns(df=nasdaq_symbols_df, mappings=column_mappings.get("nasdaq_symbols"))
    other_symbols_df = rename_and_select_columns(df=other_symbols_df, mappings=column_mappings.get("other_symbols"))
    index_symbols_df = rename_and_select_columns(df=index_symbols_df, mappings=column_mappings.get("indicies"))

    # other_symbols_df = other_symbols_df.loc[:, column_mappings.get("other_symbols").keys()].rename(
    #     columns=column_mappings.get("other_symbols")
    # )

    # index_symbols_df = index_symbols_df.loc[:, column_mappings.get("indicies").keys()].rename(columns=column_mappings.get("indicies"))

    nasdaq_symbols_df = nasdaq_symbols_schema.validate(nasdaq_symbols_df)
    other_symbols_df = other_symbols_schema.validate(other_symbols_df)
    index_symbols_df = indicies_schema.validate(index_symbols_df)

    # Save symbol data to temp files
    nasdaq_symbols_df.to_parquet(output_folder / "nasdaq_symbols.parquet")
    other_symbols_df.to_parquet(output_folder / "other_symbols.parquet")
    index_symbols_df.to_parquet(output_folder / "index_symbols.parquet")

    sector_etfs = load_mappings(name="sector_etfs").get("sector_etfs")

    equity_symbols, etf_symbols = select_symbols(
        nasdaq_df=nasdaq_symbols_df,
        other_df=other_symbols_df,
        sector_etfs=sector_etfs,
        indicies_df=index_symbols_df,
        get_etfs=config.get("symbols").get("get_etfs"),
        get_sector_etfs=config.get("symbols").get("get_sector_etfs"),
        get_dowjones=config.get("symbols").get("get_dowjones"),
        get_nasdaq100=config.get("symbols").get("get_nasdaq100"),
        get_sp500=config.get("symbols").get("get_sp500"),
        sample=5,
    )
    all_symbols = sorted(set(equity_symbols + etf_symbols))
    # print(equity_symbols)
    # print(etf_symbols)
    # print(all_symbols)
    # symbols = sorted(
    #     set(nasdaq_symbols_df.query('etf == "N"')["symbol"].to_list() + other_symbols_df.query('etf == "Y"')["symbol"].to_list())
    # )
    # sector_etfs = list(load_mappings(name="sector_etfs").get("sector_etfs").values())
    # symbols = [
    #     symbol
    #     for symbol in symbols
    #     if symbol in index_symbols_df.query("sp500 == True | dowjones == False | nasdaq100 == False")["symbol"].to_list() + sector_etfs
    # ][:10]

    # Only keep symbols that are in the Nasdaq 100
    # symbols = [symbol for symbol in symbols if symbol in nasdaq100_df.Symbol.to_list()]

    print(f"Equity symbols to download: {len(equity_symbols)}", equity_symbols)
    print(f"ETF symbols to download: {len(etf_symbols)}", etf_symbols)
    equity_info = YahooFinanceBatchDownloader(symbols=equity_symbols, path=output_folder, cls=YahooFinanceTickerInfo)
    etf_info = YahooFinanceBatchDownloader(symbols=etf_symbols, path=output_folder, cls=YahooFinanceTickerInfo)
    all_price = YahooFinanceBatchDownloader(symbols=all_symbols, path=output_folder, cls=YahooFinancePriceHistory)

    equity_info.data.to_parquet(output_folder / "equity_info.parquet", index=False)
    etf_info.data.to_parquet(output_folder / "etf_info.parquet", index=False)

    all_price_data = all_price.data
    # all_price_data["Date"] = to_datetime(all_price_data["Date"])
    all_price_data.to_parquet(output_folder / "yahoo_price.parquet", index=False)

    # Load price data
    price_df = read_parquet(output_folder / "yahoo_price.parquet")
    # print("price_df", price_df)
    # Compute regression channels
    # regression_df = concat(
    #     [
    #         run_regression_for_symbol(
    #             date_column=config.get("regression").get("date_column"),
    #             price_column=config.get("regression").get("price_column"),
    #             symbol=df[0],
    #             df=df[1],
    #             min_regression_days=config.get("regression").get("min_regression_days"),
    #             max_regression_days=config.get("regression").get("max_regression_days"),
    #         )
    #         for df in tqdm(price_df.groupby("symbol"), desc="Regression")
    #     ]
    # )
    regression_df = run_all_regression(price_df=price_df, regression_config=config.get("regression"))
    # regression_df = concat([df for df in regression_results_list if df is not None])
    regression_df.to_parquet(output_folder / "regression_data.parquet", index=False)
    # regression_df = price_df.merge(regression_df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner")

    # Compute TA-LIB indicators
    # talib__df = concatenate_ta_results(
    #     [
    #         run_talib_functions(df=df[1], functions=talib_functions, pattern_columns=pattern_columns)
    #         for df in tqdm(price_df.groupby("symbol"))
    #     ]
    # )
    talib__df = run_all_talib(data_df=price_df, functions=talib_functions, pattern_columns=pattern_columns)
    # talib__df = concatenate_ta_results(talib_results)
    talib__df.to_parquet(output_folder / "ta_talib.parquet")

    # Compute Moving Average Ratios and Price Change Ratios
    # ta__ma_ratio__df = concatenate_ta_results(
    #     [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__ma_ratio) for df in tqdm(talib__df.groupby("symbol"))]
    # )
    ta__ma_ratio__df = run_all_custom_ta(data_df=talib__df, functions=custom_ta_sets__ma_ratio)

    # ta__ma_ratio__df = concatenate_ta_results(ma_ratio_results)
    ta__ma_ratio__df.to_parquet(output_folder / "ta__ma_ratio.parquet", index=False)

    # Compute Price Change Ratios
    # ta__change__df = concatenate_ta_results(
    #     [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__change_ratio) for df in tqdm(price_df.groupby("symbol"))]
    # )
    ta__change__df = run_all_custom_ta(data_df=price_df, functions=custom_ta_sets__change_ratio)

    # ta__change__df = concatenate_ta_results(ma_change_results)
    ta__change__df.to_parquet(output_folder / "ta__change.parquet", index=False)

    # Compute Future Price Ratios
    # ma_future_df = concatenate_ta_results(
    #     [run_custom_ta(df=df[1], custom_ta=custom_ta_sets__future) for df in tqdm(price_df.groupby("symbol"))]
    # )
    ma_future_df = run_all_custom_ta(data_df=price_df, functions=custom_ta_sets__future)

    # ma_future_df = concatenate_ta_results(ma_future_results)
    ma_future_df.to_parquet(output_folder / "ta__future.parquet", index=False)

    # Compute 1st regression channel indicators
    # regression_indicators_df = concatenate_ta_results(
    #     [
    #         run_custom_ta(df=df[1], custom_ta=custom_ta_sets__regression_channel)
    #         for df in tqdm(
    #             price_df.merge(regression_df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner").groupby("symbol")
    #         )
    #     ]
    # )
    regression_indicators_df = run_all_custom_ta(
        data_df=price_df.merge(regression_df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner"),
        functions=custom_ta_sets__regression_channel,
    )

    # regression_indicators_df = concatenate_ta_results(regression_indicators_results)
    regression_indicators_df.to_parquet(output_folder / "regression_indicators.parquet")

    # Compute TA-LIB indicators
    # regression_indicators_ma_df = concatenate_ta_results(
    #     [
    #         run_custom_ta(df=df[1], custom_ta=custom_ta_sets__regression_channel_ma)
    #         for df in tqdm(regression_indicators_df.groupby("symbol"))
    #     ]
    # )
    regression_indicators_ma_df = run_all_custom_ta(data_df=regression_indicators_df, functions=custom_ta_sets__regression_channel_ma)

    # regression_indicators_ma_df = concatenate_ta_results(regression_indicators_ma_results)
    regression_indicators_ma_df.to_parquet(output_folder / "regression_indicators_ma.parquet")

    # Select and rename columns
    # regression_indicators_ma_df = regression_indicators_ma_df.loc[:, column_mappings.get("regression_indicators_ma").keys()].rename(
    #     columns=column_mappings.get("regression_indicators_ma")
    # )
    regression_indicators_ma_df = rename_and_select_columns(
        df=regression_indicators_ma_df, mappings=column_mappings.get("regression_indicators_ma")
    )

    # regression_indicators_df = regression_indicators_df.loc[:, column_mappings.get("regression_indicators").keys()].rename(
    #     columns=column_mappings.get("regression_indicators")
    # )
    regression_indicators_df = rename_and_select_columns(df=regression_indicators_df, mappings=column_mappings.get("regression_indicators"))

    # ma_future_df = ma_future_df.loc[:, column_mappings.get("ma_future").keys()].rename(columns=column_mappings.get("ma_future"))
    ma_future_df = rename_and_select_columns(df=ma_future_df, mappings=column_mappings.get("ma_future"))

    # ta__change__df = ta__change__df.loc[:, column_mappings.get("ta__change").keys()].rename(columns=column_mappings.get("ta__change"))
    ta__change__df = rename_and_select_columns(df=ta__change__df, mappings=column_mappings.get("ta__change"))

    # ta__ma_ratio__df = ta__ma_ratio__df.loc[:, column_mappings.get("ta__ma_ratio").keys()].rename(
    #     columns=column_mappings.get("ta__ma_ratio")
    # )
    ta__ma_ratio__df = rename_and_select_columns(df=ta__ma_ratio__df, mappings=column_mappings.get("ta__ma_ratio"))

    # talib__df = talib__df.loc[:, column_mappings.get("ta__talib").keys()].rename(columns=column_mappings.get("ta__talib"))
    talib__df = rename_and_select_columns(df=talib__df, mappings=column_mappings.get("ta__talib"))

    # regression_df = regression_df.loc[:, column_mappings.get("regression").keys()].rename(columns=column_mappings.get("regression"))
    regression_df = rename_and_select_columns(df=regression_df, mappings=column_mappings.get("regression"))

    # price_df = price_df.loc[:, column_mappings.get("price").keys()].rename(columns=column_mappings.get("price"))
    price_df = rename_and_select_columns(df=price_df, mappings=column_mappings.get("price"))

    # equity_info_df = equity_info.data.loc[:, column_mappings.get("equity_info").keys()].rename(columns=column_mappings.get("equity_info"))
    # etf_info_df = etf_info.data.loc[:, column_mappings.get("etf_info").keys()].rename(columns=column_mappings.get("etf_info"))
    equity_info_df = rename_and_select_columns(df=equity_info.data, mappings=column_mappings.get("equity_info"))
    etf_info_df = rename_and_select_columns(df=etf_info.data, mappings=column_mappings.get("etf_info"))

    # Write to database
    db = duckdb.connect(db_folder / f"{config.get('database').get('database_name')}.db")
    write_table(db=db, df=nasdaq_symbols_df, table="nasdaq_symbols")
    write_table(db=db, df=other_symbols_df, table="other_symbols")
    write_table(db=db, df=index_symbols_df, table="indicies")
    write_table(db=db, df=equity_info_schema.validate(equity_info_df), table="equity_info")
    write_table(db=db, df=etf_info_schema.validate(etf_info_df), table="etf_info")
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
