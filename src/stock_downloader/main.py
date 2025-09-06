#!/usr/bin/env python3

from stock_downloader.utilities import validate_folder, rename_and_select_columns
from stock_downloader.data.nasdaq import NasdaqDownloader
from stock_downloader.data.index_symbols import GetIndexSymbols
from stock_downloader.data.listed_symbols import StockSymbolDownloader
from stock_downloader.data.yfinance_batch import YahooFinanceBatchDownloader
from stock_downloader.data.yfinance_info import YahooFinanceTickerInfo
from stock_downloader.data.yfinance_price import YahooFinancePriceHistory
from stock_downloader.technical_analysis.talib import (
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
from stock_downloader.data.select_symbols import select_symbols, symbolLists

import duckdb
from loguru import logger


def main():
    logger.info("Running Stock Downloader!")

    logger.info("Load the configuration file")
    config = load_config()

    logger.info("Load the data column mapping file")
    column_mappings = load_mappings(name="columns")

    logger.info("Validate the output and database folders")
    output_folder = validate_folder(path=config.get("data").get("output_folder"))
    db_folder = validate_folder(path=config.get("database").get("database_folder"))

    logger.info("Retrieve the symbol files")
    nasdaq_symbols_df = NasdaqDownloader().df
    other_symbols_df = StockSymbolDownloader().df
    index_symbols_df = GetIndexSymbols().df

    logger.info("Rename and select the symbol tables")
    nasdaq_symbols_df = rename_and_select_columns(df=nasdaq_symbols_df, mappings=column_mappings.get("nasdaq_symbols"))
    other_symbols_df = rename_and_select_columns(df=other_symbols_df, mappings=column_mappings.get("other_symbols"))
    index_symbols_df = rename_and_select_columns(df=index_symbols_df, mappings=column_mappings.get("indicies"))

    # Validate symbol data
    logger.info("Validate the symbol tables")
    nasdaq_symbols_df = nasdaq_symbols_schema.validate(nasdaq_symbols_df)
    other_symbols_df = other_symbols_schema.validate(other_symbols_df)
    index_symbols_df = indicies_schema.validate(index_symbols_df)

    logger.info("Save symbol tables to temporary files")
    nasdaq_symbols_df.to_parquet(output_folder / "nasdaq_symbols.parquet")
    other_symbols_df.to_parquet(output_folder / "other_symbols.parquet")
    index_symbols_df.to_parquet(output_folder / "index_symbols.parquet")

    logger.info("Load the sector ETF mappings")
    sector_etfs = load_mappings(name="sector_etfs").get("sector_etfs")

    logger.info("Create the equity and etf symbol lists")
    symbol_lists: symbolLists = select_symbols(
        nasdaq_df=nasdaq_symbols_df,
        other_df=other_symbols_df,
        sector_etfs=sector_etfs,
        indicies_df=index_symbols_df,
        get_etfs=config.get("symbols").get("get_etfs"),
        get_sector_etfs=config.get("symbols").get("get_sector_etfs"),
        get_dowjones=config.get("symbols").get("get_dowjones"),
        get_nasdaq100=config.get("symbols").get("get_nasdaq100"),
        get_sp500=config.get("symbols").get("get_sp500"),
        sample=10,
    )

    logger.info("Create the list of symbols to process")
    all_symbols = sorted(set(symbol_lists.equity + symbol_lists.etf))

    logger.info(f"Equity symbols to process: {len(symbol_lists.equity)}")
    logger.info(f"ETF symbols to process: {len(symbol_lists.etf)}")
    logger.info(f"Total symbols to process: {len(all_symbols)}")

    logger.info("Use yfinance to get the info and price data for all symbols")
    equity_info = YahooFinanceBatchDownloader(symbols=symbol_lists.equity, path=output_folder, cls=YahooFinanceTickerInfo)
    etf_info = YahooFinanceBatchDownloader(symbols=symbol_lists.etf, path=output_folder, cls=YahooFinanceTickerInfo)
    all_price = YahooFinanceBatchDownloader(symbols=all_symbols, path=output_folder, cls=YahooFinancePriceHistory)

    logger.info("Save equity and etf info and price tables to temporary files")
    equity_info.data.to_parquet(output_folder / "equity_info.parquet", index=False)
    etf_info.data.to_parquet(output_folder / "etf_info.parquet", index=False)
    all_price.data.to_parquet(output_folder / "yahoo_price.parquet", index=False)

    price_df = all_price.data.copy(deep=True)

    logger.info("Find best regression lines")
    regression_df = run_all_regression(price_df=price_df, regression_config=config.get("regression"))
    regression_df.to_parquet(output_folder / "regression_data.parquet", index=False)

    logger.info("Calculate talib indicators")
    talib__df = run_all_talib(data_df=price_df, functions=talib_functions, pattern_columns=pattern_columns)
    talib__df.to_parquet(output_folder / "ta_talib.parquet")

    logger.info("Calculate custom talib moving averages")
    ta__ma_ratio__df = run_all_custom_ta(
        data_df=price_df.merge(talib__df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner"),
        functions=custom_ta_sets__ma_ratio,
    )
    ta__ma_ratio__df.to_parquet(output_folder / "ta__ma_ratio.parquet", index=False)

    logger.info("Calculate price change values")
    ta__change__df = run_all_custom_ta(data_df=price_df, functions=custom_ta_sets__change_ratio)
    ta__change__df.to_parquet(output_folder / "ta__change.parquet", index=False)

    logger.info("Calculate future price changes")
    ma_future_df = run_all_custom_ta(data_df=price_df, functions=custom_ta_sets__future)
    ma_future_df.to_parquet(output_folder / "ta__future.parquet", index=False)

    logger.info("Calculate regression channel relative price positions")
    regression_indicators_df = run_all_custom_ta(
        data_df=price_df.merge(regression_df.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner"),
        functions=custom_ta_sets__regression_channel,
    )
    regression_indicators_df.to_parquet(output_folder / "regression_indicators.parquet")

    logger.info("Calculate regression moving averages")
    regression_indicators_ma_df = run_all_custom_ta(data_df=regression_indicators_df, functions=custom_ta_sets__regression_channel_ma)
    regression_indicators_ma_df.to_parquet(output_folder / "regression_indicators_ma.parquet")

    logger.info("Format columns for each dataframe to be added to database")
    regression_indicators_ma_df = rename_and_select_columns(
        df=regression_indicators_ma_df, mappings=column_mappings.get("regression_indicators_ma")
    )
    regression_indicators_df = rename_and_select_columns(df=regression_indicators_df, mappings=column_mappings.get("regression_indicators"))
    ma_future_df = rename_and_select_columns(df=ma_future_df, mappings=column_mappings.get("ma_future"))
    ta__change__df = rename_and_select_columns(df=ta__change__df, mappings=column_mappings.get("ta__change"))
    ta__ma_ratio__df = rename_and_select_columns(df=ta__ma_ratio__df, mappings=column_mappings.get("ta__ma_ratio"))
    talib__df = rename_and_select_columns(df=talib__df, mappings=column_mappings.get("ta__talib"))
    regression_df = rename_and_select_columns(df=regression_df, mappings=column_mappings.get("regression"))
    price_df = rename_and_select_columns(df=price_df, mappings=column_mappings.get("price"))
    equity_info_df = rename_and_select_columns(df=equity_info.data, mappings=column_mappings.get("equity_info"))
    etf_info_df = rename_and_select_columns(df=etf_info.data, mappings=column_mappings.get("etf_info"))

    # Write to database
    logger.info("Create database connection")
    db = duckdb.connect(db_folder / f"{config.get('database').get('database_name')}.db")

    logger.info("Write each dataframe to database")
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

    logger.info("Close connection to database")
    db.close()


if __name__ == "__main__":
    main()
