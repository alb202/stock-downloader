# from workflow_manager.defs.config import runtimeConfig
from stock_downloader.data.loaders import load_mappings, load_config
from stock_downloader.data.nasdaq import NasdaqDownloader
from stock_downloader.data.index_symbols import GetIndexSymbols
from stock_downloader.data.listed_symbols import StockSymbolDownloader
from stock_downloader.data.yfinance_batch import YahooFinanceBatchDownloader
from stock_downloader.data.yfinance_info import YahooFinanceTickerInfo
from stock_downloader.data.yfinance_price import YahooFinancePriceHistory
from stock_downloader.utilities import rename_and_select_columns
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
from stock_downloader.data.select_symbols import select_symbols, symbolLists
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
from pathlib import Path

# from dagster_duckdb import DuckDBResource
import dagster as dg
from pandas import DataFrame
import duckdb


@dg.asset(tags={"domain": "config"})
def config_asset() -> dict:
    return load_config()


@dg.asset(tags={"domain": "schemas"})
def column_mappings_asset() -> dict:
    return load_mappings(name="columns")


@dg.asset(tags={"domain": "symbols"})
def nasdaq_symbols_asset(column_mappings_asset: dict) -> DataFrame:
    return nasdaq_symbols_schema.validate(
        rename_and_select_columns(df=NasdaqDownloader().df, mappings=column_mappings_asset.get("nasdaq_symbols"))
    )


@dg.asset(tags={"domain": "symbols"})
def other_stock_symbols_asset(column_mappings_asset: dict) -> DataFrame:
    return other_symbols_schema.validate(
        rename_and_select_columns(df=StockSymbolDownloader().df, mappings=column_mappings_asset.get("other_symbols"))
    )


@dg.asset(tags={"domain": "symbols"})
def index_symbols_asset(column_mappings_asset: dict) -> DataFrame:
    return indicies_schema.validate(rename_and_select_columns(df=GetIndexSymbols().df, mappings=column_mappings_asset.get("indicies")))


@dg.asset(tags={"domain": "symbols"})
def sector_etfs_asset() -> dict:
    return load_mappings(name="sector_etfs").get("sector_etfs")


@dg.asset(tags={"domain": "symbols"})
def select_symbols_asset(
    config_asset: dict,
    nasdaq_symbols_asset: DataFrame,
    other_stock_symbols_asset: DataFrame,
    sector_etfs_asset: dict,
    index_symbols_asset: DataFrame,
) -> symbolLists:
    return select_symbols(
        nasdaq_df=nasdaq_symbols_asset,
        other_df=other_stock_symbols_asset,
        sector_etfs=sector_etfs_asset,
        indicies_df=index_symbols_asset,
        get_etfs=config_asset.get("symbols").get("get_etfs"),
        get_sector_etfs=config_asset.get("symbols").get("get_sector_etfs"),
        get_dowjones=config_asset.get("symbols").get("get_dowjones"),
        get_nasdaq100=config_asset.get("symbols").get("get_nasdaq100"),
        get_sp500=config_asset.get("symbols").get("get_sp500"),
        sample=None,
    )


@dg.asset(tags={"domain": "yfinance"})
def equity_info_asset(select_symbols_asset: symbolLists, config_asset: dict, column_mappings_asset: dict) -> DataFrame:
    equity_info = YahooFinanceBatchDownloader(
        symbols=select_symbols_asset.equity, path=config_asset.get("data").get("temp_folder"), cls=YahooFinanceTickerInfo
    )
    return equity_info_schema.validate(rename_and_select_columns(df=equity_info.data, mappings=column_mappings_asset.get("equity_info")))


@dg.asset(tags={"domain": "yfinance"})
def etf_info_asset(select_symbols_asset: symbolLists, config_asset: dict, column_mappings_asset: dict) -> DataFrame:
    etf_info = YahooFinanceBatchDownloader(
        symbols=select_symbols_asset.etf, path=config_asset.get("data").get("temp_folder"), cls=YahooFinanceTickerInfo
    )
    return etf_info_schema.validate(rename_and_select_columns(df=etf_info.data, mappings=column_mappings_asset.get("etf_info")))


@dg.asset(tags={"domain": "yfinance"})
def price_asset(select_symbols_asset: symbolLists, config_asset: dict) -> DataFrame:
    all_symbols = sorted(set(select_symbols_asset.etf + select_symbols_asset.equity))
    return YahooFinanceBatchDownloader(
        symbols=all_symbols, path=config_asset.get("data").get("temp_folder"), cls=YahooFinancePriceHistory
    ).data


@dg.asset(tags={"domain": "validation"})
def price_validation_asset(price_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    return price_schema.validate(rename_and_select_columns(df=price_asset, mappings=column_mappings_asset.get("price")))


@dg.asset(tags={"domain": "regression"})
def run_regression_asset(price_asset: DataFrame, config_asset: dict) -> DataFrame:
    return run_all_regression(price_df=price_asset, regression_config=config_asset.get("regression"))


@dg.asset(tags={"domain": "validation"})
def regression_validation_asset(run_regression_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    return regression_schema.validate(rename_and_select_columns(df=run_regression_asset, mappings=column_mappings_asset.get("regression")))


@dg.asset(tags={"domain": "talib"})
def run_talib_asset(price_asset: DataFrame) -> DataFrame:
    return run_all_talib(data_df=price_asset, functions=talib_functions, pattern_columns=pattern_columns)


@dg.asset(tags={"domain": "validation"})
def talib_validation_asset(run_talib_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    return talib_schema.validate(rename_and_select_columns(df=run_talib_asset, mappings=column_mappings_asset.get("ta__talib")))


@dg.asset
def run_talib_ma_ratio_asset(price_asset: DataFrame, run_talib_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    df = price_asset.merge(run_talib_asset.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner")
    df = run_all_custom_ta(data_df=df, functions=custom_ta_sets__ma_ratio)
    df = rename_and_select_columns(df=df, mappings=column_mappings_asset.get("ta__ma_ratio"))
    return ta__ma_ratio_schema.validate(df)


@dg.asset
def run_talib_change_asset(price_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    df = run_all_custom_ta(data_df=price_asset, functions=custom_ta_sets__change_ratio)
    df = rename_and_select_columns(df=df, mappings=column_mappings_asset.get("ta__change"))
    return ta__change_schema.validate(df)


@dg.asset
def run_ma_future_asset(price_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    df = run_all_custom_ta(data_df=price_asset, functions=custom_ta_sets__future)
    df = rename_and_select_columns(df=df, mappings=column_mappings_asset.get("ma_future"))
    return ma_future_schema.validate(df)


@dg.asset
def run_regression_indicators_asset(price_asset: DataFrame, run_regression_asset: DataFrame) -> DataFrame:
    df = price_asset.merge(run_regression_asset.rename(columns={"date": "Date"}), on=["symbol", "Date"], how="inner")
    df = run_all_custom_ta(data_df=df, functions=custom_ta_sets__regression_channel)
    return df


@dg.asset(tags={"domain": "validation"})
def regression_indicators_validation_asset(run_regression_indicators_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    return regression_indicators_schema.validate(
        rename_and_select_columns(df=run_regression_indicators_asset, mappings=column_mappings_asset.get("regression_indicators"))
    )


@dg.asset
def run_regression_indicators_ma_asset(run_regression_indicators_asset: DataFrame, column_mappings_asset: dict) -> DataFrame:
    df = run_all_custom_ta(data_df=run_regression_indicators_asset, functions=custom_ta_sets__regression_channel_ma)
    df = rename_and_select_columns(df=df, mappings=column_mappings_asset.get("regression_indicators_ma"))
    return regression_indicators_ma_schema.validate(df)


@dg.asset
def write_to_database(
    config_asset: dict,
    regression_validation_asset: DataFrame,
    nasdaq_symbols_asset: DataFrame,
    other_stock_symbols_asset: DataFrame,
    index_symbols_asset: DataFrame,
    equity_info_asset: DataFrame,
    etf_info_asset: DataFrame,
    price_validation_asset: DataFrame,
    talib_validation_asset: DataFrame,
    run_talib_ma_ratio_asset: DataFrame,
    run_talib_change_asset: DataFrame,
    run_ma_future_asset: DataFrame,
    regression_indicators_validation_asset: DataFrame,
    run_regression_indicators_ma_asset: DataFrame,
) -> None:
    db = duckdb.connect(
        Path(config_asset.get("database").get("database_folder")) / f"{config_asset.get('database').get('database_name')}.db"
    )

    write_table(db=db, df=nasdaq_symbols_asset, table="nasdaq_symbols")
    write_table(db=db, df=other_stock_symbols_asset, table="other_symbols")
    write_table(db=db, df=index_symbols_asset, table="indicies")
    write_table(db=db, df=equity_info_asset, table="equity_info")
    write_table(db=db, df=etf_info_asset, table="etf_info")
    write_table(db=db, df=price_validation_asset, table="price")
    write_table(db=db, df=talib_validation_asset, table="talib")
    write_table(db=db, df=run_talib_change_asset, table="ta__change")
    write_table(db=db, df=run_talib_ma_ratio_asset, table="ta__ma_ratio")
    write_table(db=db, df=regression_validation_asset, table="regression")
    write_table(db=db, df=run_ma_future_asset, table="ma_future")
    write_table(db=db, df=regression_indicators_validation_asset, table="regression_indicators")
    write_table(db=db, df=run_regression_indicators_ma_asset, table="regression_indicators_ma")

    db.close()
