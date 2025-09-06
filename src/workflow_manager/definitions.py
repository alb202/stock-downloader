from workflow_manager.defs.assets import (
    config_asset,
    column_mappings_asset,
    nasdaq_symbols_asset,
    other_stock_symbols_asset,
    index_symbols_asset,
    sector_etfs_asset,
    select_symbols_asset,
    equity_info_asset,
    etf_info_asset,
    price_asset,
    price_validation_asset,
    run_regression_asset,
    write_to_database,
    run_talib_asset,
    talib_validation_asset,
    run_talib_ma_ratio_asset,
    run_talib_change_asset,
    run_ma_future_asset,
    run_regression_indicators_asset,
    regression_indicators_validation_asset,
    run_regression_indicators_ma_asset,
    regression_validation_asset,
)

# from pathlib import Path
from dagster_duckdb import DuckDBResource
from dagster import Definitions

database_resource = DuckDBResource(database="/tmp/jaffle_platform.duckdb")

defs = Definitions(
    assets=[
        config_asset,
        column_mappings_asset,
        nasdaq_symbols_asset,
        other_stock_symbols_asset,
        index_symbols_asset,
        sector_etfs_asset,
        select_symbols_asset,
        equity_info_asset,
        etf_info_asset,
        price_asset,
        price_validation_asset,
        run_regression_asset,
        regression_validation_asset,
        write_to_database,
        run_talib_asset,
        talib_validation_asset,
        run_talib_ma_ratio_asset,
        run_talib_change_asset,
        run_ma_future_asset,
        run_regression_indicators_asset,
        regression_indicators_validation_asset,
        run_regression_indicators_ma_asset,
    ],
    resources={
        "duckdb": database_resource,
    },
)


# @definitions
# def defs():
#     return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)


# import dagster as dg

# from dagster.defs.assets import test_asset

# defs = dg.Definitions(
#     assets=[test_asset],
# )
