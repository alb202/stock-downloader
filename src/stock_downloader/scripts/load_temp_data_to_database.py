from stock_downloader.schemas.equity_info import equity_info_schema
from stock_downloader.schemas.etf_info import etf_info_schema
from stock_downloader.schemas.price import price_schema
from stock_downloader.schemas.talib import talib_schema
from stock_downloader.schemas.ta__change import ta__change_schema
from stock_downloader.schemas.ta__ma_ratio import ta__ma_ratio_schema
from stock_downloader.schemas.regression import regression_schema
from stock_downloader.schemas.regression_indicators import regression_indicators_schema
from stock_downloader.schemas.regression_indicators_ma import regression_indicators_ma_schema
from stock_downloader.schemas.ma_future import ma_future_schema

# from stock_downloader.schemas.indicies import indicies_schema
# from stock_downloader.schemas.nasdaq_symbols import nasdaq_symbols_schema
# from stock_downloader.schemas.other_symbols import other_symbols_schema

from stock_downloader.utilities import validate_folder, rename_and_select_columns
from stock_downloader.database.db import write_table
from stock_downloader.data.loaders import load_mappings, load_config
import duckdb
from pandas import read_parquet

# Load config and column mappings
config = load_config()
column_mappings = load_mappings(name="columns")

# Set folders
output_folder = validate_folder(path=config.get("data").get("output_folder"))
db_folder = validate_folder(path=config.get("database").get("database_folder"))

# Load temp files
nasdaq_symbols_df = read_parquet(output_folder / "nasdaq_symbols.parquet")
other_symbols_df = read_parquet(output_folder / "other_symbols.parquet")
index_symbols_df = read_parquet(output_folder / "index_symbols.parquet")
regression_indicators_df = read_parquet(path=output_folder / f"{'regression_indicators'}.parquet")
regression_indicators_ma_df = read_parquet(output_folder / "regression_indicators_ma.parquet")
ma_future_df = read_parquet(path=output_folder / f"{'ta__future'}.parquet")
ta__change__df = read_parquet(path=output_folder / f"{'ta__change'}.parquet")
ta__ma_ratio__df = read_parquet(path=output_folder / f"{'ta__ma_ratio'}.parquet")
talib__df = read_parquet(path=output_folder / f"{'ta_talib'}.parquet")
regression_df = read_parquet(path=output_folder / f"{'regression_data'}.parquet")
price_df = read_parquet(path=output_folder / f"{'yahoo_price'}.parquet")
equity_info_df = read_parquet(path=output_folder / f"{'equity_info'}.parquet")
etf_info_df = read_parquet(path=output_folder / f"{'etf_info'}.parquet")


regression_indicators_df = rename_and_select_columns(df=regression_indicators_df, mappings=column_mappings.get("regression_indicators"))
regression_indicators_ma_df = rename_and_select_columns(
    df=regression_indicators_ma_df, mappings=column_mappings.get("regression_indicators_ma")
)
ma_future_df = rename_and_select_columns(df=ma_future_df, mappings=column_mappings.get("ma_future"))
ta__change__df = rename_and_select_columns(df=ta__change__df, mappings=column_mappings.get("ta__change"))
ta__ma_ratio__df = rename_and_select_columns(df=ta__ma_ratio__df, mappings=column_mappings.get("ta__ma_ratio"))
talib__df = rename_and_select_columns(df=talib__df, mappings=column_mappings.get("ta__talib"))
regression_df = rename_and_select_columns(df=regression_df, mappings=column_mappings.get("regression"))
price_df = rename_and_select_columns(df=price_df, mappings=column_mappings.get("price"))
equity_info_df = rename_and_select_columns(df=equity_info_df, mappings=column_mappings.get("equity_info"))
etf_info_df = rename_and_select_columns(df=etf_info_df, mappings=column_mappings.get("etf_info"))


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
