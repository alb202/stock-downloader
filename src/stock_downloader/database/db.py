from duckdb import DuckDBPyConnection
from pandas import DataFrame


def write_table(db: DuckDBPyConnection, df: DataFrame, table: str) -> None:
    db.execute(f"DROP TABLE IF EXISTS {table}")
    db.register("df", df)
    db.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
