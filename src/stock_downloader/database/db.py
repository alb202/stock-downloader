from duckdb import DuckDBPyConnection, IOException, connect
from pandas import DataFrame


def write_table(db: DuckDBPyConnection, df: DataFrame, table: str) -> None:
    db.execute(f"DROP TABLE IF EXISTS {table}")
    db.register("df", df)
    db.execute(f"CREATE TABLE {table} AS SELECT * FROM df")


def check_database(db_path: str) -> bool:
    try:
        db = connect(db_path)
        db.close()
        return True
    except IOException as e:
        print(f"Database connection error: {e}")
        return False
