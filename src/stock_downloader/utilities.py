from pandas import to_numeric, DataFrame
from pathlib import Path


def downcast_numeric_columns(df):
    """
    Downcast numeric columns in a DataFrame to the smallest possible numeric dtype.
    Returns a new DataFrame with optimized dtypes.
    """
    df_optimized = df.copy()
    for col in df_optimized.select_dtypes(include=["int", "float"]).columns:
        col_type = df_optimized[col].dtype
        if "int" in str(col_type):
            df_optimized[col] = to_numeric(df_optimized[col], downcast="integer")
        elif "float" in str(col_type):
            df_optimized[col] = to_numeric(df_optimized[col], downcast="float")
    return df_optimized


def camel_to_snake(text):
    snake_case_text = ""
    for char in text:
        if char.isupper():
            if snake_case_text:  # Add underscore only if not the first character
                snake_case_text += "_"
            snake_case_text += char.lower()
        else:
            snake_case_text += char
    return snake_case_text


def validate_folder(path: str, exist_ok: bool = True, parents: bool = True) -> Path:
    path_ = Path(path)
    path_.mkdir(exist_ok=exist_ok, parents=parents)
    return path_


def rename_and_select_columns(df: DataFrame, mappings: dict) -> DataFrame:
    return df.loc[:, [col for col in df.columns if col in mappings.keys()]].rename(columns=mappings)
