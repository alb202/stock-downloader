from pandas import DataFrame, Series, concat
from functools import reduce
import talib as ta

from stock_downloader.utilities import downcast_numeric_columns


def run_talib_functions(df: DataFrame, talib_functions: list[dict], pattern_columns: list[str]) -> DataFrame:
    # Store each function's result dataframe

    input_map = {
        "open": df["Open"],
        "high": df["High"],
        "low": df["Low"],
        "close": df["Close"],
        "volume": df["Volume"],
        "real": df["Close"],
    }

    results = []

    for function_set in talib_functions:
        func_name = list(function_set.keys())[0]
        params = function_set.get(func_name)

        try:
            func = getattr(ta, func_name)
        except AttributeError as e:
            print(f"Invalid talib function: {e}")
            continue
        # sig = inspect.signature(func)
        # params = sig.parameters
        param_value_list = []
        try:
            # Build argument list for this TA-Lib function
            args = {}
            # print(func, params)
            for pname in params.keys():
                if pname in input_map:
                    args[pname] = input_map[pname]
                else:
                    args[pname] = params.get(pname)
                    param_value_list.append(str(params.get(pname)))
                # elif sig.parameters[pname].default != inspect.Parameter.empty:
                #     args[pname] = sig.parameters[pname].default
                # else:
                #     raise ValueError(f"Missing required input: {pname}")

            # Call the function
            output = func(**args)

            # Handle single or multiple outputs
            if isinstance(output, Series) and len(param_value_list) == 0:
                df_out = output.rename(f"{func_name}").to_frame()
            elif isinstance(output, Series):
                df_out = output.rename(f"{func_name}_{'_'.join(param_value_list)}").to_frame()
            elif isinstance(output, tuple):
                df_out = concat([s.rename(f"{func_name}_{'_'.join(param_value_list)}__{i + 1}") for i, s in enumerate(output)], axis=1)
            elif isinstance(output, DataFrame):
                df_out = output.rename(columns=lambda c: f"{func_name}_{c}")
            else:
                continue  # skip unsupported outputs

            # Add Date and symbol
            df_out["Date"] = df["Date"]
            df_out["symbol"] = df["symbol"]
            df_out = df_out.set_index(["Date", "symbol"])

            results.append(df_out)

        except (ValueError, Exception) as e:
            print(f"# Skipped {func_name}: {e}")

    # Merge all results into a single DataFrame

    if results:
        final_df = reduce(lambda left, right: left.join(right, how="outer"), results)
    else:
        final_df = DataFrame()

    for col in final_df.columns:
        if col in pattern_columns:
            final_df[col] = final_df[col] / 100

    # Show the final result
    return final_df.dropna(how="all", axis=1)  # .dropna(how='any', axis=0)


def run_custom_ta(df: DataFrame, custom_ta: list[dict]) -> DataFrame:
    # Store each function's result dataframe
    results = []

    for function_set in custom_ta:
        output_name = function_set.get("output")
        func = function_set.get("func")
        args = {k: df[v] for k, v in function_set.get("columns").items()}

        try:
            # Call the function
            output = func(**args)
            df_out = output.rename(output_name).to_frame()

            # Add Date and symbol
            df_out["Date"] = df["Date"]
            df_out["symbol"] = df["symbol"]
            df_out = df_out.set_index(["Date", "symbol"])

            results.append(df_out)

        except (ValueError, Exception) as e:
            print(f"# Skipped column {output_name}: {e}")

    # # Merge all results into a single DataFrame
    if results:
        final_df = reduce(lambda left, right: left.join(right, how="outer"), results)
    else:
        final_df = DataFrame()

    # Sort and return the final result
    return final_df.dropna(how="all", axis=1)  # .dropna(how='any', axis=0)


def concatenate_ta_results(dfs: list[DataFrame], symbol_col: str = "symbol", date_col: str = "Date") -> DataFrame:
    df = concat(dfs, axis=0).sort_values([symbol_col, date_col]).reset_index(drop=False)
    df = downcast_numeric_columns(df)
    return df
