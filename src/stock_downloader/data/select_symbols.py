from pandas import DataFrame, concat

# symbols = sorted(
#     set(nasdaq_symbols_df.query('ETF == "N"')["symbol"].to_list() + other_symbols_df.query('ETF == "Y"')["symbol"].to_list())
# )
# sector_etfs = list(load_mappings(name="sector_etfs").get("sector_etfs").values())
# symbols = [
#     symbol
#     for symbol in symbols
#     if symbol in index_symbols.df.query("sp500 == True | dowjones == False | nasdaq100 == False")["symbol"].to_list() + sector_etfs
# ][:100]


def select_symbols(
    nasdaq_df: DataFrame,
    other_df: DataFrame,
    sector_etfs: dict,
    indicies_df: DataFrame,
    get_etfs: bool = False,
    get_sector_etfs: bool = False,
    get_sp500: bool = False,
    get_dowjones: bool = False,
    get_nasdaq100: bool = False,
    sample: int | float = None,
) -> tuple[list, list]:
    equity_symbol_list: list = []
    etf_symbol_list: list = []
    symbols_df: DataFrame = concat([nasdaq_df, other_df], axis=0).merge(indicies_df, how="left", on="symbol")
    print("symbols_df", symbols_df)
    # get_sector_etfs = True if get_etfs else False

    if get_etfs:
        etf_symbol_list.append(symbols_df.query('etf == "Y"').symbol)
    if get_sector_etfs:
        etf_symbol_list.append(symbols_df.loc[symbols_df["symbol"].isin(sector_etfs.values())].query('etf == "Y"').symbol)
    if get_sp500:
        equity_symbol_list.append(symbols_df.query("sp500 == True").query('etf == "N"').symbol)
    if get_dowjones:
        equity_symbol_list.append(symbols_df.query("dowjones == True").query('etf == "N"').symbol)
    if get_nasdaq100:
        equity_symbol_list.append(symbols_df.query("nasdaq100 == True").query('etf == "N"').symbol)

    # unique_symbols = sorted(set(sum(symbols_list, [])))
    equity_symbols: list = concat(equity_symbol_list, axis=0).sort_values().drop_duplicates().to_list()
    etf_symbols: list = concat(etf_symbol_list, axis=0).sort_values().drop_duplicates().to_list()
    if sample:
        return equity_symbols[:sample], etf_symbols[:sample]
    return equity_symbols, etf_symbols
