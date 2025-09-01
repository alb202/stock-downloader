import toml


def load_config() -> dict:
    url = "src/stock_downloader/config/config.toml"
    return toml.load(open(url, "r"))


def load_column_mappings() -> dict:
    url = "src/stock_downloader/models/column_mappings.toml"
    return toml.load(open(url, "r"))
