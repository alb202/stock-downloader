import toml
from pathlib import Path


def load_config() -> dict:
    path = Path(__file__).parents[1] / "config" / "config.toml"
    return toml.load(open(path, "r"))


def load_column_mappings() -> dict:
    path = Path(__file__).parents[1] / "models" / "column_mappings.toml"
    return toml.load(open(path, "r"))
