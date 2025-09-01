import toml
from pathlib import Path


def load_config() -> dict:
    path = Path(__file__).parents[1] / "config" / "config.toml"
    return toml.load(open(path, "r"))


def load_mappings(name: str) -> dict:
    path = Path(__file__).parents[1] / "mappings" / f"{name}.toml"
    return toml.load(open(path, "r"))
