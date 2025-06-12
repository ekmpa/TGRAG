import pathlib


def get_root_dir() -> pathlib.Path:
    return pathlib.Path(__file__).parent.parent


def get_cwd() -> pathlib.Path:
    return pathlib.Path.cwd()
