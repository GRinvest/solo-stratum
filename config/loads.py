import pathlib
import sys

import pytoml as toml
from loguru import logger

from config import schemas

BASE_DIR = pathlib.Path(__file__).parent.parent
DEFAULT_CONFIG_PATH = BASE_DIR / 'config' / '.stratum-config.toml'


def load_config(path=DEFAULT_CONFIG_PATH) -> schemas.Config:
    try:
        with open(path) as f:
            conf = toml.load(f)
    except FileNotFoundError:
        logger.remove()
        logger.add(
            sys.stdout, format="<r>{level}:</r> <W><bold><k>{message}</k></bold></W>")
        logger.warning(
            f"I created a template in {path}, please fill it with the required data and start the server again")

        with open(path, 'w') as f:
            toml.dump(schemas.Config(
                general={},
                server={},
                coind={},
            ).dict(), f)
        raise SystemExit(1)
    else:
        return schemas.Config(**conf)


data = load_config()
