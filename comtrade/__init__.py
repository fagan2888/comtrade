import os
from .core import Comtrade
from .util import (
    DATA_DIR, KEY_ENV_NAME, KEY_FILE_NAME, DEFAULT_API_URL, get_partner_areas,
    get_reporter_areas, get_trade_regimes, get_classification
)

__version__ = "0.0.1"

if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR)
