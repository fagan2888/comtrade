import os
import requests
import pandas as pd

DEFAULT_API_URL = "http://comtrade.un.org/api/"
KEY_ENV_NAME = "COMTRADE_TOKEN"
KEY_FILE_NAME = os.path.join(os.path.expanduser("~"), ".comtraderc")
DATA_DIR = os.path.join(os.path.expanduser("~"), ".comtrade", "data")


def update_metadata_file(url):
    print("Updating url ", url)
    r = requests.get(url)
    assert r.ok
    js = r.json()
    assert not js["more"]

    df = pd.DataFrame(js["results"])
    fn = os.path.join(DATA_DIR, os.path.basename(url).split(".")[0])
    print("saving to: ", fn)
    df.to_csv(fn + ".csv")
    return df


def update_metadata_files():
    urls = ["https://comtrade.un.org/data/cache/reporterAreas.json",
            "https://comtrade.un.org/data/cache/partnerAreas.json",
            "https://comtrade.un.org/data/cache/tradeRegimes.json",
            "https://comtrade.un.org/data/cache/classificationHS.json",
            "https://comtrade.un.org/data/cache/classificationH0.json",
            "https://comtrade.un.org/data/cache/classificationH1.json",
            "https://comtrade.un.org/data/cache/classificationH2.json",
            "https://comtrade.un.org/data/cache/classificationH3.json",
            "https://comtrade.un.org/data/cache/classificationH4.json",
            "https://comtrade.un.org/data/cache/classificationST.json",
            "https://comtrade.un.org/data/cache/classificationS1.json",
            "https://comtrade.un.org/data/cache/classificationS2.json",
            "https://comtrade.un.org/data/cache/classificationS3.json",
            "https://comtrade.un.org/data/cache/classificationS4.json",
            "https://comtrade.un.org/data/cache/classificationBEC.json",
            "https://comtrade.un.org/data/cache/classificationEB02.json"]

    for url in urls:
        update_metadata_file(url)


def _get_metadata_file(root_name):
    fn = os.path.join(DATA_DIR, root_name + ".csv")
    if os.path.isfile(fn):
        return pd.read_csv(fn, index_col=0)
    else:
        url = f"https://comtrade.un.org/data/cache/{root_name}.json"
        return update_metadata_file(url)


def get_partner_areas():
    return _get_metadata_file("partnerAreas")


def get_reporter_areas():
    return _get_metadata_file("reporterAreas")


def get_trade_regimes():
    return _get_metadata_file("tradeRegimes")


def get_classification(n):
    allowed = ["HS", "H0", "H1", "H2", "H3", "H4", "ST", "S1", "S2", "S3",
               "S4", "BEC", "EB02"]
    if n not in allowed:
        m = f"classification system {n} not known. Please use one of\n"
        m += ", ".join(allowed)
        raise ValueError(n)

    return _get_metadata_file("classification" + n)
