from fpl_analytics.config import BOOTSTRAP_STATIC_URL
from fpl_analytics.fetch.client import fetch_json, save_raw_json


def fetch_bootstrap_static() -> dict:
    """Fetch players (elements) and teams in one call."""
    data = fetch_json(BOOTSTRAP_STATIC_URL)
    save_raw_json("bootstrap_static", data)
    return data
