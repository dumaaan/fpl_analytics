from fpl_analytics.config import FIXTURES_URL
from fpl_analytics.fetch.client import fetch_json, save_raw_json


def fetch_fixtures() -> list[dict]:
    """Fetch every fixture this season, with difficulty ratings."""
    data = fetch_json(FIXTURES_URL)
    save_raw_json("fixtures", data)
    return data
