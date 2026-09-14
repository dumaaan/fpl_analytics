import json
import time
from datetime import datetime, timezone

import requests

from fpl_analytics.config import RAW_DIR, RAW_SNAPSHOTS_TO_KEEP

_session = requests.Session()


def fetch_json(url: str, retries: int = 2, backoff_seconds: float = 1.0) -> dict:
    """GET a URL as JSON, retrying on failure with linear backoff."""
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = _session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_seconds * (attempt + 1))
    assert last_error is not None
    raise last_error


def save_raw_json(name: str, data: dict | list) -> None:
    """Save a raw API response for reproducibility and debugging."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RAW_DIR / f"{stamp}_{name}.json"
    path.write_text(json.dumps(data))


def prune_raw_snapshots(keep: int = RAW_SNAPSHOTS_TO_KEEP) -> None:
    """Keep only the newest `keep` snapshots of each response type."""
    by_name: dict[str, list] = {}
    for path in RAW_DIR.glob("*_*.json"):
        name = path.stem.split("_", 1)[1]
        by_name.setdefault(name, []).append(path)
    for paths in by_name.values():
        for old in sorted(paths, reverse=True)[keep:]:
            old.unlink()
