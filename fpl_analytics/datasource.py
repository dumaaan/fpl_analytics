"""Where the dashboard gets its database file.

Locally the pipeline writes `data/fpl.duckdb` and nothing else is needed.
When the app is hosted (Streamlit Community Cloud), it has no pipeline, so it
downloads the file that the scheduled refresh published. Set the source with
the `FPL_DB_URL` secret or environment variable.
"""

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from fpl_analytics.config import DATA_DIR, DB_PATH

# A downloaded copy is reused for this long before we ask the server again.
DOWNLOAD_TTL_SECONDS = 30 * 60
DOWNLOAD_TIMEOUT_SECONDS = 60
SOURCE_STATE_PATH = DATA_DIR / ".db_source.json"


def database_url() -> str | None:
    """The published database URL, from Streamlit secrets or the environment.
    None means local mode: use the file the pipeline wrote."""
    url = os.environ.get("FPL_DB_URL")
    if url:
        return url.strip() or None
    try:
        import streamlit as st

        return str(st.secrets["FPL_DB_URL"]).strip() or None
    except Exception:  # noqa: BLE001 - no secrets file, or not running in Streamlit
        return None


def _read_state() -> dict:
    try:
        return json.loads(SOURCE_STATE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_state(state: dict) -> None:
    SOURCE_STATE_PATH.write_text(json.dumps(state, indent=2))


def _age_seconds(state: dict) -> float:
    checked = state.get("checked_at")
    if not checked:
        return float("inf")
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(checked)).total_seconds()
    except ValueError:
        return float("inf")


def ensure_database(url: str | None = None, ttl_seconds: float = DOWNLOAD_TTL_SECONDS,
                    session: requests.Session | None = None) -> str:
    """Make sure `DB_PATH` holds the newest published database.

    Returns what happened: "local" (no URL set), "fresh" (the copy on disk is
    recent enough), "unchanged" (the server has the same file), "downloaded",
    or "failed" (kept any existing copy). Downloads are atomic, so a reader
    never sees a half-written file.
    """
    url = url or database_url()
    if not url:
        return "local"

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = _read_state()
    if DB_PATH.exists() and _age_seconds(state) < ttl_seconds:
        return "fresh"

    http = session or requests
    headers = {"User-Agent": "fpl-analytics"}
    try:
        if DB_PATH.exists() and state.get("etag"):
            head = http.head(url, headers=headers, timeout=DOWNLOAD_TIMEOUT_SECONDS, allow_redirects=True)
            if head.ok and head.headers.get("ETag") == state["etag"]:
                _write_state({**state, "checked_at": datetime.now(timezone.utc).isoformat()})
                return "unchanged"

        response = http.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(dir=DATA_DIR, suffix=".download", delete=False) as tmp:
            temp_path = Path(tmp.name)
            for chunk in response.iter_content(chunk_size=1 << 20):
                tmp.write(chunk)
        shutil.move(str(temp_path), DB_PATH)  # atomic within the same directory
        _write_state({"etag": response.headers.get("ETag"), "url": url,
                      "checked_at": datetime.now(timezone.utc).isoformat()})
        return "downloaded"
    except requests.RequestException:
        return "failed"
