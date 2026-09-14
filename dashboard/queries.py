import duckdb
import pandas as pd
import streamlit as st

from fpl_analytics.config import DB_PATH


def _db_version() -> float:
    """Changes when the pipeline swaps in a new database file, so cached
    queries refresh on their own after a pipeline run."""
    return DB_PATH.stat().st_mtime if DB_PATH.exists() else 0.0


def _read(sql: str, params: list | None = None) -> pd.DataFrame:
    """Run one query on a short-lived read-only connection. Holding no open
    connection between queries lets the pipeline replace the file at any time."""
    if not DB_PATH.exists():
        return pd.DataFrame()
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as conn:
            return conn.execute(sql, params or []).fetchdf()
    except duckdb.CatalogException:
        # Database from an older pipeline version: a table is missing.
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def _players(version: float) -> pd.DataFrame:
    return _read("SELECT * FROM mart_players")


@st.cache_data(ttl=3600)
def _player_gameweeks(version: float) -> pd.DataFrame:
    return _read("SELECT * FROM mart_player_gameweeks ORDER BY player_id, round, kickoff_time")


@st.cache_data(ttl=3600)
def _teams(version: float) -> pd.DataFrame:
    return _read("SELECT * FROM stg_teams")


@st.cache_data(ttl=3600)
def _team_fixtures(version: float) -> pd.DataFrame:
    return _read("SELECT * FROM mart_team_fixtures ORDER BY team_id, event, kickoff_time")


@st.cache_data(ttl=3600)
def _events(version: float) -> pd.DataFrame:
    return _read("SELECT * FROM stg_events ORDER BY id")


@st.cache_data(ttl=3600)
def _last_refresh(version: float) -> pd.Timestamp | None:
    df = _read("SELECT refreshed_at FROM data_refresh_log ORDER BY refreshed_at DESC LIMIT 1")
    return None if df.empty else df.iloc[0]["refreshed_at"]


def get_all_players() -> pd.DataFrame:
    return _players(_db_version())


def get_player_gameweeks() -> pd.DataFrame:
    return _player_gameweeks(_db_version())


def get_player_history(player_id: int) -> pd.DataFrame:
    history = get_player_gameweeks()
    if history.empty:
        return history
    return history[history["player_id"] == player_id].reset_index(drop=True)


def get_teams() -> pd.DataFrame:
    return _teams(_db_version())


def get_team_fixtures() -> pd.DataFrame:
    return _team_fixtures(_db_version())


def get_events() -> pd.DataFrame:
    return _events(_db_version())


def get_last_refresh() -> pd.Timestamp | None:
    return _last_refresh(_db_version())
