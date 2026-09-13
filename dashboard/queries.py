import duckdb
import pandas as pd
import streamlit as st

from fpl_analytics.config import DB_PATH


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def get_all_players() -> pd.DataFrame:
    return get_connection().execute("SELECT * FROM mart_players").fetchdf()


@st.cache_data(ttl=300)
def get_teams() -> pd.DataFrame:
    return get_connection().execute("SELECT * FROM stg_teams").fetchdf()


@st.cache_data(ttl=300)
def get_fixtures() -> pd.DataFrame:
    return get_connection().execute("SELECT * FROM stg_fixtures").fetchdf()


@st.cache_data(ttl=300)
def get_player_history(player_id: int) -> pd.DataFrame:
    return (
        get_connection()
        .execute(
            "SELECT * FROM stg_player_gameweek_history WHERE player_id = ? ORDER BY round",
            [player_id],
        )
        .fetchdf()
    )


@st.cache_data(ttl=300)
def get_last_refresh() -> pd.Timestamp | None:
    df = get_connection().execute(
        "SELECT refreshed_at FROM data_refresh_log ORDER BY refreshed_at DESC LIMIT 1"
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]["refreshed_at"]
