from datetime import datetime, timezone

import pandas as pd

from fpl_analytics.db import get_connection, load_dataframe
from fpl_analytics.fetch.bootstrap import fetch_bootstrap_static
from fpl_analytics.fetch.fixtures import fetch_fixtures
from fpl_analytics.fetch.player_history import fetch_all_player_history
from fpl_analytics.transform.marts import build_mart_players
from fpl_analytics.transform.staging import (
    build_fixtures_df,
    build_player_gameweek_history_df,
    build_players_raw_df,
    build_teams_df,
)


def run() -> None:
    print("Fetching bootstrap-static (players + teams)...")
    bootstrap_data = fetch_bootstrap_static()
    teams_df = build_teams_df(bootstrap_data)
    players_raw_df = build_players_raw_df(bootstrap_data)
    print(f"  {len(teams_df)} teams, {len(players_raw_df)} players")

    print("Fetching fixtures...")
    fixtures_data = fetch_fixtures()
    fixtures_df = build_fixtures_df(fixtures_data)
    print(f"  {len(fixtures_df)} fixtures")

    print(f"Fetching gameweek history for {len(players_raw_df)} players...")
    history_by_player = fetch_all_player_history(players_raw_df["id"].tolist())
    history_df = build_player_gameweek_history_df(history_by_player)
    print(f"  {len(history_df)} player-gameweek rows")

    print("Building mart_players...")
    mart_players_df = build_mart_players(players_raw_df, teams_df, history_df)

    print("Loading tables into DuckDB...")
    conn = get_connection()
    try:
        load_dataframe(conn, "stg_teams", teams_df)
        load_dataframe(conn, "stg_fixtures", fixtures_df)
        load_dataframe(conn, "stg_player_gameweek_history", history_df)
        load_dataframe(conn, "mart_players", mart_players_df)

        log_row = pd.DataFrame(
            [
                {
                    "refreshed_at": datetime.now(timezone.utc),
                    "player_count": len(mart_players_df),
                    "history_row_count": len(history_df),
                    "status": "success",
                }
            ]
        )
        conn.register("log_row", log_row)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS data_refresh_log AS SELECT * FROM log_row WHERE 1=0"
        )
        conn.execute("INSERT INTO data_refresh_log SELECT * FROM log_row")
        conn.unregister("log_row")
    finally:
        conn.close()

    print(
        f"Done. {len(teams_df)} teams, {len(fixtures_df)} fixtures, "
        f"{len(mart_players_df)} players, {len(history_df)} player-gameweek rows."
    )


if __name__ == "__main__":
    run()
