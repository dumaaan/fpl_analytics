import fcntl
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import pandas as pd

from fpl_analytics.config import DATA_DIR, DB_PATH, PIPELINE_LOCK_PATH, TMP_DB_PATH
from fpl_analytics.db import copy_refresh_log, get_connection, load_dataframe
from fpl_analytics.fetch.bootstrap import fetch_bootstrap_static
from fpl_analytics.fetch.client import prune_raw_snapshots
from fpl_analytics.fetch.fixtures import fetch_fixtures
from fpl_analytics.fetch.player_history import fetch_all_player_history
from fpl_analytics.transform.marts import attach_team_xg, build_mart_player_gameweeks, build_mart_players
from fpl_analytics.transform.scoring import Scoring
from fpl_analytics.transform.staging import (
    build_events_df,
    next_event_id,
    build_fixtures_df,
    build_player_gameweek_history_df,
    build_players_raw_df,
    build_teams_df,
)
from fpl_analytics.transform.teams import (
    build_team_fixture_results,
    build_team_ratings,
    build_upcoming_team_fixtures,
)


class PipelineBusy(RuntimeError):
    """Another pipeline run holds the lock."""


@contextmanager
def pipeline_lock():
    """Exclusive lock for one pipeline run. Two runs would write the same
    temporary database file. The lock ends with the process, even on a crash."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PIPELINE_LOCK_PATH, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PipelineBusy("Another data refresh is running. Wait for it to finish.") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run() -> None:
    with pipeline_lock():
        _run()


def _run() -> None:
    print("Fetching bootstrap-static (players, teams, gameweeks, scoring)...")
    bootstrap_data = fetch_bootstrap_static()
    scoring = Scoring.from_bootstrap(bootstrap_data)
    teams_df = build_teams_df(bootstrap_data)
    events_df = build_events_df(bootstrap_data)
    players_raw_df = build_players_raw_df(bootstrap_data)
    print(f"  {len(teams_df)} teams, {len(players_raw_df)} players, {len(events_df)} gameweeks")

    print("Fetching fixtures...")
    fixtures_df = build_fixtures_df(fetch_fixtures())
    print(f"  {len(fixtures_df)} fixtures")

    print(f"Fetching gameweek history for {len(players_raw_df)} players...")
    history_by_player = fetch_all_player_history(players_raw_df["id"].tolist())
    history_df = build_player_gameweek_history_df(history_by_player)
    print(f"  {len(history_df)} player-gameweek rows")

    print("Building marts...")
    player_gameweeks_df = build_mart_player_gameweeks(history_df, players_raw_df, fixtures_df, scoring)
    team_results_df = build_team_fixture_results(player_gameweeks_df, fixtures_df)
    player_gameweeks_df = attach_team_xg(player_gameweeks_df, team_results_df)
    team_ratings_df = build_team_ratings(team_results_df, teams_df, fixtures_df)
    upcoming_df = build_upcoming_team_fixtures(fixtures_df, team_ratings_df, next_event_id(events_df))
    mart_players_df = build_mart_players(
        players_raw_df, teams_df, player_gameweeks_df, team_ratings_df, upcoming_df, scoring
    )

    unexplained = player_gameweeks_df["pts_unexplained"].abs().gt(0)
    if unexplained.any():
        print(
            f"  Warning: {int(unexplained.sum())} player-gameweek rows have points the scoring "
            "rules do not explain. Check game_config.scoring for a rule change."
        )

    print("Loading tables into DuckDB...")
    TMP_DB_PATH.unlink(missing_ok=True)
    conn = get_connection(TMP_DB_PATH)
    try:
        load_dataframe(conn, "stg_teams", teams_df)
        load_dataframe(conn, "stg_events", events_df)
        load_dataframe(conn, "stg_fixtures", fixtures_df)
        load_dataframe(conn, "mart_player_gameweeks", player_gameweeks_df)
        load_dataframe(conn, "mart_team_fixture_results", team_results_df)
        load_dataframe(conn, "mart_team_ratings", team_ratings_df)
        load_dataframe(conn, "mart_team_fixtures", upcoming_df)
        load_dataframe(conn, "mart_players", mart_players_df)

        copy_refresh_log(conn, DB_PATH)
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
        conn.execute("CREATE TABLE IF NOT EXISTS data_refresh_log AS SELECT * FROM log_row WHERE 1=0")
        conn.execute("INSERT INTO data_refresh_log SELECT * FROM log_row")
        conn.unregister("log_row")
    finally:
        conn.close()

    # Atomic swap: readers see either the old database or the new one.
    os.replace(TMP_DB_PATH, DB_PATH)
    prune_raw_snapshots()

    print(
        f"Done. {len(teams_df)} teams, {len(fixtures_df)} fixtures, "
        f"{len(mart_players_df)} players, {len(history_df)} player-gameweek rows."
    )


if __name__ == "__main__":
    run()
