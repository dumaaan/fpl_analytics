"""Load a season's per-match history for the backtest, in one shape for all sources.

`history` has the mart_player_gameweeks columns the model needs (player_id,
fixture_id, round, team_id, position, minutes, starts, xG/xA/xGC, saves,
bonus, cards, defensive_contribution, total_points, price, selected).
"""

import glob
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from fpl_analytics.config import DB_PATH, EXTERNAL_DIR, RAW_DIR
from fpl_analytics.transform.scoring import Scoring
from fpl_analytics.transform.teams import assign_team_ids


@dataclass
class Season:
    name: str
    history: pd.DataFrame
    fixtures: pd.DataFrame
    teams: pd.DataFrame
    scoring: Scoring
    names: pd.Series  # player_id -> display name
    last_complete_gw: int


def _last_complete_gw(fixtures: pd.DataFrame) -> int:
    by_event = fixtures.dropna(subset=["event"]).groupby("event")["finished"].all()
    complete = by_event[by_event].index
    return int(complete.max()) if len(complete) else 0


def current_scoring() -> Scoring:
    """Scoring rules from the newest bootstrap-static snapshot in data/raw."""
    paths = sorted(glob.glob(str(RAW_DIR / "*_bootstrap_static.json")))
    if not paths:
        raise FileNotFoundError("No bootstrap-static snapshot in data/raw. Run `poetry run fpl-pipeline` first.")
    return Scoring.from_bootstrap(json.loads(Path(paths[-1]).read_text()))


def load_external_season(name: str, scoring: Scoring | None = None) -> Season:
    """A past season from the vaastav/Fantasy-Premier-League CSV layout in
    data/external/<name>/: merged_gw.csv, fixtures.csv, teams.csv."""
    folder = EXTERNAL_DIR / name
    gw = pd.read_csv(folder / "merged_gw.csv")
    fixtures = pd.read_csv(folder / "fixtures.csv")
    teams = pd.read_csv(folder / "teams.csv")

    fixtures["finished"] = fixtures["finished"].astype(str).str.lower() == "true"
    fixtures["event"] = pd.to_numeric(fixtures["event"], errors="coerce").astype("Int64")
    fixtures["kickoff_time"] = pd.to_datetime(fixtures["kickoff_time"], utc=True, errors="coerce")

    history = gw.rename(columns={"element": "player_id", "fixture": "fixture_id",
                                 "opponent_team": "opponent_team_id"})
    history["round"] = history["GW"].astype(int)
    history["position"] = history["position"].replace({"GKP": "GK"})
    history["price"] = history["value"] / 10.0
    history["was_home"] = history["was_home"].astype(str).str.lower() == "true"
    history["kickoff_time"] = pd.to_datetime(history["kickoff_time"], utc=True, errors="coerce")
    history["team_id"] = assign_team_ids(history, fixtures)

    # The dataset's "xP" column is not used: it is not a pre-deadline
    # prediction (it averages 0.5 points against 2.7 actual, and it is 0 for
    # almost every player who then did not play), so it leaks the outcome.
    names = history.groupby("player_id")["name"].last()
    return Season(
        name=name,
        history=history,
        fixtures=fixtures,
        teams=teams,
        scoring=scoring or current_scoring(),
        names=names,
        last_complete_gw=_last_complete_gw(fixtures),
    )


def load_current_season() -> Season:
    """This season from the pipeline's DuckDB file."""
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        history = conn.execute("SELECT * FROM mart_player_gameweeks").fetchdf()
        fixtures = conn.execute("SELECT * FROM stg_fixtures").fetchdf()
        teams = conn.execute("SELECT * FROM stg_teams").fetchdf()
        names = conn.execute("SELECT id, web_name FROM mart_players").fetchdf().set_index("id")["web_name"]
    fixtures["finished"] = fixtures["finished"].astype(bool)
    return Season(
        name="current",
        history=history,
        fixtures=fixtures,
        teams=teams,
        scoring=current_scoring(),
        names=names,
        last_complete_gw=_last_complete_gw(fixtures),
    )
