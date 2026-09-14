import numpy as np
import pandas as pd

from fpl_analytics.config import POSITION_BY_ELEMENT_TYPE

TEAM_COLUMNS = [
    "id",
    "name",
    "short_name",
    "strength",
    "strength_overall_home",
    "strength_overall_away",
    "strength_attack_home",
    "strength_attack_away",
    "strength_defence_home",
    "strength_defence_away",
]

FIXTURE_COLUMNS = [
    "id",
    "event",
    "team_h",
    "team_a",
    "team_h_difficulty",
    "team_a_difficulty",
    "kickoff_time",
    "finished",
    "team_h_score",
    "team_a_score",
]

EVENT_COLUMNS = [
    "id",
    "name",
    "deadline_time",
    "finished",
    "is_previous",
    "is_current",
    "is_next",
    "average_entry_score",
    "highest_score",
    "most_captained",
    "most_vice_captained",
    "most_selected",
    "most_transferred_in",
    "top_element",
    "transfers_made",
]

CHIP_NAMES = ["wildcard", "freehit", "bboost", "3xc"]

PLAYER_RAW_NUMERIC_STRING_COLUMNS = [
    "points_per_game",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "selected_by_percent",
    "form",
    "value_form",
    "value_season",
    "ep_next",
    "ep_this",
]

PLAYER_RAW_COLUMNS = [
    "id",
    "code",
    "web_name",
    "first_name",
    "second_name",
    "team",
    "element_type",
    "now_cost",
    "total_points",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "starts",
    "transfers_in",
    "transfers_out",
    "transfers_in_event",
    "transfers_out_event",
    "cost_change_start",
    "cost_change_event",
    "dreamteam_count",
    "in_dreamteam",
    "chance_of_playing_next_round",
    "chance_of_playing_this_round",
    "status",
    "news",
    "event_points",
    "starts_per_90",
    "clean_sheets_per_90",
    "defensive_contribution",
    "defensive_contribution_per_90",
    "clearances_blocks_interceptions",
    "recoveries",
    "tackles",
    *PLAYER_RAW_NUMERIC_STRING_COLUMNS,
]

# Newer or less stable bootstrap fields. Missing ones load as empty instead of
# failing the whole pipeline.
PLAYER_OPTIONAL_COLUMNS = [
    "expected_goals_per_90",
    "expected_assists_per_90",
    "expected_goal_involvements_per_90",
    "expected_goals_conceded_per_90",
    "saves_per_90",
    "goals_conceded_per_90",
    "penalties_order",
    "corners_and_indirect_freekicks_order",
    "direct_freekicks_order",
    "price_change_percent",
    "price_change_projections",
]

HISTORY_NUMERIC_STRING_COLUMNS = [
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
]

HISTORY_COLUMNS = [
    "element",
    "fixture",
    "round",
    "opponent_team",
    "was_home",
    "kickoff_time",
    "total_points",
    "minutes",
    "starts",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "value",
    "selected",
    "transfers_in",
    "transfers_out",
    "transfers_balance",
    "defensive_contribution",
    "clearances_blocks_interceptions",
    "recoveries",
    "tackles",
    *HISTORY_NUMERIC_STRING_COLUMNS,
]


def build_teams_df(bootstrap_data: dict) -> pd.DataFrame:
    return pd.DataFrame(bootstrap_data["teams"])[TEAM_COLUMNS]


def build_events_df(bootstrap_data: dict) -> pd.DataFrame:
    events = bootstrap_data["events"]
    df = pd.DataFrame(events).reindex(columns=EVENT_COLUMNS)
    df["deadline_time"] = pd.to_datetime(df["deadline_time"], utc=True, errors="coerce")
    for chip in CHIP_NAMES:
        df[f"chip_{chip}"] = [
            next((c["num_played"] for c in (e.get("chip_plays") or []) if c["chip_name"] == chip), 0)
            for e in events
        ]
    return df


def next_event_id(events: pd.DataFrame) -> int | None:
    """Gameweek of the next transfer deadline, or None after the season ends."""
    upcoming = events[events["is_next"].astype(bool)]
    return int(upcoming.iloc[0]["id"]) if not upcoming.empty else None


def _tonight_projection(projections: object) -> tuple[float, float]:
    """(projected_percent, likelihood) for tonight's price update. Percent runs
    -100..+100; +/-100 means the price moves. Likelihood is signed by direction."""
    if not isinstance(projections, list):
        return np.nan, np.nan
    tonight = next((p for p in projections if p.get("offset") == 0), None)
    if tonight is None:
        return np.nan, np.nan
    return pd.to_numeric(tonight.get("projected_percent"), errors="coerce"), tonight.get("likelihood", np.nan)


def build_players_raw_df(bootstrap_data: dict) -> pd.DataFrame:
    elements = pd.DataFrame(bootstrap_data["elements"])
    df = elements[PLAYER_RAW_COLUMNS].copy()
    optional = elements.reindex(columns=PLAYER_OPTIONAL_COLUMNS)
    df = pd.concat([df, optional], axis=1)

    for col in PLAYER_RAW_NUMERIC_STRING_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    for col in [
        "expected_goals_per_90",
        "expected_assists_per_90",
        "expected_goal_involvements_per_90",
        "expected_goals_conceded_per_90",
        "saves_per_90",
        "goals_conceded_per_90",
        "price_change_percent",
        "penalties_order",
        "corners_and_indirect_freekicks_order",
        "direct_freekicks_order",
        "chance_of_playing_next_round",
        "chance_of_playing_this_round",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    projections = df["price_change_projections"].map(_tonight_projection)
    df["price_change_tonight_pct"] = [p[0] for p in projections]
    df["price_change_likelihood"] = [p[1] for p in projections]
    df = df.drop(columns=["price_change_projections"])

    df["position"] = df["element_type"].map(POSITION_BY_ELEMENT_TYPE).fillna("Unknown")
    df["news"] = df["news"].fillna("")
    return df


def build_fixtures_df(fixtures_data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(fixtures_data)[FIXTURE_COLUMNS].copy()
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], utc=True, errors="coerce")
    df["event"] = pd.to_numeric(df["event"], errors="coerce").astype("Int64")
    return df


def build_player_gameweek_history_df(history_by_player: dict[int, list[dict]]) -> pd.DataFrame:
    rows = []
    for player_id, history in history_by_player.items():
        for row in history:
            row = dict(row)
            row["player_id"] = player_id
            rows.append(row)

    if not rows:
        df = pd.DataFrame(columns=["player_id", *HISTORY_COLUMNS, "price"])
        return df.drop(columns=["value", "element"]).rename(
            columns={"fixture": "fixture_id", "opponent_team": "opponent_team_id"}
        )

    df = pd.DataFrame(rows).reindex(columns=["player_id", *HISTORY_COLUMNS]).copy()
    for col in HISTORY_NUMERIC_STRING_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], utc=True, errors="coerce")
    df["price"] = df["value"] / 10.0
    df = df.drop(columns=["value", "element"])
    df = df.rename(columns={"fixture": "fixture_id", "opponent_team": "opponent_team_id"})
    return df
