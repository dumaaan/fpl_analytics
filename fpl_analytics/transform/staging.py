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
    "defensive_contribution",
    "clearances_blocks_interceptions",
    "recoveries",
    "tackles",
    *HISTORY_NUMERIC_STRING_COLUMNS,
]


def build_teams_df(bootstrap_data: dict) -> pd.DataFrame:
    df = pd.DataFrame(bootstrap_data["teams"])[TEAM_COLUMNS]
    return df.rename(columns={"name": "name", "short_name": "short_name"})


def build_players_raw_df(bootstrap_data: dict) -> pd.DataFrame:
    df = pd.DataFrame(bootstrap_data["elements"])[PLAYER_RAW_COLUMNS].copy()
    for col in PLAYER_RAW_NUMERIC_STRING_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["position"] = df["element_type"].map(POSITION_BY_ELEMENT_TYPE).fillna("Unknown")
    df["chance_of_playing_next_round"] = pd.to_numeric(
        df["chance_of_playing_next_round"], errors="coerce"
    )
    df["chance_of_playing_this_round"] = pd.to_numeric(
        df["chance_of_playing_this_round"], errors="coerce"
    )
    return df


def build_fixtures_df(fixtures_data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(fixtures_data)[FIXTURE_COLUMNS].copy()
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], utc=True, errors="coerce")
    return df


def build_player_gameweek_history_df(history_by_player: dict[int, list[dict]]) -> pd.DataFrame:
    rows = []
    for player_id, history in history_by_player.items():
        for row in history:
            row = dict(row)
            row["player_id"] = player_id
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["player_id", *HISTORY_COLUMNS])

    df = pd.DataFrame(rows)[["player_id", *HISTORY_COLUMNS]].copy()
    for col in HISTORY_NUMERIC_STRING_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], utc=True, errors="coerce")
    df["price"] = df["value"] / 10.0
    df = df.drop(columns=["value", "element"])
    df = df.rename(columns={"fixture": "fixture_id", "opponent_team": "opponent_team_id"})
    return df
