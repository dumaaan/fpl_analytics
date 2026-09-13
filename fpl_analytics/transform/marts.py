import numpy as np
import pandas as pd

from fpl_analytics.transform.metrics import per_90, safe_divide


def build_mart_players(
    players_raw: pd.DataFrame,
    teams: pd.DataFrame,
    player_gameweek_history: pd.DataFrame,
) -> pd.DataFrame:
    """One row per player: identity, raw season totals, and every derived
    metric. Formulas are ported from the project's original dbt SQL
    (fpl_data/models/fpl_player_stats.sql) and its later TypeScript port,
    now written as vectorized pandas.
    """
    df = players_raw.merge(
        teams[["id", "name"]].rename(columns={"id": "team", "name": "team_name"}),
        on="team",
        how="left",
    )

    df["price"] = df["now_cost"] / 10.0
    minutes = df["minutes"]

    df["goals_per_90"] = per_90(df["goals_scored"], minutes)
    df["assists_per_90"] = per_90(df["assists"], minutes)
    df["xg_per_90"] = per_90(df["expected_goals"], minutes)
    df["xa_per_90"] = per_90(df["expected_assists"], minutes)
    df["points_per_million"] = safe_divide(df["total_points"], df["price"])
    df["points_per_90"] = per_90(df["total_points"], minutes)

    position_avg_points = df.groupby("position")["total_points"].transform("mean")
    df["value_added"] = safe_divide(df["total_points"] - position_avg_points, df["price"])

    df["goal_involvement_per_90"] = df["goals_per_90"] + df["assists_per_90"]
    df["xgi_per_90"] = df["xg_per_90"] + df["xa_per_90"]
    df["goal_involvement_overperformance"] = (df["goals_scored"] + df["assists"]) - (
        df["expected_goals"] + df["expected_assists"]
    )

    df["clean_sheet_percentage"] = safe_divide(df["clean_sheets"] * 90, minutes)
    df["saves_per_90"] = per_90(df["saves"], minutes)
    df["goals_conceded_per_90"] = per_90(df["goals_conceded"], minutes)
    df["bonus_points_per_90"] = per_90(df["bonus"], minutes)
    df["bps_per_90"] = per_90(df["bps"], minutes)
    df["ict_index_per_90"] = per_90(df["ict_index"], minutes)

    df["total_contribution_per_90"] = per_90(
        df["goals_scored"] * 4
        + df["assists"] * 3
        + df["clean_sheets"] * 4
        - df["goals_conceded"] * 0.5
        + df["saves"] * 0.5,
        minutes,
    )

    goal_involvements = df["goals_scored"] + df["assists"]
    df["minutes_per_goal_involvement"] = np.where(
        goal_involvements > 0, minutes / goal_involvements, np.nan
    )
    df["points_per_minute"] = safe_divide(df["total_points"], minutes)
    df["card_rate"] = per_90(df["yellow_cards"] + df["red_cards"] * 2, minutes)

    # Real consistency score: standard deviation of points across gameweeks
    # played this season. Needs per-gameweek history, which earlier versions
    # of this project did not have — previously this was a no-op that always
    # returned null/0.
    if not player_gameweek_history.empty:
        consistency = (
            player_gameweek_history.groupby("player_id")["total_points"]
            .std()
            .rename("consistency_score")
        )
        df = df.merge(consistency, left_on="id", right_index=True, how="left")
    else:
        df["consistency_score"] = np.nan

    return df
