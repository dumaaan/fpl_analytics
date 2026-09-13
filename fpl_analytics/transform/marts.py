import numpy as np
import pandas as pd

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90
from fpl_analytics.transform.metrics import per_90, safe_divide

# FPL's own pre-computed per-90 fields (unlike our per_90() column math, these
# come straight from the API) get the same small-sample floor applied below.
FPL_NATIVE_PER_90_COLUMNS = [
    "defensive_contribution_per_90",
    "clean_sheets_per_90",
    "starts_per_90",
]

# Defensive Contribution (DefCon), introduced 2025/26: goalkeepers are not
# eligible. Defenders earn 2 points for 10+ combined clearances, blocks,
# interceptions and tackles (CBIT) in a match. Midfielders and forwards earn
# 2 points for 12+ CBIT plus ball recoveries (CBIRT). Points are capped at 2
# per match regardless of how far over the threshold a player goes.
DEFCON_THRESHOLD_DEF = 10
DEFCON_THRESHOLD_MID_FWD = 12


def _appearance_points(minutes: pd.Series) -> pd.Series:
    """2 pts for 60+ minutes, 1 pt for 1-59 minutes, 0 otherwise."""
    return np.select([minutes >= 60, minutes >= 1], [2, 1], default=0)


def _compute_defcon_hit_rate(
    player_gameweek_history: pd.DataFrame, position_by_player_id: pd.Series
) -> pd.Series:
    """Fraction of games played where a player crossed their DefCon threshold."""
    history = player_gameweek_history[player_gameweek_history["minutes"] > 0].copy()
    history["position"] = history["player_id"].map(position_by_player_id)

    cbit = history["clearances_blocks_interceptions"] + history["tackles"]
    cbirt = cbit + history["recoveries"]
    threshold = np.where(history["position"] == "DEF", DEFCON_THRESHOLD_DEF, DEFCON_THRESHOLD_MID_FWD)
    combined = np.where(history["position"] == "DEF", cbit, cbirt)
    history["defcon_hit"] = (combined >= threshold) & history["position"].isin(["DEF", "MID", "FWD"])

    return history.groupby("player_id")["defcon_hit"].mean().rename("defcon_hit_rate")


def _compute_vapm(player_gameweek_history: pd.DataFrame) -> pd.Series:
    """Value Added Per Million: points earned beyond the automatic appearance
    points (2 for 60+ minutes, 1 for 1-59), averaged per game and divided by
    price at the time. This is the standard FPL-community "value" metric --
    plain points-per-million rewards players just for being nailed-on
    starters, VAPM isolates the returns that actually came from playing well.
    """
    history = player_gameweek_history.copy()
    history["appearance_points"] = _appearance_points(history["minutes"])
    history["non_appearance_points"] = history["total_points"] - history["appearance_points"]
    avg_non_appearance_points = history.groupby("player_id")["non_appearance_points"].mean()
    price_by_player = history.groupby("player_id")["price"].last()
    values = safe_divide(avg_non_appearance_points, price_by_player)
    return pd.Series(values, index=avg_non_appearance_points.index, name="vapm")


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

    df["goal_involvement_per_90"] = df["goals_per_90"] + df["assists_per_90"]
    df["xgi_per_90"] = df["xg_per_90"] + df["xa_per_90"]
    df["goal_involvement_overperformance"] = (df["goals_scored"] + df["assists"]) - (
        df["expected_goals"] + df["expected_assists"]
    )

    df["clean_sheet_percentage"] = per_90(df["clean_sheets"], minutes)
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

    # Defensive Contribution volume stats (season totals, direct from FPL).
    df["cbit"] = df["clearances_blocks_interceptions"] + df["tackles"]
    df["cbirt"] = df["cbit"] + df["recoveries"]

    # Same small-sample floor as per_90(), applied to FPL's own native
    # per-90 fields (they don't go through per_90() since we pass them
    # through directly rather than deriving them ourselves).
    below_floor = df["minutes"] < MIN_MINUTES_FOR_PER_90
    for col in FPL_NATIVE_PER_90_COLUMNS:
        df.loc[below_floor, col] = 0.0

    if not player_gameweek_history.empty:
        position_by_player_id = df.set_index("id")["position"]

        consistency = (
            player_gameweek_history.groupby("player_id")["total_points"]
            .std()
            .rename("consistency_score")
        )
        df = df.merge(consistency, left_on="id", right_index=True, how="left")

        defcon_hit_rate = _compute_defcon_hit_rate(player_gameweek_history, position_by_player_id)
        df = df.merge(defcon_hit_rate, left_on="id", right_index=True, how="left")

        vapm = _compute_vapm(player_gameweek_history)
        df = df.merge(vapm, left_on="id", right_index=True, how="left")
    else:
        df["consistency_score"] = np.nan
        df["defcon_hit_rate"] = np.nan
        df["vapm"] = np.nan

    df["defcon_hit_rate"] = df["defcon_hit_rate"].fillna(0.0)
    df["vapm"] = df["vapm"].fillna(0.0)

    return df
