import numpy as np
import pandas as pd

from fpl_analytics.config import (
    DEFCON_NEAR_MISS_WINDOW,
    DEFCON_THRESHOLDS,
    GOALS_CONCEDED_PER_DEDUCTION,
    SAVES_PER_SAVE_POINT,
    SCORING_POSITION_KEYS,
)

# Where a player's points come from. Fixed order: dashboard colors follow it.
POINT_SOURCES = [
    "appearance",
    "clean_sheets",
    "defcon",
    "goals",
    "assists",
    "saves",
    "bonus",
    "deductions",
]


class Scoring:
    """FPL points per event, read from bootstrap-static game_config.scoring so
    the metrics follow rule changes without code changes. Values are either a
    number (same for all positions) or a {"GKP"|"DEF"|"MID"|"FWD": number} map.
    """

    def __init__(self, raw: dict):
        self.raw = raw

    @classmethod
    def from_bootstrap(cls, bootstrap_data: dict) -> "Scoring":
        return cls(bootstrap_data["game_config"]["scoring"])

    def value(self, event: str, position: str) -> float:
        points = self.raw.get(event, 0)
        if isinstance(points, dict):
            return float(points.get(SCORING_POSITION_KEYS.get(position, ""), 0))
        return float(points)

    def by_position(self, event: str, positions: pd.Series) -> pd.Series:
        return positions.map(lambda p: self.value(event, p)).astype(float)


def defcon_threshold(positions: pd.Series) -> pd.Series:
    """Per-row DefCon threshold; NaN for goalkeepers (not eligible)."""
    return positions.map(DEFCON_THRESHOLDS).astype(float)


def add_points_breakdown(history: pd.DataFrame, scoring: Scoring) -> pd.DataFrame:
    """Split every player-gameweek row's points into POINT_SOURCES (pts_* columns).

    `history` needs a `position` column. `pts_unexplained` is total_points minus
    the rebuilt sum; it is 0 when the rules here match FPL's scoring.
    """
    df = history.copy()
    pos = df["position"]
    minutes = df["minutes"]

    threshold = defcon_threshold(pos)
    played = minutes > 0
    df["defcon_hit"] = played & (df["defensive_contribution"] >= threshold)
    df["defcon_near_miss"] = (
        played
        & (df["defensive_contribution"] < threshold)
        & (df["defensive_contribution"] >= threshold - DEFCON_NEAR_MISS_WINDOW)
    )

    df["pts_appearance"] = np.select(
        [minutes >= 60, minutes >= 1],
        [scoring.value("long_play", ""), scoring.value("short_play", "")],
        default=0.0,
    )
    df["pts_clean_sheets"] = df["clean_sheets"] * scoring.by_position("clean_sheets", pos)
    df["pts_defcon"] = df["defcon_hit"].astype(float) * scoring.by_position("defensive_contribution", pos)
    df["pts_goals"] = df["goals_scored"] * scoring.by_position("goals_scored", pos)
    df["pts_assists"] = df["assists"] * scoring.value("assists", "")
    df["pts_saves"] = (df["saves"] // SAVES_PER_SAVE_POINT) * scoring.value("saves", "") + df[
        "penalties_saved"
    ] * scoring.value("penalties_saved", "")
    df["pts_bonus"] = df["bonus"] * scoring.value("bonus", "")
    df["pts_deductions"] = (
        (df["goals_conceded"] // GOALS_CONCEDED_PER_DEDUCTION) * scoring.by_position("goals_conceded", pos)
        + df["penalties_missed"] * scoring.value("penalties_missed", "")
        + df["yellow_cards"] * scoring.value("yellow_cards", "")
        + df["red_cards"] * scoring.value("red_cards", "")
        + df["own_goals"] * scoring.value("own_goals", "")
    )

    rebuilt = df[[f"pts_{s}" for s in POINT_SOURCES]].sum(axis=1)
    df["pts_unexplained"] = df["total_points"] - rebuilt
    return df
