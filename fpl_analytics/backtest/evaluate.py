"""Projection accuracy: compare point-in-time projections with actual points."""

import numpy as np
import pandas as pd

from fpl_analytics.backtest.data import Season
from fpl_analytics.backtest.projectors import PROJECTORS, as_of
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90

EARLY_SEASON_LAST_GW = 10


def actual_points(season: Season, first_gw: int, n_gws: int) -> pd.Series:
    """Points per player over gameweeks first_gw .. first_gw+n_gws-1.
    Doubles add up; blanks and missed matches are 0."""
    window = season.history[season.history["round"].between(first_gw, first_gw + n_gws - 1)]
    return window.groupby("player_id")["total_points"].sum()


def collect_predictions(season: Season, start_gw: int, end_gw: int, projectors: list[str]) -> pd.DataFrame:
    """One row per (gw, player) with every projector's h1/h3 and the actuals.

    Rows are limited to players with MIN_MINUTES_FOR_PER_90+ minutes before
    the deadline. The group does not depend on any model setting, so every
    projector and every model version is scored on the same players.
    """
    frames = []
    for gw in range(start_gw, end_gw + 1):
        ctx = as_of(season, gw)
        minutes = ctx.history.groupby("player_id")["minutes"].sum()
        eligible = minutes[minutes >= MIN_MINUTES_FOR_PER_90].index
        model = PROJECTORS["model"](ctx)
        rows = pd.DataFrame(index=eligible.intersection(ctx.positions.index))
        rows.index.name = "player_id"
        rows["gw"] = gw
        rows["position"] = ctx.positions.reindex(rows.index)
        rows["play_prob"] = model["play_prob"].reindex(rows.index)
        for name in projectors:
            out = model if name == "model" else PROJECTORS[name](ctx)
            rows[f"{name}_h1"] = out["h1"].reindex(rows.index)
            rows[f"{name}_h3"] = out["h3"].reindex(rows.index)
        rows["actual_h1"] = actual_points(season, gw, 1).reindex(rows.index).fillna(0.0)
        played = season.history[season.history["round"] == gw].groupby("player_id")["minutes"].sum()
        rows["played"] = played.reindex(rows.index).fillna(0.0) > 0
        last_gw = ctx.history[ctx.history["round"] == gw - 1].groupby("player_id")["minutes"].sum()
        rows["regular"] = last_gw.reindex(rows.index).fillna(0.0) >= 60
        if gw + 2 <= season.last_complete_gw:
            rows["actual_h3"] = actual_points(season, gw, 3).reindex(rows.index).fillna(0.0)
        else:
            rows["actual_h3"] = np.nan
        frames.append(rows.reset_index())
    return pd.concat(frames, ignore_index=True)


def _rank_corr(group: pd.DataFrame, pred: str, actual: str) -> float:
    if group[pred].nunique() < 2 or group[actual].nunique() < 2:
        return np.nan
    return group[pred].rank().corr(group[actual].rank())


def score_projector(df: pd.DataFrame, name: str, horizon: str) -> dict:
    """Error, bias, ranking and pick quality for one projector and horizon."""
    pred, actual = f"{name}_{horizon}", f"actual_{horizon}"
    rows = df.dropna(subset=[pred, actual])
    if rows.empty:
        return {}
    error = rows[pred] - rows[actual]
    groups = rows.groupby(["gw", "position"])
    rank_corr = groups.apply(lambda g: _rank_corr(g, pred, actual), include_groups=False).mean()

    def top_k_capture(g: pd.DataFrame, k: int = 10) -> float:
        best = g.nlargest(k, actual)[actual].mean()
        picked = g.nlargest(k, pred)[actual].mean()
        return picked / best if best > 0 else np.nan

    capture = groups.apply(top_k_capture, include_groups=False).mean()
    captain = rows.loc[rows.groupby("gw")[pred].idxmax(), actual].mean()
    best_captain = rows.groupby("gw")[actual].max().mean()
    return {
        "projector": name,
        "horizon": horizon,
        "rows": len(rows),
        "gameweeks": rows["gw"].nunique(),
        "mae": error.abs().mean(),
        "rmse": np.sqrt((error ** 2).mean()),
        "bias": error.mean(),
        "rank_corr": rank_corr,
        "top10_capture": capture,
        "captain_pts": captain,
        "best_captain_pts": best_captain,
    }


def accuracy_table(df: pd.DataFrame, projectors: list[str]) -> pd.DataFrame:
    rows = []
    for phase, subset in [("all", df), ("early (GW≤10)", df[df["gw"] <= EARLY_SEASON_LAST_GW]),
                          ("later (GW>10)", df[df["gw"] > EARLY_SEASON_LAST_GW]),
                          ("regular starters", df[df["regular"]])]:
        for horizon in ["h1", "h3"]:
            for name in projectors:
                result = score_projector(subset, name, horizon)
                if result:
                    rows.append({"phase": phase, **result})
    return pd.DataFrame(rows)


def by_position_table(df: pd.DataFrame, projectors: list[str]) -> pd.DataFrame:
    rows = []
    for position, subset in df.groupby("position"):
        for name in projectors:
            result = score_projector(subset, name, "h1")
            if result:
                rows.append({"position": position, **result})
    return pd.DataFrame(rows)


def calibration_table(df: pd.DataFrame, name: str = "model", bins: int = 10) -> pd.DataFrame:
    """Mean projected vs mean actual points, by projection decile."""
    rows = df.dropna(subset=[f"{name}_h1"])
    decile = pd.qcut(rows[f"{name}_h1"].rank(method="first"), bins, labels=range(1, bins + 1))
    return rows.groupby(decile, observed=True).agg(
        players=("actual_h1", "size"),
        projected=(f"{name}_h1", "mean"),
        actual=("actual_h1", "mean"),
    ).reset_index(names="decile")


def minutes_table(df: pd.DataFrame, bins: int = 5) -> pd.DataFrame:
    """How well the model's chance to appear predicts who plays."""
    rows = df.dropna(subset=["play_prob"])
    bucket = pd.cut(rows["play_prob"], bins=np.linspace(0, 1, bins + 1), include_lowest=True)
    table = rows.groupby(bucket, observed=True).agg(
        players=("played", "size"), predicted=("play_prob", "mean"), actual=("played", "mean"),
    ).reset_index(names="play_prob_bucket")
    table["play_prob_bucket"] = table["play_prob_bucket"].astype(str)
    return table


def brier_score(df: pd.DataFrame) -> float:
    rows = df.dropna(subset=["play_prob"])
    return float(((rows["play_prob"] - rows["played"].astype(float)) ** 2).mean())
