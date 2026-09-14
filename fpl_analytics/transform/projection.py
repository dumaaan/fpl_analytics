"""The expected-points model, in one place.

The pipeline uses it for the dashboard, and the backtest calls the same
functions with history cut off at a past deadline. So a backtest result
always describes the model the dashboard shows.

Inputs come only from per-match rows (mart_player_gameweeks shape), never
from season totals, so a cut-off history gives a true point-in-time view.

The model has three parts, each set by ModelSettings:
- Minutes: chance of a 60+ minute match, chance of a substitute appearance,
  and expected minutes, from team matches where each older match counts less.
- Rates per 90 minutes (xG, xA, saves, bonus, cards): season rates pulled
  toward the position average, more strongly when a player has few minutes.
  Recent-match weighting and price bands exist as settings but are off: the
  backtest showed no gain from them.
- Fixtures: team attack and defence from the team model in teams.py.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fpl_analytics.config import (
    MIN_MINUTES_FOR_PER_90,
    OUTFIELD_POSITIONS,
    SAVES_PER_SAVE_POINT,
    SHRINK_PRIOR_GAMES,
)
from fpl_analytics.transform.metrics import safe_divide
from fpl_analytics.transform.scoring import Scoring, defcon_threshold

PROJECTION_COLUMNS = [
    "xpts_next_gw", "xpts_next_3gw", "xpts_next_5gw", "xg_next_3gw", "cs_expected_next_gw", "fixtures_next_gw",
]
RATE_STATS = ["xg", "xa", "saves", "bonus", "cards"]


@dataclass(frozen=True)
class ModelSettings:
    # Minutes. "recent": 60+ / substitute / expected minutes from recent team
    # matches. "start_rate": the first version (start rate, no substitutes).
    minutes_model: str = "recent"
    minutes_decay: float = 1.0  # weight of each older team match in the minutes model (1 = all equal)
    # Rates per 90. rate_decay: weight of each older team match (1 = whole season equally).
    rate_decay: float = 1.0
    # Pull toward the average of similar players, as this many minutes at the
    # average rate added to the player's own (0 = no pull).
    shrink_minutes: float = 0.0
    prior_price_bands: int = 1  # price bands inside each position for that average (1 = position only)
    min_minutes: int = MIN_MINUTES_FOR_PER_90  # below this, no projection


FIRST_VERSION = ModelSettings(minutes_model="start_rate")

# Chosen with `fpl-backtest` on 2025/26 (with a 270-minute floor at the time):
# tuned on GW4-20, checked on GW21-38,
# then confirmed in the decision replay (20 starting squads, same squads for
# every version). On GW21-38 vs FIRST_VERSION: next-gameweek error 1.83 ->
# 1.79 points, player ranking over 3 gameweeks 0.50 -> 0.55. Replay: +22
# points (standard error 15) over GW5-38.
# - The pull toward the position average (shrink_minutes) is what helps
#   decisions; without it the replay loses 40 points.
# - A faster minutes weight (0.6) ranks all players better, but it overreacts
#   to one rest day for regular starters and lost 11 points in the replay.
# - Recent-match weighting of rates and price bands for the prior: no gain.
DEFAULT_SETTINGS = ModelSettings(minutes_decay=0.9, shrink_minutes=900)


def _match_age(pg: pd.DataFrame) -> pd.Series:
    """0 for each player's most recent team match, 1 for the one before, ..."""
    order = pg.sort_values(["player_id", "kickoff_time", "fixture_id"])
    return order.groupby("player_id").cumcount(ascending=False).reindex(pg.index)


def _price_band(prices: pd.Series, positions: pd.Series, bands: int) -> pd.Series:
    if bands <= 1 or prices is None:
        return pd.Series(0, index=positions.index)
    frame = pd.DataFrame({"position": positions, "price": prices.reindex(positions.index)})
    ranks = frame.groupby("position")["price"].rank(pct=True, method="first")
    return np.ceil(ranks.fillna(0.5) * bands).clip(1, bands).astype(int)


def model_inputs(
    player_gameweeks: pd.DataFrame,
    positions: pd.Series,
    scoring: Scoring,
    prices: pd.Series | None = None,
    settings: ModelSettings = DEFAULT_SETTINGS,
) -> pd.DataFrame:
    """Per-player model inputs from per-match rows.

    `positions` (and `prices`, for price bands) are indexed by player id.
    Returns one row per player with: minutes, starts, team_fixtures,
    games_played, start_rate; p60, p_sub, exp_minutes (minutes model);
    xg90, xa90, xgc90, saves90, bonus90, cards90 (rates, empty below
    `settings.min_minutes`); defcon_rate (chance of a DefCon hit in a 60+ match).
    """
    out = pd.DataFrame(index=positions.index)
    out.index.name = "player_id"
    out["position"] = positions
    rate_cols = ["xg90", "xa90", "xgc90", "saves90", "bonus90", "cards90"]
    if player_gameweeks.empty:
        for col in ["team_fixtures", "minutes", "starts", "games_played"]:
            out[col] = 0.0
        for col in ["start_rate", "p60", "p_sub", "exp_minutes", "defcon_rate", *rate_cols]:
            out[col] = np.nan
        return out

    pg = player_gameweeks
    pos = pg["player_id"].map(positions)
    minutes = pg["minutes"]
    played60 = minutes >= 60
    age = _match_age(pg)
    w_rate = settings.rate_decay ** age
    w_min = settings.minutes_decay ** age
    hit = (minutes > 0) & (pg["defensive_contribution"] >= defcon_threshold(pos))
    cards = pg["yellow_cards"] * scoring.value("yellow_cards", "") + pg["red_cards"] * scoring.value("red_cards", "")

    rows = pd.DataFrame({
        "player_id": pg["player_id"],
        "team_fixtures": 1.0,
        "minutes": minutes,
        "starts": pg["starts"],
        "games_played": (minutes > 0).astype(float),
        "games60": played60.astype(float),
        "hits60": (hit & played60).astype(float),
        "hits_any": hit.astype(float),
        "xgc": pg["expected_goals_conceded"],
        # Recency-weighted sums for rates.
        "w_minutes": minutes * w_rate,
        "w_xg": pg["expected_goals"] * w_rate,
        "w_xa": pg["expected_assists"] * w_rate,
        "w_saves": pg["saves"] * w_rate,
        "w_bonus": pg["bonus"] * w_rate,
        "w_cards": cards * w_rate,
        "w_games60": played60 * w_rate,
        "w_hits60": (hit & played60) * w_rate,
        # Recency-weighted sums for the minutes model.
        "m_weight": w_min,
        "m_60": played60 * w_min,
        "m_sub": ((minutes > 0) & ~played60) * w_min,
        "m_minutes": minutes * w_min,
    })
    totals = rows.groupby("player_id").sum()
    out = out.join(totals)
    out[totals.columns] = out[totals.columns].fillna(0.0)

    out["start_rate"] = safe_divide(out["starts"], out["team_fixtures"])
    if settings.minutes_model == "start_rate":
        out["p60"] = out["start_rate"]
        out["p_sub"] = 0.0
        out["exp_minutes"] = 90 * out["start_rate"]
    else:
        out["p60"] = safe_divide(out["m_60"], out["m_weight"])
        out["p_sub"] = safe_divide(out["m_sub"], out["m_weight"])
        out["exp_minutes"] = safe_divide(out["m_minutes"], out["m_weight"])

    # Average rate of similar players (the prior), from everyone's weighted totals.
    group = out["position"].astype(str) + "_" + _price_band(prices, out["position"], settings.prior_price_bands).astype(str)
    group_totals = out.groupby(group)[["w_minutes", *[f"w_{s}" for s in RATE_STATS]]].sum()
    k = settings.shrink_minutes
    enough = out["minutes"] >= settings.min_minutes
    for stat in RATE_STATS:
        prior90 = group.map(safe_divide(group_totals[f"w_{stat}"], group_totals["w_minutes"]) * 90).fillna(0.0)
        rate = (out[f"w_{stat}"] + prior90 * k / 90) / (out["w_minutes"] + k).where(lambda d: d > 0) * 90
        out[f"{stat}90"] = rate.where(enough)
    out["saves90"] = out["saves90"].where(out["position"] == "GK", 0.0)
    out["xgc90"] = (out["xgc"] / out["minutes"].where(out["minutes"] > 0) * 90).where(enough)

    outfield = out["position"].isin(OUTFIELD_POSITIONS)
    if settings.minutes_model == "start_rate":
        # First version: hits per game played, applied to every start.
        hits, games = out["hits_any"], out["games_played"]
    else:
        hits, games = out["w_hits60"], out["w_games60"]
    position_rate = safe_divide(hits.groupby(out["position"]).sum(), games.groupby(out["position"]).sum())
    out["defcon_rate"] = ((hits + SHRINK_PRIOR_GAMES * out["position"].map(position_rate))
                          / (games + SHRINK_PRIOR_GAMES)).where(outfield)

    keep = ["position", "team_fixtures", "minutes", "starts", "games_played", "start_rate", "p60", "p_sub",
            "exp_minutes", "defcon_rate", *rate_cols]
    return out[keep]


def xpts_per_90(inputs: pd.DataFrame, scoring: Scoring) -> pd.Series:
    """Expected points per 90 minutes played, against an average opponent."""
    pos = inputs["position"]
    xgc90 = inputs["xgc90"]
    return (
        scoring.value("long_play", "")
        + scoring.by_position("goals_scored", pos) * inputs["xg90"]
        + scoring.value("assists", "") * inputs["xa90"]
        + scoring.by_position("clean_sheets", pos) * np.exp(-xgc90)
        + scoring.by_position("goals_conceded", pos) * xgc90 / 2
        + inputs["saves90"] / SAVES_PER_SAVE_POINT * scoring.value("saves", "")
        + scoring.by_position("defensive_contribution", pos) * inputs["defcon_rate"].fillna(0.0)
        + inputs["bonus90"]
        + inputs["cards90"]
    )


def project_fixtures(
    inputs: pd.DataFrame,
    teams: pd.Series,
    availability: pd.Series,
    upcoming: pd.DataFrame,
    ratings: pd.DataFrame,
    scoring: Scoring,
) -> pd.DataFrame:
    """Expected points per player per upcoming fixture.

    With a = availability, p60 / p_sub = chance of a 60+ / substitute
    appearance, and f = expected minutes / 90:

      appearance   2 * a * p60 + 1 * a * p_sub
      clean sheet  clean-sheet points * fixture clean-sheet chance * a * p60
      returns      (goal points * xG/90 + 3 * xA/90) * attack strength * a * f
      conceded     goals-conceded points * projected goals against / 2 * a * f
      saves, bonus, cards  per-90 rate * a * f
      DefCon       DefCon points * hit chance in a 60+ match * a * p60

    `teams` and `availability` are indexed by player id. Returns columns
    player_id, event, fixture_id, xpts, xg, cs_prob.
    """
    columns = ["player_id", "event", "fixture_id", "xpts", "xg", "cs_prob"]
    if upcoming.empty:
        return pd.DataFrame(columns=columns)

    team_xg_pg = ratings.set_index("team_id")["xg_per_game"]
    fx = upcoming[["team_id", "event", "fixture_id", "xg_for", "xg_against", "cs_prob"]].copy()
    fx["attack_mult"] = fx["xg_for"] / fx["team_id"].map(team_xg_pg)

    p = inputs.reset_index()[["player_id", "position", "xg90", "xa90", "saves90", "bonus90", "cards90",
                              "defcon_rate", "p60", "p_sub", "exp_minutes"]]
    p["team"] = p["player_id"].map(teams)
    a = p["player_id"].map(availability).fillna(1.0)
    p["p60_a"] = p["p60"].fillna(0.0) * a
    p["p_sub_a"] = p["p_sub"].fillna(0.0) * a
    p["f_a"] = p["exp_minutes"].fillna(0.0) / 90 * a
    m = p.merge(fx, left_on="team", right_on="team_id", how="inner")

    pos = m["position"]
    m["xpts"] = (
        scoring.value("long_play", "") * m["p60_a"]
        + scoring.value("short_play", "") * m["p_sub_a"]
        + scoring.by_position("clean_sheets", pos) * m["cs_prob"] * m["p60_a"]
        + (scoring.by_position("goals_scored", pos) * m["xg90"] + scoring.value("assists", "") * m["xa90"])
        * m["attack_mult"] * m["f_a"]
        + scoring.by_position("goals_conceded", pos) * m["xg_against"] / 2 * m["f_a"]
        + (m["saves90"] / SAVES_PER_SAVE_POINT * scoring.value("saves", "") + m["bonus90"] + m["cards90"]) * m["f_a"]
        + scoring.by_position("defensive_contribution", pos) * m["defcon_rate"].fillna(0.0) * m["p60_a"]
    )
    m["xg"] = m["xg90"] * m["attack_mult"] * m["f_a"]
    return m[columns]


def summarize_projections(per_fixture: pd.DataFrame, inputs: pd.DataFrame, first_event: int | None) -> pd.DataFrame:
    """Per-player totals for the next 1, 3 and 5 gameweeks from `first_event`.

    A blank gameweek is a real 0 for players the model can project; players
    below the minutes floor get empty projections.
    """
    out = pd.DataFrame(index=inputs.index, columns=PROJECTION_COLUMNS, dtype=float)
    if per_fixture.empty or first_event is None:
        return out

    by_player = lambda frame: frame.groupby("player_id")  # noqa: E731
    next_gw = per_fixture[per_fixture["event"] == first_event]
    three = per_fixture[per_fixture["event"] < first_event + 3]
    five = per_fixture[per_fixture["event"] < first_event + 5]

    out["xpts_next_gw"] = by_player(next_gw)["xpts"].sum()
    out["xpts_next_3gw"] = by_player(three)["xpts"].sum()
    out["xpts_next_5gw"] = by_player(five)["xpts"].sum()
    out["xg_next_3gw"] = by_player(three)["xg"].sum()
    out["cs_expected_next_gw"] = by_player(next_gw)["cs_prob"].sum()
    out["fixtures_next_gw"] = by_player(next_gw).size()

    projectable = inputs["xg90"].notna()
    out.loc[projectable] = out.loc[projectable].fillna(0.0)
    out.loc[~projectable, ["xpts_next_gw", "xpts_next_3gw", "xpts_next_5gw", "xg_next_3gw"]] = np.nan
    return out
