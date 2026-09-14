"""Point-in-time projections for gameweek t, using only matches before t.

Each projector returns a frame indexed by player_id with columns
`h1` (points expected in gameweek t) and `h3` (gameweeks t, t+1, t+2).
A projector may leave `h3` empty if it cannot look ahead.
"""

from dataclasses import dataclass
from functools import partial
from typing import Callable

import pandas as pd

from fpl_analytics.backtest.data import Season
from fpl_analytics.transform.projection import (
    DEFAULT_SETTINGS,
    FIRST_VERSION,
    ModelSettings,
    model_inputs,
    project_fixtures,
    summarize_projections,
)
from fpl_analytics.transform.teams import (
    build_team_fixture_results,
    build_team_ratings,
    build_upcoming_team_fixtures,
)


@dataclass
class AsOf:
    """Everything known at the gameweek-t deadline."""

    season: Season
    gw: int
    history: pd.DataFrame  # matches before gw
    positions: pd.Series  # player_id -> position (latest known)
    teams: pd.Series  # player_id -> team_id (latest known)
    prices: pd.Series  # player_id -> price in £m (latest known)
    upcoming: pd.DataFrame  # team fixtures for gw .. gw+4 with projections
    ratings: pd.DataFrame
    fixtures_per_team: pd.DataFrame  # (team_id, event) -> fixture count


def as_of(season: Season, gw: int) -> AsOf:
    history = season.history[season.history["round"] < gw]
    latest = history.sort_values(["round", "kickoff_time"]).groupby("player_id").last()
    fixtures = season.fixtures.copy()
    fixtures["finished"] = fixtures["event"] < gw  # nothing from gw on is known yet
    results = build_team_fixture_results(history, fixtures)
    ratings = build_team_ratings(results, season.teams, fixtures)
    upcoming = build_upcoming_team_fixtures(fixtures, ratings, next_event=gw, horizon_gws=5)
    counts = upcoming.groupby(["team_id", "event"]).size().rename("n") if not upcoming.empty else pd.Series()
    return AsOf(season, gw, history, latest["position"], latest["team_id"], latest["price"], upcoming, ratings,
                counts.reset_index() if len(counts) else pd.DataFrame(columns=["team_id", "event", "n"]))


def _fixture_count(ctx: AsOf, horizon: int) -> pd.Series:
    """Fixtures each player's team plays in gw .. gw+horizon-1."""
    window = ctx.fixtures_per_team[ctx.fixtures_per_team["event"] < ctx.gw + horizon]
    per_team = window.groupby("team_id")["n"].sum()
    return ctx.teams.map(per_team).fillna(0.0)


def model_projector(ctx: AsOf, settings: ModelSettings = DEFAULT_SETTINGS) -> pd.DataFrame:
    """The production model. No injury news exists in history, so every
    player counts as available; the minutes model still sees missed matches."""
    scoring = ctx.season.scoring
    inputs = model_inputs(ctx.history, ctx.positions, scoring, prices=ctx.prices, settings=settings)
    availability = pd.Series(1.0, index=inputs.index)
    per_fixture = project_fixtures(inputs, ctx.teams, availability, ctx.upcoming, ctx.ratings, scoring)
    summary = summarize_projections(per_fixture, inputs, ctx.gw)
    return pd.DataFrame({
        "h1": summary["xpts_next_gw"],
        "h3": summary["xpts_next_3gw"],
        "play_prob": inputs["p60"] + inputs["p_sub"],
    })


def _per_fixture_mean(ctx: AsOf, rounds_back: int | None) -> pd.Series:
    history = ctx.history
    if rounds_back is not None:
        history = history[history["round"] >= ctx.gw - rounds_back]
    return history.groupby("player_id")["total_points"].mean().reindex(ctx.positions.index)


def points_per_game_projector(ctx: AsOf) -> pd.DataFrame:
    """Season points per team match (missed matches count as 0) x fixtures ahead."""
    mean = _per_fixture_mean(ctx, None)
    return pd.DataFrame({"h1": mean * _fixture_count(ctx, 1), "h3": mean * _fixture_count(ctx, 3)})


def form_projector(ctx: AsOf) -> pd.DataFrame:
    """Points per team match over the last 4 gameweeks x fixtures ahead."""
    mean = _per_fixture_mean(ctx, 4)
    return pd.DataFrame({"h1": mean * _fixture_count(ctx, 1), "h3": mean * _fixture_count(ctx, 3)})


PROJECTORS: dict[str, Callable[[AsOf], pd.DataFrame]] = {
    "model": model_projector,
    "model_v1": partial(model_projector, settings=FIRST_VERSION),
    "points_per_game": points_per_game_projector,
    "form_4gw": form_projector,
}
