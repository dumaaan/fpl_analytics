import math

import numpy as np
import pandas as pd
import pytest

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90
from fpl_analytics.transform.marts import _gameweek_aggregates
from fpl_analytics.transform.metrics import per_90, safe_divide, shrunk_rate
from fpl_analytics.transform.scoring import POINT_SOURCES, Scoring, add_points_breakdown
from fpl_analytics.transform.teams import (
    build_team_ratings,
    build_upcoming_team_fixtures,
    next_fixtures_label,
)

# game_config.scoring values for 2026/27 (subset used by the metrics).
SCORING = Scoring(
    {
        "long_play": 2,
        "short_play": 1,
        "goals_conceded": {"DEF": -1, "FWD": 0, "GKP": -1, "MID": 0},
        "saves": 1,
        "goals_scored": {"DEF": 6, "FWD": 4, "GKP": 10, "MID": 5},
        "assists": 3,
        "clean_sheets": {"DEF": 4, "FWD": 0, "GKP": 4, "MID": 1},
        "penalties_saved": 5,
        "penalties_missed": -2,
        "yellow_cards": -1,
        "red_cards": -3,
        "own_goals": -2,
        "bonus": 1,
        "defensive_contribution": {"DEF": 2, "FWD": 2, "GKP": 0, "MID": 2},
    }
)


def _match(**overrides) -> dict:
    row = {
        "player_id": 1, "fixture_id": 1, "round": 1, "position": "DEF", "minutes": 90, "starts": 1,
        "total_points": 0, "goals_scored": 0, "assists": 0, "clean_sheets": 0, "goals_conceded": 0,
        "own_goals": 0, "penalties_saved": 0, "penalties_missed": 0, "yellow_cards": 0, "red_cards": 0,
        "saves": 0, "bonus": 0, "defensive_contribution": 0, "expected_goals": 0.0,
        "expected_goal_involvements": 0.0, "team_xg": 1.0, "price": 5.0,
    }
    row.update(overrides)
    return row


def test_per_90_is_empty_below_floor():
    minutes = pd.Series([MIN_MINUTES_FOR_PER_90 - 1, MIN_MINUTES_FOR_PER_90, 0])
    rates = per_90(pd.Series([3, 3, 0]), minutes)
    assert math.isnan(rates[0])
    assert rates[1] == pytest.approx(3 * 90 / MIN_MINUTES_FOR_PER_90)
    assert math.isnan(rates[2])


def test_safe_divide_and_shrunk_rate():
    assert math.isnan(safe_divide(pd.Series([1.0]), pd.Series([0.0]))[0])
    # 2 hits in 3 games, position rate 25%, k = 4 -> (2 + 1) / 7
    assert shrunk_rate(pd.Series([2]), pd.Series([3]), 0.25, prior_trials=4)[0] == pytest.approx(3 / 7)


@pytest.mark.parametrize(
    "position, row, expected",
    [
        # GK: 60+ mins, clean sheet, 7 saves (2 save points), penalty save, 1 bonus
        ("GK", dict(minutes=90, clean_sheets=1, saves=7, penalties_saved=1, bonus=1), 2 + 4 + 2 + 5 + 1),
        # GK goal is 10 points in 2026/27
        ("GK", dict(minutes=90, goals_scored=1, goals_conceded=3), 2 + 10 - 1),
        # DEF: goal, DefCon hit (10 CBIT), 4 conceded (-2), yellow card
        ("DEF", dict(minutes=90, goals_scored=1, defensive_contribution=10, goals_conceded=4, yellow_cards=1),
         2 + 6 + 2 - 2 - 1),
        # MID: clean sheet is 1, 11 CBIRT is a near miss, not a hit
        ("MID", dict(minutes=75, clean_sheets=1, assists=1, defensive_contribution=11), 2 + 1 + 3),
        # FWD: goals conceded cost nothing; sub appearance is 1 point
        ("FWD", dict(minutes=20, goals_scored=1, goals_conceded=3), 1 + 4),
    ],
)
def test_points_breakdown_rebuilds_total(position, row, expected):
    history = pd.DataFrame([_match(position=position, total_points=expected, **row)])
    out = add_points_breakdown(history, SCORING)
    assert out[[f"pts_{s}" for s in POINT_SOURCES]].sum(axis=1)[0] == expected
    assert out["pts_unexplained"][0] == 0


def test_defcon_flags_by_position():
    history = pd.DataFrame([
        _match(position="DEF", defensive_contribution=9),
        _match(position="MID", defensive_contribution=12),
        _match(position="GK", defensive_contribution=20),
        _match(position="DEF", defensive_contribution=15, minutes=0),
    ])
    out = add_points_breakdown(history, SCORING)
    assert out["defcon_hit"].tolist() == [False, True, False, False]
    assert out["defcon_near_miss"].tolist() == [True, False, False, False]


def test_vapm_inputs_ignore_games_not_played():
    history = pd.DataFrame([
        _match(round=1, minutes=90, total_points=8, goals_scored=1),
        _match(round=2, minutes=0, total_points=0),
    ])
    agg = _gameweek_aggregates(add_points_breakdown(history, SCORING))
    assert agg.loc[1, "games_played"] == 1
    # 8 points - 2 appearance points, averaged over the 1 played game only
    assert agg.loc[1, "avg_non_appearance_points"] == 6
    assert agg.loc[1, "team_fixtures"] == 2


def _teams() -> pd.DataFrame:
    return pd.DataFrame({
        "id": [1, 2, 3],
        "short_name": ["AAA", "BBB", "CCC"],
        **{c: [0, 0, 0] for c in ["strength_attack_home", "strength_attack_away",
                                  "strength_defence_home", "strength_defence_away"]},
    })


def _fixtures() -> pd.DataFrame:
    return pd.DataFrame({
        "id": [10, 11, 12, 13],
        "event": pd.array([5, 5, 6, 7], dtype="Int64"),
        "team_h": [1, 3, 2, 1],
        "team_a": [2, 1, 3, 3],
        "team_h_difficulty": [3, 3, 3, 3],
        "team_a_difficulty": [3, 3, 3, 3],
        "kickoff_time": pd.to_datetime(["2026-09-19", "2026-09-21", "2026-09-26", "2026-10-03"], utc=True),
        "finished": [False, False, False, False],
    })


def test_fixture_model_home_advantage_and_clean_sheet_probability():
    empty_results = pd.DataFrame(columns=["team_id", "xg", "xgc"])
    ratings = build_team_ratings(empty_results, _teams(), _fixtures())
    upcoming = build_upcoming_team_fixtures(_fixtures(), ratings)
    first = upcoming[upcoming["fixture_id"] == 10].set_index("is_home")
    # Equal teams: the home side projects more goals than the away side.
    assert first.loc[True, "xg_for"] > first.loc[False, "xg_for"]
    assert np.allclose(upcoming["cs_prob"], np.exp(-upcoming["xg_against"]))


def test_next_fixtures_label_marks_double_and_blank_gameweeks():
    ratings = build_team_ratings(pd.DataFrame(columns=["team_id", "xg", "xgc"]), _teams(), _fixtures())
    upcoming = build_upcoming_team_fixtures(_fixtures(), ratings, next_event=5)
    labels = next_fixtures_label(upcoming, n_gws=3)
    # Team 1: GW5 double (home BBB, away CCC), GW6 blank, GW7 home CCC
    assert labels[1] == "BBB (H)+CCC (A) — CCC (H)"


def test_upcoming_window_starts_at_next_deadline_gameweek():
    fixtures = _fixtures()
    ratings = build_team_ratings(pd.DataFrame(columns=["team_id", "xg", "xgc"]), _teams(), fixtures)
    # GW5 matches are unplayed but their deadline has passed: next deadline is GW6.
    upcoming = build_upcoming_team_fixtures(fixtures, ratings, next_event=6)
    assert upcoming["event"].min() == 6
    assert (upcoming["window_start"] == 6).all()
    # Team 1 has no GW6 match: the label starts with a blank, not with GW7.
    assert next_fixtures_label(upcoming, n_gws=2)[1] == "— CCC (H)"
