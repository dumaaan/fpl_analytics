import numpy as np
import pandas as pd
import pytest

from fpl_analytics.transform.projection import FIRST_VERSION, ModelSettings, model_inputs
from tests.test_transforms import SCORING


def _history(player_id: int, minutes: list[int], xg: list[float], position: str = "MID") -> pd.DataFrame:
    n = len(minutes)
    return pd.DataFrame({
        "player_id": player_id, "fixture_id": range(player_id * 100, player_id * 100 + n),
        "kickoff_time": pd.date_range("2025-08-16", periods=n, freq="7D", tz="UTC"),
        "minutes": minutes, "starts": [int(m >= 45) for m in minutes],
        "expected_goals": xg, "expected_assists": 0.0, "expected_goals_conceded": 1.0,
        "saves": 0, "bonus": 0, "yellow_cards": 0, "red_cards": 0, "defensive_contribution": 0,
    })


def test_minutes_model_counts_substitute_appearances_and_recent_matches_more():
    # Started the first 4, then 4 substitute appearances of 20 minutes.
    history = _history(1, [90, 90, 90, 90, 20, 20, 20, 20], [0.3] * 8)
    positions = pd.Series({1: "MID"})
    old = model_inputs(history, positions, SCORING, settings=FIRST_VERSION).loc[1]
    new = model_inputs(history, positions, SCORING, settings=ModelSettings(minutes_decay=0.6)).loc[1]  # a fast weight shows the effect clearly
    assert old["start_rate"] == 0.5 and old["p_sub"] == 0.0
    assert new["p60"] + new["p_sub"] == pytest.approx(1.0)  # always played
    assert new["p_sub"] > new["p60"]  # the recent pattern is substitute minutes
    assert new["exp_minutes"] < 90 * 0.5  # recent short appearances weigh more


def test_shrinkage_pulls_small_samples_harder_than_large_ones():
    # Two midfielders with the same 1.0 xG/90: one over 3 matches, one over 20.
    # A third player sets a low position average.
    history = pd.concat([
        _history(1, [90] * 3, [1.0] * 3),
        _history(2, [90] * 20, [1.0] * 20),
        _history(3, [90] * 30, [0.1] * 30),
    ], ignore_index=True)
    positions = pd.Series({1: "MID", 2: "MID", 3: "MID"})
    raw = model_inputs(history, positions, SCORING, settings=ModelSettings(shrink_minutes=0))
    pulled = model_inputs(history, positions, SCORING, settings=ModelSettings(shrink_minutes=900))
    assert raw.loc[1, "xg90"] == pytest.approx(1.0) and raw.loc[2, "xg90"] == pytest.approx(1.0)
    assert pulled.loc[1, "xg90"] < pulled.loc[2, "xg90"] < 1.0
    prior = (3 + 20 + 3) / (53 * 90) * 90  # position total xG per 90
    expected_small = (3.0 + prior * 10) / (270 + 900) * 90
    assert pulled.loc[1, "xg90"] == pytest.approx(expected_small)


def test_rates_stay_empty_below_the_minutes_floor():
    history = _history(1, [90, 90], [0.5, 0.5])
    inputs = model_inputs(history, pd.Series({1: "FWD"}), SCORING)
    assert np.isnan(inputs.loc[1, "xg90"])
    assert inputs.loc[1, "p60"] == pytest.approx(1.0)  # the minutes model still works
