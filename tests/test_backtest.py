import pandas as pd

from fpl_analytics.advisor import Lineup
from fpl_analytics.backtest.data import Season
from fpl_analytics.backtest.projectors import as_of
from fpl_analytics.backtest.replay import score_week
from tests.test_transforms import SCORING as RULES


def _positions():
    pos = {1: "GK", 2: "GK"}
    pos.update({i: "DEF" for i in range(3, 8)})
    pos.update({i: "MID" for i in range(8, 13)})
    pos.update({i: "FWD" for i in range(13, 16)})
    return pos


def _lineup(captain=13, vice=8):
    # 4-4-2: GK 1; DEF 3-6; MID 8-11; FWD 13-14. Bench: GK 2, then 7, 12, 15.
    return Lineup(starters=[1, 3, 4, 5, 6, 8, 9, 10, 11, 13, 14], bench=[2, 7, 12, 15],
                  captain=captain, vice_captain=vice, formation=(4, 4, 2), score=0.0)


def test_score_week_counts_captain_twice():
    points = {pid: 2 for pid in range(1, 16)} | {13: 10}
    minutes = {pid: 90 for pid in range(1, 16)}
    assert score_week(_lineup(), _positions(), points, minutes) == 10 * 2 + 10 + 10


def test_autosub_keeps_formation_and_vice_takes_over():
    points = {pid: 2 for pid in range(1, 16)}
    minutes = {pid: 90 for pid in range(1, 16)}
    # Captain (FWD 13) and a defender (3) do not play; bench DEF 7 is first legal outfield sub.
    minutes[13] = 0
    minutes[3] = 0
    points[8] = 6  # vice-captain
    total = score_week(_lineup(), _positions(), points, minutes)
    # Starters after subs: 7 replaces 3, 12 replaces 13 (4-5-1 is legal); vice 8 doubled.
    assert total == 2 * 10 + 6 + 6


def test_as_of_uses_only_matches_before_the_deadline():
    history = pd.DataFrame({
        "player_id": [1, 1, 1], "fixture_id": [10, 11, 12], "round": [1, 2, 3], "team_id": [1, 1, 1],
        "position": ["MID"] * 3, "minutes": [90] * 3, "starts": [1] * 3, "kickoff_time": pd.to_datetime(
            ["2025-08-16", "2025-08-23", "2025-08-30"], utc=True),
        "expected_goals": [0.5, 0.5, 5.0], "expected_assists": [0.0] * 3, "expected_goals_conceded": [1.0] * 3,
        "saves": [0] * 3, "bonus": [0] * 3, "yellow_cards": [0] * 3, "red_cards": [0] * 3,
        "defensive_contribution": [0] * 3, "total_points": [2, 2, 20], "was_home": [True] * 3, "price": [6.0] * 3,
    })
    fixtures = pd.DataFrame({
        "id": [10, 11, 12], "event": pd.array([1, 2, 3], dtype="Int64"), "team_h": [1, 1, 1], "team_a": [2, 2, 2],
        "team_h_difficulty": [3] * 3, "team_a_difficulty": [3] * 3, "finished": [True] * 3,
        "kickoff_time": pd.to_datetime(["2025-08-16", "2025-08-23", "2025-08-30"], utc=True),
        "team_h_score": [1, 1, 1], "team_a_score": [0, 0, 0],
    })
    teams = pd.DataFrame({"id": [1, 2], "short_name": ["AAA", "BBB"],
                          **{c: [0, 0] for c in ["strength_attack_home", "strength_attack_away",
                                                 "strength_defence_home", "strength_defence_away"]}})
    season = Season("test", history, fixtures, teams, RULES, pd.Series({1: "P"}), last_complete_gw=3)
    ctx = as_of(season, 3)
    assert ctx.history["round"].max() == 2  # the 5.0 xG game in GW3 is not visible
    assert ctx.history["expected_goals"].sum() == 1.0
    assert ctx.upcoming["event"].min() == 3
