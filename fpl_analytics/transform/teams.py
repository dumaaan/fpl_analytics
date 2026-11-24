import numpy as np
import pandas as pd

from fpl_analytics.config import (
    FIXTURE_HORIZON_GWS,
    HOME_ADVANTAGE,
    LEAGUE_AVG_GOALS_FALLBACK,
    SHRINK_PRIOR_GAMES,
)


def assign_team_ids(history: pd.DataFrame, fixtures: pd.DataFrame) -> pd.Series:
    """The team a history row was played for, from the fixture and was_home.
    Correct for players who moved club mid-season, unlike their current team."""
    lookup = fixtures.set_index("id")[["team_h", "team_a"]]
    team_h = history["fixture_id"].map(lookup["team_h"])
    team_a = history["fixture_id"].map(lookup["team_a"])
    return pd.Series(np.where(history["was_home"].astype(bool), team_h, team_a), index=history.index)


def build_team_fixture_results(player_gameweeks: pd.DataFrame, fixtures: pd.DataFrame) -> pd.DataFrame:
    """One row per team per finished fixture: team xG (sum of its players' xG),
    xG conceded (the opponent's xG), and actual goals."""
    columns = ["fixture_id", "event", "team_id", "opponent_id", "is_home", "xg", "xgc", "goals_for", "goals_against"]
    finished = fixtures[fixtures["finished"].astype(bool)]
    if player_gameweeks.empty or finished.empty:
        return pd.DataFrame(columns=columns)

    team_xg = player_gameweeks.groupby(["fixture_id", "team_id"], as_index=False)["expected_goals"].sum()
    xg_lookup = team_xg.set_index(["fixture_id", "team_id"])["expected_goals"]

    sides = []
    for is_home, team_col, opp_col, gf_col, ga_col in [
        (True, "team_h", "team_a", "team_h_score", "team_a_score"),
        (False, "team_a", "team_h", "team_a_score", "team_h_score"),
    ]:
        side = pd.DataFrame(
            {
                "fixture_id": finished["id"],
                "event": finished["event"],
                "team_id": finished[team_col],
                "opponent_id": finished[opp_col],
                "is_home": is_home,
                "goals_for": finished[gf_col],
                "goals_against": finished[ga_col],
            }
        )
        sides.append(side)
    results = pd.concat(sides, ignore_index=True)

    keys = list(zip(results["fixture_id"], results["team_id"]))
    opp_keys = list(zip(results["fixture_id"], results["opponent_id"]))
    results["xg"] = xg_lookup.reindex(keys).to_numpy()
    results["xgc"] = xg_lookup.reindex(opp_keys).to_numpy()
    return results[columns]


def _strength_indices(teams: pd.DataFrame, fixtures: pd.DataFrame | None) -> tuple[pd.Series, pd.Series]:
    """(attack, defence) strength per team, each scaled to a league mean of 1.

    Uses FPL's strength_attack/defence ratings when published. FPL sends them
    as 0 in some seasons; then both indices come from fixture difficulty: the
    average difficulty other teams are given when they face this team (FDR
    rates the opponent, so it is one overall strength). With neither, all 1.
    """
    team_ids = teams["id"]
    attack = (teams["strength_attack_home"] + teams["strength_attack_away"]) / 2
    defence = (teams["strength_defence_home"] + teams["strength_defence_away"]) / 2
    if (attack > 0).all() and (defence > 0).all():
        return (
            pd.Series((attack / attack.mean()).to_numpy(), index=team_ids),
            pd.Series((defence / defence.mean()).to_numpy(), index=team_ids),
        )
    if fixtures is not None and not fixtures.empty:
        faced = pd.concat([
            fixtures[["team_a", "team_h_difficulty"]].set_axis(["team_id", "difficulty"], axis=1),
            fixtures[["team_h", "team_a_difficulty"]].set_axis(["team_id", "difficulty"], axis=1),
        ])
        strength = faced.groupby("team_id")["difficulty"].mean().reindex(team_ids)
        if strength.notna().all() and (strength > 0).all():
            index = strength / strength.mean()
            return index, index
    ones = pd.Series(1.0, index=team_ids)
    return ones, ones


def build_team_ratings(
    team_results: pd.DataFrame, teams: pd.DataFrame, fixtures: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Per-team attack (xG per game) and defence (xG conceded per game).

    Early-season samples are tiny, so each rate is shrunk toward a prior:
    the league average scaled by the team's attack (or defence) strength
    index. With no finished matches the ratings are the prior alone.
    """
    played = team_results.dropna(subset=["xg", "xgc"])
    league_avg = float(played["xg"].mean()) if not played.empty else LEAGUE_AVG_GOALS_FALLBACK

    t = teams[["id", "short_name"]].rename(columns={"id": "team_id"}).copy()
    attack_index, defence_index = _strength_indices(teams, fixtures)
    t["prior_xg"] = league_avg * t["team_id"].map(attack_index)
    # Stronger defence -> fewer goals conceded.
    t["prior_xgc"] = league_avg / t["team_id"].map(defence_index)

    agg = played.groupby("team_id").agg(games=("xg", "size"), xg_total=("xg", "sum"), xgc_total=("xgc", "sum"))
    t = t.merge(agg, left_on="team_id", right_index=True, how="left")
    t[["games", "xg_total", "xgc_total"]] = t[["games", "xg_total", "xgc_total"]].astype(float).fillna(0)

    k = SHRINK_PRIOR_GAMES
    t["xg_per_game"] = (t["xg_total"] + k * t["prior_xg"]) / (t["games"] + k)
    t["xgc_per_game"] = (t["xgc_total"] + k * t["prior_xgc"]) / (t["games"] + k)
    t["league_avg_goals"] = league_avg
    return t[["team_id", "short_name", "games", "xg_per_game", "xgc_per_game", "league_avg_goals"]]


def _ease_rating(values: pd.Series, higher_is_easier: bool) -> pd.Series:
    """Quintile rating over all upcoming fixtures: 1 = easiest, 5 = hardest."""
    if len(values) < 5:
        return pd.Series(3, index=values.index)
    ranks = values.rank(method="first", ascending=not higher_is_easier)
    return pd.qcut(ranks, 5, labels=[1, 2, 3, 4, 5]).astype(int)


def build_upcoming_team_fixtures(
    fixtures: pd.DataFrame,
    ratings: pd.DataFrame,
    next_event: int | None = None,
    horizon_gws: int = FIXTURE_HORIZON_GWS,
) -> pd.DataFrame:
    """One row per team per upcoming fixture in the next `horizon_gws` gameweeks,
    with projected goals for/against, clean-sheet probability, and 1-5 ease
    ratings for attackers (goals for) and defenders (clean-sheet chance).

    The window starts at `next_event`, the gameweek of the next transfer
    deadline. Unplayed matches of the current gameweek are left out: their
    deadline has passed, so no FPL decision can use them. `window_start`
    stores that gameweek, so a team with a blank in it still reads correctly.

    Projection (a standard multiplicative model):
      xg_for     = avg * (team attack / avg) * (opponent defence / avg) * home factor
      xg_against = avg * (opponent attack / avg) * (team defence / avg) / home factor
      cs_prob    = exp(-xg_against)   (Poisson chance of conceding 0)
    """
    upcoming = fixtures[~fixtures["finished"].astype(bool) & fixtures["event"].notna()]
    if next_event is not None:
        upcoming = upcoming[upcoming["event"] >= next_event]
    if upcoming.empty or ratings.empty:
        return pd.DataFrame()
    first_event = int(next_event if next_event is not None else upcoming["event"].min())
    upcoming = upcoming[upcoming["event"] < first_event + horizon_gws]

    sides = []
    for is_home, team_col, opp_col, diff_col in [
        (True, "team_h", "team_a", "team_h_difficulty"),
        (False, "team_a", "team_h", "team_a_difficulty"),
    ]:
        sides.append(
            pd.DataFrame(
                {
                    "fixture_id": upcoming["id"],
                    "event": upcoming["event"].astype(int),
                    "kickoff_time": upcoming["kickoff_time"],
                    "team_id": upcoming[team_col],
                    "opponent_id": upcoming[opp_col],
                    "is_home": is_home,
                    "fpl_difficulty": upcoming[diff_col],
                }
            )
        )
    df = pd.concat(sides, ignore_index=True)

    r = ratings.set_index("team_id")
    avg = float(ratings["league_avg_goals"].iloc[0])
    home_factor = np.where(df["is_home"], HOME_ADVANTAGE, 1 / HOME_ADVANTAGE)

    team_xg = df["team_id"].map(r["xg_per_game"])
    team_xgc = df["team_id"].map(r["xgc_per_game"])
    opp_xg = df["opponent_id"].map(r["xg_per_game"])
    opp_xgc = df["opponent_id"].map(r["xgc_per_game"])

    df["team_short"] = df["team_id"].map(r["short_name"])
    df["opponent_short"] = df["opponent_id"].map(r["short_name"])
    df["xg_for"] = team_xg * opp_xgc / avg * home_factor
    df["xg_against"] = opp_xg * team_xgc / avg / home_factor
    df["cs_prob"] = np.exp(-df["xg_against"])
    df["window_start"] = first_event
    df["attack_ease"] = _ease_rating(df["xg_for"], higher_is_easier=True)
    df["defence_ease"] = _ease_rating(df["cs_prob"], higher_is_easier=True)
    return df.sort_values(["team_id", "event", "kickoff_time"]).reset_index(drop=True)


def next_fixtures_label(upcoming: pd.DataFrame, n_gws: int = 5) -> pd.Series:
    """Per team, the next n gameweeks as text: each opponent followed by (H)
    for home or (A) for away, doubles joined with "+", and a dash for a blank
    gameweek. Example: "ARS (H) CHE (A)+BOU (H) — LIV (A) MCI (H)".

    Venue is spelled out rather than coded by letter case, which readers
    missed.
    """
    if upcoming.empty:
        return pd.Series(dtype=str)
    first_event = int(upcoming["window_start"].iloc[0])
    events = list(range(first_event, first_event + n_gws))
    labels = {}
    for team_id, team_rows in upcoming.groupby("team_id"):
        parts = []
        for event in events:
            rows = team_rows[team_rows["event"] == event]
            if rows.empty:
                parts.append("—")
            else:
                parts.append(
                    "+".join(
                        f"{opp} ({'H' if home else 'A'})"
                        for opp, home in zip(rows["opponent_short"], rows["is_home"])
                    )
                )
        labels[team_id] = " ".join(parts)
    return pd.Series(labels, name="next_fixtures")
