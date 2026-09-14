import numpy as np
import pandas as pd

from fpl_analytics.config import (
    BLANK_POINTS,
    HAUL_POINTS,
    MIN_MINUTES_FOR_PER_90,
    OUTFIELD_POSITIONS,
    RECENT_FORM_GWS,
    RECENT_FORM_MIN_MINUTES,
    SAVES_PER_SAVE_POINT,
)
from fpl_analytics.transform.metrics import per_90, safe_divide, shrunk_rate
from fpl_analytics.transform.scoring import POINT_SOURCES, Scoring, add_points_breakdown
from fpl_analytics.transform.teams import assign_team_ids, next_fixtures_label

# FPL's own pre-computed per-90 fields get the same small-sample floor as
# per_90() (they come straight from the API, so they skip that function).
FPL_NATIVE_PER_90_COLUMNS = [
    "defensive_contribution_per_90",
    "clean_sheets_per_90",
    "starts_per_90",
    "expected_goals_per_90",
    "expected_assists_per_90",
    "expected_goal_involvements_per_90",
    "expected_goals_conceded_per_90",
    "saves_per_90",
    "goals_conceded_per_90",
]

STATUS_LABELS = {
    "a": "Available",
    "d": "Doubtful",
    "i": "Injured",
    "s": "Suspended",
    "u": "Unavailable",
    "n": "Not in squad",
}


def build_mart_player_gameweeks(
    history: pd.DataFrame,
    players_raw: pd.DataFrame,
    fixtures: pd.DataFrame,
    scoring: Scoring,
) -> pd.DataFrame:
    """One row per player per team fixture, with the team played for, the
    points split by source, and DefCon hit / near-miss flags."""
    df = history.copy()
    df["position"] = df["player_id"].map(players_raw.set_index("id")["position"]).fillna("Unknown")
    df["team_id"] = assign_team_ids(df, fixtures) if not df.empty else pd.Series(dtype="Int64")
    return add_points_breakdown(df, scoring)


def attach_team_xg(player_gameweeks: pd.DataFrame, team_results: pd.DataFrame) -> pd.DataFrame:
    df = player_gameweeks.copy()
    lookup = team_results.set_index(["fixture_id", "team_id"])["xg"]
    df["team_xg"] = lookup.reindex(list(zip(df["fixture_id"], df["team_id"]))).to_numpy()
    return df


def _gameweek_aggregates(player_gameweeks: pd.DataFrame) -> pd.DataFrame:
    """Season aggregates that need per-match rows."""
    pg = player_gameweeks.copy()
    pg["non_appearance_points"] = pg["total_points"] - pg["pts_appearance"]
    pg["played_60"] = pg["minutes"] >= 60
    pg["is_return"] = (pg["goals_scored"] + pg["assists"]) > 0
    pg["is_haul"] = pg["total_points"] >= HAUL_POINTS
    pg["is_blank"] = pg["total_points"] <= BLANK_POINTS

    all_rows = pg.groupby("player_id").agg(
        team_fixtures=("fixture_id", "size"),
        player_xg_total=("expected_goals", "sum"),
        team_xg_total=("team_xg", "sum"),
        **{f"pts_{s}": (f"pts_{s}", "sum") for s in POINT_SOURCES},
        pts_unexplained=("pts_unexplained", "sum"),
    )

    # Only matches the player actually played. Rows with 0 minutes would add
    # 0 to every total but 1 to every count, and halve rates for one missed game.
    played = pg[pg["minutes"] > 0]
    played_agg = played.groupby("player_id").agg(
        games_played=("minutes", "size"),
        games_60=("played_60", "sum"),
        return_rate=("is_return", "mean"),
        haul_rate=("is_haul", "mean"),
        blank_rate=("is_blank", "mean"),
        ceiling=("total_points", lambda s: s.quantile(0.9)),
        volatility=("total_points", "std"),
        avg_non_appearance_points=("non_appearance_points", "mean"),
        defcon_hits=("defcon_hit", "sum"),
        defcon_near_misses=("defcon_near_miss", "sum"),
        price_last=("price", "last"),
    )

    last_round = pg["round"].max()
    recent = pg[pg["round"] > last_round - RECENT_FORM_GWS]
    recent_agg = recent.groupby("player_id").agg(
        recent_minutes=("minutes", "sum"),
        recent_xgi=("expected_goal_involvements", "sum"),
        points_last_4=("total_points", "sum"),
    )

    return all_rows.join(played_agg, how="left").join(recent_agg, how="left")


def _fixture_projections(
    df: pd.DataFrame, upcoming: pd.DataFrame, ratings: pd.DataFrame, scoring: Scoring
) -> pd.DataFrame:
    """Expected points for each player's next gameweek and next 3 gameweeks.

    Per fixture: play_prob * (appearance + attack returns scaled by how many
    goals the team is projected to score vs its average + clean-sheet points
    at the fixture's clean-sheet chance + goals-conceded deduction at the
    fixture's projected goals against + saves + DefCon + bonus + cards).
    """
    out = pd.DataFrame(index=df.index)
    for col in ["xpts_next_gw", "xpts_next_3gw", "xg_next_3gw", "cs_expected_next_gw", "fixtures_next_gw"]:
        out[col] = np.nan
    if upcoming.empty:
        return out

    first_event = int(upcoming["window_start"].iloc[0])
    team_xg_pg = ratings.set_index("team_id")["xg_per_game"]
    fx = upcoming[upcoming["event"] < first_event + 3][
        ["team_id", "event", "xg_for", "xg_against", "cs_prob"]
    ].copy()
    fx["attack_mult"] = fx["xg_for"] / fx["team_id"].map(team_xg_pg)

    p = df.reset_index()[[
        "index", "team", "position", "play_prob", "xg90", "xa90", "saves90", "bonus90", "cards90",
        "defcon_rate_for_xpts",
    ]]
    m = p.merge(fx, left_on="team", right_on="team_id", how="left")

    pos = m["position"]
    goal_w = scoring.by_position("goals_scored", pos)
    cs_w = scoring.by_position("clean_sheets", pos)
    gc_w = scoring.by_position("goals_conceded", pos)
    defcon_w = scoring.by_position("defensive_contribution", pos)

    m["xpts"] = m["play_prob"] * (
        scoring.value("long_play", "")
        + (goal_w * m["xg90"] + scoring.value("assists", "") * m["xa90"]) * m["attack_mult"]
        + cs_w * m["cs_prob"]
        + gc_w * m["xg_against"] / 2
        + m["saves90"] / SAVES_PER_SAVE_POINT * scoring.value("saves", "")
        + defcon_w * m["defcon_rate_for_xpts"]
        + m["bonus90"]
        + m["cards90"]
    )
    m["xg_fixture"] = m["play_prob"] * m["xg90"] * m["attack_mult"]

    next_gw = m[m["event"] == first_event]
    has_fixture = m["event"].notna()
    grouped_3 = m[has_fixture].groupby("index")
    grouped_1 = next_gw.groupby("index")

    out["xpts_next_3gw"] = grouped_3["xpts"].sum(min_count=1)
    out["xg_next_3gw"] = grouped_3["xg_fixture"].sum(min_count=1)
    out["xpts_next_gw"] = grouped_1["xpts"].sum(min_count=1)
    out["cs_expected_next_gw"] = grouped_1["cs_prob"].sum(min_count=1)
    out["fixtures_next_gw"] = grouped_1["event"].size()

    # A blank gameweek is a real 0, but only for players we can project at all.
    projectable = df["xg90"].notna()
    for col in ["xpts_next_gw", "cs_expected_next_gw", "fixtures_next_gw"]:
        out.loc[projectable & out[col].isna(), col] = 0.0
    out.loc[~projectable, ["xpts_next_gw", "xpts_next_3gw", "xg_next_3gw"]] = np.nan
    return out


def build_mart_players(
    players_raw: pd.DataFrame,
    teams: pd.DataFrame,
    player_gameweeks: pd.DataFrame,
    team_ratings: pd.DataFrame,
    upcoming_fixtures: pd.DataFrame,
    scoring: Scoring,
) -> pd.DataFrame:
    """One row per player: identity, season totals, and every derived metric."""
    df = players_raw.merge(
        teams[["id", "name", "short_name"]].rename(
            columns={"id": "team", "name": "team_name", "short_name": "team_short"}
        ),
        on="team",
        how="left",
    )

    df["price"] = df["now_cost"] / 10.0
    df["price_change_start"] = df["cost_change_start"] / 10.0
    df["transfers_net_event"] = df["transfers_in_event"] - df["transfers_out_event"]
    minutes = df["minutes"]
    below_floor = minutes < MIN_MINUTES_FOR_PER_90

    for col in FPL_NATIVE_PER_90_COLUMNS:
        df.loc[below_floor, col] = np.nan

    # --- Season rates -----------------------------------------------------
    df["goals_per_90"] = per_90(df["goals_scored"], minutes)
    df["assists_per_90"] = per_90(df["assists"], minutes)
    df["xg_per_90"] = per_90(df["expected_goals"], minutes)
    df["xa_per_90"] = per_90(df["expected_assists"], minutes)
    df["xgi_per_90"] = df["xg_per_90"] + df["xa_per_90"]
    df["goal_involvement_per_90"] = df["goals_per_90"] + df["assists_per_90"]
    df["points_per_90"] = per_90(df["total_points"], minutes)
    df["bonus_per_90"] = per_90(df["bonus"], minutes)
    df["bps_per_90"] = per_90(df["bps"], minutes)
    df["ict_index_per_90"] = per_90(df["ict_index"], minutes)
    df["threat_per_90"] = per_90(df["threat"], minutes)
    df["creativity_per_90"] = per_90(df["creativity"], minutes)
    df["card_points_per_90"] = per_90(
        df["yellow_cards"] * scoring.value("yellow_cards", "") + df["red_cards"] * scoring.value("red_cards", ""),
        minutes,
    )
    df["points_per_million"] = safe_divide(df["total_points"], df["price"])

    df["goals_minus_xg"] = df["goals_scored"] - df["expected_goals"]
    df["assists_minus_xa"] = df["assists"] - df["expected_assists"]
    df["goal_involvement_overperformance"] = df["goals_minus_xg"] + df["assists_minus_xa"]
    df["goals_prevented"] = df["expected_goals_conceded"] - df["goals_conceded"]

    goal_involvements = df["goals_scored"] + df["assists"]
    df["minutes_per_goal_involvement"] = safe_divide(minutes, goal_involvements)
    df["minutes_per_xgi"] = safe_divide(minutes, df["expected_goal_involvements"]).where(~below_floor)

    df["cbit"] = df["clearances_blocks_interceptions"] + df["tackles"]
    df["cbirt"] = df["cbit"] + df["recoveries"]
    df["tackles_per_90"] = per_90(df["tackles"], minutes)
    df["cbi_per_90"] = per_90(df["clearances_blocks_interceptions"], minutes)
    df["recoveries_per_90"] = per_90(df["recoveries"], minutes)

    # --- Roles and availability ------------------------------------------
    def _set_pieces(row: pd.Series) -> str:
        roles = [
            ("Pens", row["penalties_order"]),
            ("Corners", row["corners_and_indirect_freekicks_order"]),
            ("FKs", row["direct_freekicks_order"]),
        ]
        return " · ".join(f"{name} {int(order)}" for name, order in roles if pd.notna(order))

    df["set_pieces"] = df.apply(_set_pieces, axis=1)
    df["is_penalty_taker"] = df["penalties_order"] == 1

    chance = df["chance_of_playing_next_round"]
    df["availability"] = np.where(df["status"] == "a", 1.0, (chance / 100).fillna(0.0))
    df["status_label"] = df["status"].map(STATUS_LABELS).fillna("Unknown")
    doubtful = (df["status"] != "a") & chance.notna() & (chance > 0)
    df.loc[doubtful, "status_label"] = df.loc[doubtful, "status_label"] + " " + chance[doubtful].astype(int).astype(str) + "%"

    # --- Per-match aggregates ---------------------------------------------
    if not player_gameweeks.empty:
        agg = _gameweek_aggregates(player_gameweeks)
        df = df.merge(agg, left_on="id", right_index=True, how="left")
    else:
        agg = pd.DataFrame()
    count_cols = ["team_fixtures", "games_played", "games_60", "defcon_hits", "defcon_near_misses",
                  *[f"pts_{s}" for s in POINT_SOURCES], "pts_unexplained"]
    for col in count_cols:
        df[col] = df[col].fillna(0) if col in df else 0.0
    for col in ["return_rate", "haul_rate", "blank_rate", "ceiling", "volatility", "avg_non_appearance_points",
                "player_xg_total", "team_xg_total", "recent_minutes", "recent_xgi", "points_last_4", "price_last"]:
        if col not in df:
            df[col] = np.nan

    df = df.copy()  # defragment after the many single-column inserts above

    # Value Added Per Million: points beyond the automatic appearance points,
    # per game actually played, divided by price. Plain points-per-million
    # rewards players just for being nailed-on starters; VAPM isolates returns.
    df["vapm"] = safe_divide(df["avg_non_appearance_points"], df["price"])
    df["start_rate"] = safe_divide(df["starts"], df["team_fixtures"])
    df["sixty_minute_rate"] = safe_divide(df["games_60"], df["starts"])
    df["clean_sheet_rate_60"] = safe_divide(df["clean_sheets"], df["games_60"])
    df["xg_share"] = safe_divide(df["player_xg_total"], df["team_xg_total"])
    df["recent_xgi_per_90"] = np.where(
        df["recent_minutes"] >= RECENT_FORM_MIN_MINUTES, df["recent_xgi"] * 90 / df["recent_minutes"], np.nan
    )
    df["xgi_trend"] = df["recent_xgi_per_90"] - df["xgi_per_90"]

    outfield = df["position"].isin(OUTFIELD_POSITIONS)
    df["defcon_hit_rate"] = safe_divide(df["defcon_hits"], df["games_played"]).where(outfield)
    df["defcon_near_miss_rate"] = safe_divide(df["defcon_near_misses"], df["games_played"]).where(outfield)
    position_totals = df[outfield].groupby("position")[["defcon_hits", "games_played"]].sum()
    position_rate = safe_divide(position_totals["defcon_hits"], position_totals["games_played"])
    df["defcon_hit_rate_adj"] = shrunk_rate(
        df["defcon_hits"], df["games_played"], df["position"].map(position_rate)
    ).where(outfield)
    df["defcon_points_per_90"] = per_90(df["pts_defcon"], minutes).where(outfield)
    df["save_points_per_90"] = per_90(df["pts_saves"], minutes).where(df["position"] == "GK")

    # --- Expected points per 90 (season) ---------------------------------
    df = df.copy()
    pos = df["position"]
    xgc90 = df["expected_goals_conceded_per_90"]
    df["xg90"] = df["xg_per_90"]
    df["xa90"] = df["xa_per_90"]
    df["saves90"] = df["saves_per_90"].where(pos == "GK", 0.0)
    df["bonus90"] = df["bonus_per_90"]
    df["cards90"] = df["card_points_per_90"]
    df["defcon_rate_for_xpts"] = df["defcon_hit_rate_adj"].fillna(0.0)
    df["xpts_per_90"] = (
        scoring.value("long_play", "")
        + scoring.by_position("goals_scored", pos) * df["xg90"]
        + scoring.value("assists", "") * df["xa90"]
        + scoring.by_position("clean_sheets", pos) * np.exp(-xgc90)
        + scoring.by_position("goals_conceded", pos) * xgc90 / 2
        + df["saves90"] / SAVES_PER_SAVE_POINT * scoring.value("saves", "")
        + scoring.by_position("defensive_contribution", pos) * df["defcon_rate_for_xpts"]
        + df["bonus90"]
        + df["cards90"]
    )
    df["luck_per_90"] = df["points_per_90"] - df["xpts_per_90"]

    # --- Fixture-adjusted projections ------------------------------------
    df["play_prob"] = df["start_rate"].fillna(0.0) * df["availability"]
    projections = _fixture_projections(df, upcoming_fixtures, team_ratings, scoring)
    df = df.join(projections)

    if not upcoming_fixtures.empty:
        df["next_fixtures"] = df["team"].map(next_fixtures_label(upcoming_fixtures)).fillna("")
    else:
        df["next_fixtures"] = ""

    return df.drop(columns=["xg90", "xa90", "saves90", "bonus90", "cards90", "defcon_rate_for_xpts",
                            "avg_non_appearance_points", "price_last"])
