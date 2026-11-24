import pandas as pd
import streamlit as st

from dashboard.catalog import FIXTURE_SIGNAL, METRICS, POSITION_PROFILE
from dashboard.charts import (
    SERIES_1,
    SERIES_2,
    gameweek_points_chart,
    line_chart,
    minutes_chart,
    multi_line_chart,
    percentile_profile_chart,
)
from dashboard.common import ease_cell_style, load_players_or_stop, position_percentiles
from dashboard.queries import get_player_history, get_team_fixtures
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90

NO_COMPARE = -1

players = load_players_or_stop().sort_values("total_points", ascending=False)
ids = players["id"].astype(int).tolist()
labels = {
    int(row.id): f"{row.web_name} ({row.team_short}, {row.position}, £{row.price:.1f}m)"
    for row in players.itertuples()
}

# Deep link: ?player=<id> selects the player (set by the position tables).
query_player = st.query_params.get("player")
if st.session_state.get("_player_query_seen") != query_player:
    if query_player and query_player.isdigit() and int(query_player) in labels:
        st.session_state["player_select"] = int(query_player)
    st.session_state["_player_query_seen"] = query_player
if st.session_state.get("player_select") not in labels:
    st.session_state["player_select"] = ids[0]

query_compare = st.query_params.get("compare")
if st.session_state.get("_compare_query_seen") != query_compare:
    if query_compare and query_compare.isdigit() and int(query_compare) in labels:
        st.session_state["player_compare"] = int(query_compare)
    st.session_state["_compare_query_seen"] = query_compare

pick_cols = st.columns(2)
with pick_cols[0]:
    player_id = st.selectbox("Player", ids, format_func=labels.get, key="player_select")
with pick_cols[1]:
    compare_id = st.selectbox(
        "Compare with", [NO_COMPARE, *ids], format_func=lambda pid: "No comparison" if pid == NO_COMPARE else labels[pid],
        key="player_compare",
    )
if query_player != str(player_id):
    st.query_params["player"] = str(player_id)
    st.session_state["_player_query_seen"] = str(player_id)
compare_value = None if compare_id == NO_COMPARE else str(compare_id)
if query_compare != compare_value:
    if compare_value is None:
        st.query_params.pop("compare", None)
    else:
        st.query_params["compare"] = compare_value
    st.session_state["_compare_query_seen"] = compare_value

player = players[players["id"] == player_id].iloc[0]
other = players[players["id"] == compare_id].iloc[0] if compare_id != NO_COMPARE else None
position = player["position"]
position_players = players[players["position"] == position]

# --- Header ---------------------------------------------------------------
st.title(player["web_name"])
st.caption(
    f"{player['team_name']} · {position} · £{player['price']:.1f}m · {player['status_label']} · "
    f"{int(player['games_played'])} games, {int(player['minutes'])} minutes"
    + (f" · {player['set_pieces']}" if player["set_pieces"] else "")
)
if player["news"]:
    (st.info if player["status"] == "a" else st.warning)(player["news"])

percentiles = position_percentiles(position_players, list(METRICS.keys() & set(position_players.columns)
                                                         - {"set_pieces", "games_played", "minutes"}))
own_pct = percentiles.loc[player.name]


def _top_share(key: str) -> str | None:
    value = own_pct.get(key)
    return None if pd.isna(value) else f"Top {max(1, round(100 - value))}% of {position}"


rank = int((position_players["total_points"] > player["total_points"]).sum()) + 1
kpis = st.columns(6)
kpis[0].metric("Total points", int(player["total_points"]), f"#{rank} of {len(position_players)} {position}",
               delta_color="off", delta_arrow="off", border=True)
kpis[1].metric("Points per match",
               "—" if pd.isna(player["points_per_match"]) else f"{player['points_per_match']:.2f}",
               _top_share("points_per_match"), delta_color="off", delta_arrow="off", border=True,
               help=METRICS["points_per_match"].help)
kpis[2].metric("xPts/90", "—" if pd.isna(player["xpts_per_90"]) else f"{player['xpts_per_90']:.2f}",
               _top_share("xpts_per_90"), delta_color="off", delta_arrow="off", border=True,
               help=METRICS["xpts_per_90"].help)
kpis[3].metric("VAPM", "—" if pd.isna(player["vapm"]) else f"{player['vapm']:.2f}", _top_share("vapm"),
               delta_color="off", delta_arrow="off", border=True, help=METRICS["vapm"].help)
kpis[4].metric("Selected by", f"{player['selected_by_percent']:.1f}%",
               f"{int(player['transfers_net_event']):+,} this GW", delta_color="off", delta_arrow="off",
               border=True)
tonight = player["price_change_tonight_pct"]
change = player["price_change_start"]
kpis[5].metric("Price progress", "—" if pd.isna(tonight) else f"{tonight:.0f}%",
               f"{'+' if change >= 0 else '−'}£{abs(change):.1f}m since start", delta_color="off", delta_arrow="off",
               border=True, help=METRICS["price_change_tonight_pct"].help)

# --- Profile and fixtures ------------------------------------------------
profile_keys = POSITION_PROFILE.get(position, [])
left, right = st.columns([1.3, 1], gap="large")
with left:
    st.subheader("Profile vs position")
    rows = []
    for subject in [player, other]:
        if subject is None or subject["position"] != position:
            continue
        subject_pct = percentiles.loc[subject.name]
        for key in profile_keys:
            metric = METRICS[key]
            value = subject[key]
            shown = "—" if pd.isna(value) else (metric.fmt % (value * 100 if metric.percent else value))
            rows.append({"player": subject["web_name"], "metric": metric.label,
                         "percentile": subject_pct.get(key), "value": shown})
    profile = pd.DataFrame(rows).dropna(subset=["percentile"]) if rows else pd.DataFrame()
    if other is not None and other["position"] != position:
        st.caption(f"{other['web_name']} plays {other['position']}, so the profile shows {player['web_name']} only.")
    if profile.empty:
        st.caption(f"Percentiles need {MIN_MINUTES_FOR_PER_90}+ minutes this season.")
    else:
        st.caption(f"100 = best in the position among players with {MIN_MINUTES_FOR_PER_90}+ minutes. "
                   "For xGC/90 and Mins/xGI, lower values rank higher.")
        st.altair_chart(percentile_profile_chart(profile), width="stretch")

with right:
    st.subheader("Next 5 gameweeks")
    fixtures = get_team_fixtures()
    team_fx = fixtures[fixtures["team_id"] == player["team"]] if not fixtures.empty else fixtures
    if team_fx.empty:
        st.caption("No upcoming fixtures.")
    else:
        first = int(fixtures["window_start"].iloc[0])
        _, rating_col, _ = FIXTURE_SIGNAL.get(position, ("", "fpl_difficulty", ""))
        team_fx = team_fx[team_fx["event"] < first + 5]
        table = pd.DataFrame({
            "GW": team_fx["event"].astype(int),
            "Opponent": [o if h else o.lower() for o, h in zip(team_fx["opponent_short"], team_fx["is_home"])],
            "Venue": team_fx["is_home"].map({True: "Home", False: "Away"}),
            "CS chance": team_fx["cs_prob"].map("{:.0%}".format),
            "Team xG": team_fx["xg_for"].map("{:.2f}".format),
            "FDR": team_fx["fpl_difficulty"].astype(int),
        })
        ratings = team_fx[rating_col].to_numpy()
        styled = table.style.apply(
            lambda _: [ease_cell_style(r) for r in ratings], subset=["Opponent"], axis=0
        )
        st.dataframe(styled, hide_index=True, width="stretch")
        blanks = sorted(set(range(first, first + 5)) - set(team_fx["event"].astype(int)))
        if blanks:
            st.caption("Blank gameweek: " + ", ".join(f"GW{e}" for e in blanks))
        st.caption(f"Start rate {player['start_rate']:.0%} · 60+ minute rate "
                   f"{player['sixty_minute_rate']:.0%}" if pd.notna(player["start_rate"])
                   and pd.notna(player["sixty_minute_rate"]) else "")

if other is not None:
    st.subheader(f"{player['web_name']} vs {other['web_name']}")
    compare_keys = ["total_points", "points_per_match", "xpts_next_3gw", "xpts_per_90", "points_per_90", "luck_per_90",
                    "vapm", "xgi_per_90", "bonus_per_90", "start_rate"]
    # Column names must differ even for two players with the same web name.
    name_a = f"{player['web_name']} ({player['team_short']})"
    name_b = f"{other['web_name']} ({other['team_short']})"
    if name_a == name_b:
        name_b += " "

    def _formatted(subject: pd.Series, key: str) -> str:
        metric, value = METRICS[key], subject[key]
        return "—" if pd.isna(value) else metric.fmt % (value * 100 if metric.percent else value)

    comparison = pd.DataFrame({
        "Metric": [METRICS[k].label for k in compare_keys],
        name_a: [_formatted(player, k) for k in compare_keys],
        name_b: [_formatted(other, k) for k in compare_keys],
    })
    cols = st.columns([1, 1.4], gap="large")
    with cols[0]:
        st.dataframe(comparison, hide_index=True, width="stretch")
    with cols[1]:
        both = []
        for subject, name in [(player, name_a), (other, name_b)]:
            h = get_player_history(int(subject["id"]))
            if not h.empty:
                per_gw = h.groupby("round", as_index=False)["total_points"].sum()
                per_gw[name] = per_gw["total_points"].cumsum()
                both.append(per_gw[["round", name]].set_index("round"))
        if both:
            merged = pd.concat(both, axis=1).ffill().fillna(0).reset_index()
            names = [c for c in merged.columns if c != "round"]
            st.altair_chart(
                multi_line_chart(merged, "round", {n: n for n in names}, "Gameweek", "Cumulative points",
                                 colors=[SERIES_1, SERIES_2]),
                width="stretch",
            )

# --- Gameweek history ----------------------------------------------------
history = get_player_history(int(player_id))
if history.empty:
    st.caption("No gameweek history yet for this player.")
    st.stop()

st.subheader("Season so far")
row1 = st.columns(2, gap="large")
with row1[0]:
    st.caption("Points per gameweek, split by source. DNP = did not play.")
    st.altair_chart(gameweek_points_chart(history), width="stretch")
with row1[1]:
    st.caption("Minutes per gameweek. The dashed line marks 60 minutes.")
    st.altair_chart(minutes_chart(history), width="stretch")

per_gw = history.groupby("round", as_index=False).agg(
    goals=("goals_scored", "sum"), xg=("expected_goals", "sum"), assists=("assists", "sum"),
    xa=("expected_assists", "sum"), price=("price", "last"), selected=("selected", "last"),
)
cumulative = per_gw.assign(
    goals=per_gw["goals"].cumsum(), xg=per_gw["xg"].cumsum(),
    assists=per_gw["assists"].cumsum(), xa=per_gw["xa"].cumsum(),
)
row2 = st.columns(2, gap="large")
with row2[0]:
    st.caption("Cumulative goals vs expected goals.")
    st.altair_chart(multi_line_chart(cumulative, "round", {"goals": "Goals", "xg": "xG"}, "Gameweek", "Goals"),
                    width="stretch")
with row2[1]:
    st.caption("Cumulative assists vs expected assists.")
    st.altair_chart(multi_line_chart(cumulative, "round", {"assists": "Assists", "xa": "xA"}, "Gameweek", "Assists"),
                    width="stretch")

row3 = st.columns(2, gap="large")
with row3[0]:
    st.caption("Price (£m) at each gameweek.")
    st.altair_chart(line_chart(per_gw, "round", "price", "Gameweek", "Price (£m)", y_format=".1f"), width="stretch")
with row3[1]:
    st.caption("Number of FPL teams that own the player.")
    st.altair_chart(line_chart(per_gw, "round", "selected", "Gameweek", "Owners"), width="stretch")
