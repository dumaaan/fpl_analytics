import streamlit as st

from dashboard.charts import dual_line_chart, line_chart
from dashboard.queries import get_all_players, get_player_history

st.set_page_config(page_title="Player Detail - FPL Analytics", layout="wide")
st.title("Player Detail")

players = get_all_players()
if players.empty:
    st.warning("No data yet. Run `python -m fpl_analytics.pipeline` first.")
    st.stop()

players = players.sort_values("web_name")
options = players["id"].tolist()
labels = {
    row["id"]: f"{row['web_name']} ({row['team_name']}, {row['position']})"
    for _, row in players.iterrows()
}

player_id = st.selectbox("Search for a player", options, format_func=lambda pid: labels[pid])
player = players[players["id"] == player_id].iloc[0]

st.subheader(f"{player['web_name']} — {player['team_name']} ({player['position']})")

metric_cols = st.columns(7)
metric_cols[0].metric("Price", f"£{player['price']:.1f}m")
metric_cols[1].metric("Total Points", int(player["total_points"]))
metric_cols[2].metric("Form", f"{player['form']:.1f}")
metric_cols[3].metric("Selected By", f"{player['selected_by_percent']:.1f}%")
metric_cols[4].metric("Minutes", int(player["minutes"]))
metric_cols[5].metric("VAPM", f"{player['vapm']:.2f}")
metric_cols[6].metric("Expected Points (Next GW)", f"{player['ep_next']:.1f}")

if player["news"]:
    st.info(player["news"])

history = get_player_history(int(player_id))
if history.empty:
    st.caption("No gameweek history yet for this player.")
    st.stop()

chart_cols = st.columns(2)
with chart_cols[0]:
    st.altair_chart(
        line_chart(history, "round", "total_points", "Gameweek", "Points", "Points per Gameweek"),
        width="stretch",
    )
with chart_cols[1]:
    st.altair_chart(
        line_chart(history, "round", "minutes", "Gameweek", "Minutes", "Minutes per Gameweek"),
        width="stretch",
    )

chart_cols2 = st.columns(2)
with chart_cols2[0]:
    st.altair_chart(
        dual_line_chart(
            history,
            "round",
            "goals_scored",
            "Goals Scored",
            "expected_goals",
            "Expected Goals",
            "Gameweek",
            "Goals",
            "Goals Scored vs Expected Goals per Gameweek",
        ),
        width="stretch",
    )
with chart_cols2[1]:
    st.altair_chart(
        line_chart(history, "round", "ict_index", "Gameweek", "ICT Index", "ICT Index per Gameweek"),
        width="stretch",
    )

chart_cols3 = st.columns(2)
with chart_cols3[0]:
    st.altair_chart(
        line_chart(history, "round", "price", "Gameweek", "Price (£m)", "Price Over Time"),
        width="stretch",
    )
with chart_cols3[1]:
    st.altair_chart(
        line_chart(history, "round", "selected", "Gameweek", "Owners", "Ownership Over Time"),
        width="stretch",
    )

if player["position"] != "GK":
    st.caption(
        "Defensive Contribution per gameweek — the combined stat (CBIT for "
        "defenders, CBIRT for midfielders/forwards) that needs to cross 10 or "
        "12 respectively to earn the 2-point bonus that match."
    )
    st.altair_chart(
        line_chart(
            history, "round", "defensive_contribution", "Gameweek", "Defensive Contribution",
            "Defensive Contribution per Gameweek",
        ),
        width="stretch",
    )
