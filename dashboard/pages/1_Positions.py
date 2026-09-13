import streamlit as st

from dashboard.queries import get_all_players

st.set_page_config(page_title="Positions - FPL Analytics", layout="wide")
st.title("Position Leaderboards")

players = get_all_players()
if players.empty:
    st.warning("No data yet. Run `python -m fpl_analytics.pipeline` first.")
    st.stop()

LEADERBOARD_STATS = [
    ("total_points", "Total Points"),
    ("value_added", "Value Added"),
    ("form", "Form"),
    ("points_per_90", "Points per 90"),
]

DISPLAY_COLUMNS = ["web_name", "team_name", "price", "percentile"]

progress_column = st.column_config.ProgressColumn(
    "Percentile in position", min_value=0, max_value=100, format="%.0f"
)

for position in ["GK", "DEF", "MID", "FWD"]:
    st.header(position)
    position_players = players[players["position"] == position]
    if position_players.empty:
        st.caption("No players.")
        continue

    cols = st.columns(len(LEADERBOARD_STATS))
    for col, (stat_key, stat_label) in zip(cols, LEADERBOARD_STATS):
        with col:
            st.subheader(stat_label)
            ranked = position_players.copy()
            ranked["percentile"] = ranked[stat_key].rank(pct=True) * 100
            top = ranked.sort_values(stat_key, ascending=False).head(10)
            top = top.rename(columns={stat_key: stat_label})
            display_cols = ["web_name", "team_name", "price", stat_label, "percentile"]
            st.dataframe(
                top[display_cols],
                width="stretch",
                hide_index=True,
                column_config={"percentile": progress_column},
            )

st.header("Market Movers")
movers_cols = st.columns(4)

with movers_cols[0]:
    st.subheader("Biggest Price Risers")
    risers = players.sort_values("cost_change_start", ascending=False).head(10)
    st.dataframe(
        risers[["web_name", "team_name", "price", "cost_change_start"]],
        width="stretch",
        hide_index=True,
    )

with movers_cols[1]:
    st.subheader("Biggest Price Fallers")
    fallers = players.sort_values("cost_change_start", ascending=True).head(10)
    st.dataframe(
        fallers[["web_name", "team_name", "price", "cost_change_start"]],
        width="stretch",
        hide_index=True,
    )

with movers_cols[2]:
    st.subheader("Most Transferred In")
    transfers_in = players.sort_values("transfers_in_event", ascending=False).head(10)
    st.dataframe(
        transfers_in[["web_name", "team_name", "transfers_in_event"]],
        width="stretch",
        hide_index=True,
    )

with movers_cols[3]:
    st.subheader("Most Transferred Out")
    transfers_out = players.sort_values("transfers_out_event", ascending=False).head(10)
    st.dataframe(
        transfers_out[["web_name", "team_name", "transfers_out_event"]],
        width="stretch",
        hide_index=True,
    )
