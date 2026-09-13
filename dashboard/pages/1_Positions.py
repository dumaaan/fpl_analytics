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

COLUMN_CONFIG = {
    "web_name": st.column_config.TextColumn("Player", width="medium"),
    "team_name": st.column_config.TextColumn("Team", width="small"),
    "price": st.column_config.NumberColumn("Price", format="£%.1fm", width="small"),
    "percentile": st.column_config.ProgressColumn(
        "Percentile", min_value=0, max_value=100, format="%.0f", width="medium"
    ),
}


def leaderboard_table(position_players, stat_key: str, stat_label: str) -> None:
    st.subheader(stat_label)
    ranked = position_players.copy()
    ranked["percentile"] = ranked[stat_key].rank(pct=True) * 100
    top = ranked.sort_values(stat_key, ascending=False).head(10)
    top = top.rename(columns={stat_key: stat_label})
    column_config = {**COLUMN_CONFIG, stat_label: st.column_config.NumberColumn(stat_label, width="small")}
    st.dataframe(
        top[["web_name", "team_name", "price", stat_label, "percentile"]],
        width="stretch",
        hide_index=True,
        column_config=column_config,
    )


tabs = st.tabs(["GK", "DEF", "MID", "FWD"])
for tab, position in zip(tabs, ["GK", "DEF", "MID", "FWD"]):
    with tab:
        position_players = players[players["position"] == position]
        if position_players.empty:
            st.caption("No players.")
            continue

        row1 = st.columns(2)
        row2 = st.columns(2)
        for col, (stat_key, stat_label) in zip(row1 + row2, LEADERBOARD_STATS):
            with col:
                leaderboard_table(position_players, stat_key, stat_label)

st.header("Market Movers")
movers_row1 = st.columns(2)
movers_row2 = st.columns(2)

with movers_row1[0]:
    st.subheader("Biggest Price Risers")
    risers = players.sort_values("cost_change_start", ascending=False).head(10)
    st.dataframe(
        risers[["web_name", "team_name", "price", "cost_change_start"]],
        width="stretch",
        hide_index=True,
        column_config={
            **COLUMN_CONFIG,
            "cost_change_start": st.column_config.NumberColumn("Price Change", width="small"),
        },
    )

with movers_row1[1]:
    st.subheader("Biggest Price Fallers")
    fallers = players.sort_values("cost_change_start", ascending=True).head(10)
    st.dataframe(
        fallers[["web_name", "team_name", "price", "cost_change_start"]],
        width="stretch",
        hide_index=True,
        column_config={
            **COLUMN_CONFIG,
            "cost_change_start": st.column_config.NumberColumn("Price Change", width="small"),
        },
    )

with movers_row2[0]:
    st.subheader("Most Transferred In")
    transfers_in = players.sort_values("transfers_in_event", ascending=False).head(10)
    st.dataframe(
        transfers_in[["web_name", "team_name", "transfers_in_event"]],
        width="stretch",
        hide_index=True,
        column_config={
            **COLUMN_CONFIG,
            "transfers_in_event": st.column_config.NumberColumn("Transfers In", width="small"),
        },
    )

with movers_row2[1]:
    st.subheader("Most Transferred Out")
    transfers_out = players.sort_values("transfers_out_event", ascending=False).head(10)
    st.dataframe(
        transfers_out[["web_name", "team_name", "transfers_out_event"]],
        width="stretch",
        hide_index=True,
        column_config={
            **COLUMN_CONFIG,
            "transfers_out_event": st.column_config.NumberColumn("Transfers Out", width="small"),
        },
    )
