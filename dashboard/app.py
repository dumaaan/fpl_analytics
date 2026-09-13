import streamlit as st

from dashboard.charts import scatter_chart
from dashboard.queries import get_all_players, get_last_refresh

st.set_page_config(page_title="FPL Analytics", layout="wide")

st.title("Fantasy Premier League Insights")

last_refresh = get_last_refresh()
if last_refresh is not None:
    st.caption(f"Data last updated: {last_refresh.strftime('%Y-%m-%d %H:%M UTC')}")
else:
    st.caption("Data not refreshed yet — run `python -m fpl_analytics.pipeline`.")

players = get_all_players()

if players.empty:
    st.warning("No data yet. Run `python -m fpl_analytics.pipeline` first.")
    st.stop()

st.sidebar.header("Filters")
position = st.sidebar.selectbox("Position", ["All", "GK", "DEF", "MID", "FWD"])
min_price = st.sidebar.slider(
    "Minimum Price (£m)",
    float(players["price"].min()),
    float(players["price"].max()),
    float(players["price"].min()),
    step=0.1,
)
min_points = st.sidebar.slider(
    "Minimum Total Points",
    int(players["total_points"].min()),
    int(players["total_points"].max()),
    int(players["total_points"].min()),
)
min_games = st.sidebar.slider(
    "Minimum Games Played",
    0.0,
    float((players["minutes"] / 90).max()),
    0.0,
    step=0.5,
)

filtered = players[
    ((players["position"] == position) if position != "All" else True)
    & (players["price"] >= min_price)
    & (players["total_points"] >= min_points)
    & (players["minutes"] >= min_games * 90)
]

st.download_button(
    "Export CSV",
    filtered.to_csv(index=False).encode("utf-8"),
    file_name="fpl-players.csv",
    mime="text/csv",
)

st.header("Value Analysis")
with st.expander("What does \"value\" mean here?", expanded=False):
    st.markdown(
        "- **Points per Million (PPM)** is just total points divided by price. "
        "It rewards any nailed-on starter, even one who only ever earns the automatic "
        "appearance points (2 for playing 60+ minutes, 1 for playing at all).\n"
        "- **Value Added per Million (VAPM)** removes those appearance points first: "
        "for every gameweek, it subtracts what the player would have scored just for "
        "being on the pitch, averages what's left, and divides by price. A player who "
        "only ever earns appearance points scores 0 VAPM, however cheap they are — "
        "the size of the bubble below is VAPM, not raw points."
    )
st.altair_chart(
    scatter_chart(
        filtered,
        x="price",
        y="total_points",
        x_title="Price (£m)",
        y_title="Total Points",
        title="Total Points vs Price (size: Value Added per Million)",
        size_field="vapm",
        tooltip=["web_name", "team_name", "price", "total_points", "vapm", "points_per_million"],
    ),
    width="stretch",
)
st.subheader("Top 10 by Value Added per Million (VAPM)")
st.dataframe(
    filtered.sort_values("vapm", ascending=False)
    .head(10)[["web_name", "team_name", "position", "price", "vapm", "points_per_million", "total_points"]],
    width="stretch",
    hide_index=True,
)

st.header("Performance vs Expectation")
st.altair_chart(
    scatter_chart(
        filtered,
        x="xgi_per_90",
        y="goal_involvement_per_90",
        x_title="xGI per 90",
        y_title="Goal Involvement per 90",
        title="Expected vs Actual Goal Involvement per 90",
        size_field="minutes",
        tooltip=["web_name", "team_name", "xgi_per_90", "goal_involvement_per_90", "goal_involvement_overperformance", "minutes"],
    ),
    width="stretch",
)

st.header("Form and Consistency")
st.altair_chart(
    scatter_chart(
        filtered,
        x="consistency_score",
        y="form",
        x_title="Consistency (points std. dev.)",
        y_title="Form",
        title="Form vs Consistency (size: Total Points)",
        size_field="total_points",
        tooltip=["web_name", "team_name", "consistency_score", "form", "total_points", "selected_by_percent"],
    ),
    width="stretch",
)

if position in ("GK", "All"):
    gk_data = filtered[filtered["position"] == "GK"]
    if not gk_data.empty:
        st.header("Goalkeeper Special")
        st.altair_chart(
            scatter_chart(
                gk_data,
                x="saves_per_90",
                y="clean_sheets_per_90",
                x_title="Saves per 90",
                y_title="Clean Sheets per 90",
                title="Saves per 90 vs Clean Sheets per 90 (size: Total Points)",
                color_field=None,
                size_field="total_points",
                tooltip=["web_name", "team_name", "saves_per_90", "clean_sheets_per_90", "goals_conceded_per_90", "total_points"],
            ),
            width="stretch",
        )

if position != "GK":
    defensive_data = filtered[filtered["position"] != "GK"].copy()
    if not defensive_data.empty:
        defensive_data["defcon_hit_rate_pct"] = defensive_data["defcon_hit_rate"] * 100
        st.header("Defensive Contribution")
        st.caption(
            "New for 2025/26: defenders earn 2 points for 10+ combined clearances, "
            "blocks, interceptions and tackles (CBIT) in a match; midfielders and "
            "forwards need 12+ including ball recoveries (CBIRT). Goalkeepers are not "
            "eligible. \"Hit Rate\" is how often a player crosses their threshold in "
            "games they actually played."
        )
        st.altair_chart(
            scatter_chart(
                defensive_data,
                x="defensive_contribution_per_90",
                y="defcon_hit_rate_pct",
                x_title="Defensive Contribution per 90",
                y_title="DefCon Hit Rate %",
                title="Defensive Contribution per 90 vs Hit Rate (size: Total Points)",
                size_field="total_points",
                tooltip=["web_name", "team_name", "defensive_contribution_per_90", "defcon_hit_rate_pct", "cbit", "cbirt", "total_points"],
            ),
            width="stretch",
        )

st.header("Differential Picks")
st.altair_chart(
    scatter_chart(
        filtered,
        x="selected_by_percent",
        y="total_points",
        x_title="Selected By %",
        y_title="Total Points",
        title="Total Points vs Selection % (size: Value Added per Million)",
        size_field="vapm",
        tooltip=["web_name", "team_name", "selected_by_percent", "total_points", "vapm", "points_per_million"],
    ),
    width="stretch",
)

st.header("Bonus Point Analysis")
st.altair_chart(
    scatter_chart(
        filtered,
        x="bps_per_90",
        y="bonus_points_per_90",
        x_title="BPS per 90",
        y_title="Bonus Points per 90",
        title="Bonus Points per 90 vs BPS per 90",
        size_field="total_points",
        tooltip=["web_name", "team_name", "bps_per_90", "bonus_points_per_90", "total_points"],
    ),
    width="stretch",
)

st.header("Overall Contribution")
st.altair_chart(
    scatter_chart(
        filtered,
        x="ict_index_per_90",
        y="total_contribution_per_90",
        x_title="ICT Index per 90",
        y_title="Total Contribution per 90",
        title="ICT Index per 90 vs Total Contribution per 90",
        size_field="total_points",
        tooltip=["web_name", "team_name", "ict_index_per_90", "total_contribution_per_90", "total_points"],
    ),
    width="stretch",
)

st.header("Risk Analysis")
risk_data = filtered[filtered["minutes_per_goal_involvement"].notna()]
st.altair_chart(
    scatter_chart(
        risk_data,
        x="card_rate",
        y="minutes_per_goal_involvement",
        x_title="Card Rate",
        y_title="Minutes per Goal Involvement",
        title="Card Rate vs Minutes per Goal Involvement",
        size_field="total_points",
        tooltip=["web_name", "team_name", "card_rate", "minutes_per_goal_involvement", "total_points"],
    ),
    width="stretch",
)
