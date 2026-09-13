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
st.altair_chart(
    scatter_chart(
        filtered,
        x="price",
        y="total_points",
        x_title="Price (£m)",
        y_title="Total Points",
        title="Total Points vs Price (size: Value Added)",
        size_field="value_added",
        tooltip=["web_name", "team_name", "price", "total_points", "value_added", "points_per_million"],
    ),
    width="stretch",
)
st.subheader("Top 10 by Value Added")
st.dataframe(
    filtered.sort_values("value_added", ascending=False)
    .head(10)[["web_name", "team_name", "position", "price", "value_added", "points_per_million", "total_points"]],
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
                y="clean_sheet_percentage",
                x_title="Saves per 90",
                y_title="Clean Sheet %",
                title="Saves per 90 vs Clean Sheet % (size: Total Points)",
                color_field=None,
                size_field="total_points",
                tooltip=["web_name", "team_name", "saves_per_90", "clean_sheet_percentage", "goals_conceded_per_90", "total_points"],
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
        title="Total Points vs Selection % (size: Value Added)",
        size_field="value_added",
        tooltip=["web_name", "team_name", "selected_by_percent", "total_points", "value_added", "points_per_million"],
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
