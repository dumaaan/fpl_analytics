import pandas as pd
import streamlit as st

from dashboard.catalog import (
    FIXTURE_SIGNAL,
    METRICS,
    POSITION_INTROS,
    POSITION_NAMES,
    POSITION_QUADRANT,
    POSITION_TABLE_COLUMNS,
    QUADRANT_NOTES,
)
from dashboard.charts import POSITION_COLORS, points_source_chart, quadrant_chart
from dashboard.common import (
    BASE_COLUMN_CONFIG,
    apply_filters,
    for_display,
    load_players_or_stop,
    metric_column_config,
    position_percentiles,
)
from dashboard.queries import get_player_gameweeks, get_team_fixtures
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90

PLAYER_PAGE = "views/player.py"


def _last_six_points(player_ids: pd.Series) -> pd.Series:
    history = get_player_gameweeks()
    if history.empty:
        return pd.Series([[] for _ in player_ids], index=player_ids.index)
    per_gw = history.groupby(["player_id", "round"])["total_points"].sum().reset_index()
    last = per_gw.groupby("player_id")["total_points"].apply(lambda s: s.tail(6).tolist())
    return player_ids.map(last).apply(lambda v: v if isinstance(v, list) else [])


def _easiest_run(position: str, n_gws: int = 5) -> tuple[str, float] | None:
    fixtures = get_team_fixtures()
    if fixtures.empty:
        return None
    value_col, _, _ = FIXTURE_SIGNAL[position]
    first = int(fixtures["window_start"].iloc[0])
    window = fixtures[fixtures["event"] < first + n_gws]
    totals = window.groupby("team_short")[value_col].sum().sort_values(ascending=False)
    if st.session_state.get("f_teams"):
        totals = totals[totals.index.isin(st.session_state["f_teams"])]
    if totals.empty:
        return None
    return totals.index[0], float(totals.iloc[0])


def _kpi_row(position: str, players: pd.DataFrame) -> None:
    cols = st.columns(4)
    rateable = players[players["minutes"] >= MIN_MINUTES_FOR_PER_90]

    with cols[0]:
        best = rateable.dropna(subset=["vapm"]).nlargest(1, "vapm")
        if best.empty:
            st.metric("Best value (VAPM)", "—", border=True)
        else:
            row = best.iloc[0]
            st.metric("Best value (VAPM)", row["web_name"], f"{row['vapm']:.2f} VAPM · £{row['price']:.1f}m",
                      delta_color="off", delta_arrow="off", border=True, help=METRICS["vapm"].help)
    with cols[1]:
        best = rateable.dropna(subset=["xpts_next_gw"]).nlargest(1, "xpts_next_gw")
        if best.empty:
            st.metric("Top xPts next GW", "—", border=True)
        else:
            row = best.iloc[0]
            st.metric("Top xPts next GW", row["web_name"], f"{row['xpts_next_gw']:.1f} xPts · {row['team_short']}",
                      delta_color="off", delta_arrow="off", border=True, help=METRICS["xpts_next_gw"].help)
    with cols[2]:
        run = _easiest_run(position)
        _, _, unit = FIXTURE_SIGNAL[position]
        if run is None:
            st.metric("Easiest 5-GW run", "—", border=True)
        else:
            st.metric("Easiest 5-GW run", run[0], f"{run[1]:.1f} {unit}", delta_color="off", delta_arrow="off",
                      border=True, help="Best team fixtures over the next 5 gameweeks for this position.")
    with cols[3]:
        movers = players.dropna(subset=["price_change_tonight_pct"]).nlargest(1, "price_change_tonight_pct")
        if movers.empty or movers.iloc[0]["price_change_tonight_pct"] <= 0:
            st.metric("Closest to a price rise", "—", border=True)
        else:
            row = movers.iloc[0]
            st.metric("Closest to a price rise", row["web_name"],
                      f"{row['price_change_tonight_pct']:.0f}% tonight · £{row['price']:.1f}m",
                      delta_color="off", delta_arrow="off", border=True,
                      help=METRICS["price_change_tonight_pct"].help)


def _player_table(position: str, position_players: pd.DataFrame, filtered: pd.DataFrame) -> None:
    metric_keys = POSITION_TABLE_COLUMNS[position]
    table = filtered.copy()
    table["xpts_pct"] = position_percentiles(position_players, ["xpts_per_90"])["xpts_per_90"].reindex(table.index)
    table["last_6"] = _last_six_points(table["id"])
    table = table.sort_values(["xpts_next_gw", "total_points"], ascending=False, na_position="last")
    table = for_display(table, metric_keys).reset_index(drop=True)

    columns = ["web_name", "team_short", "price", "status_label", "games_played", *metric_keys,
               "last_6", "next_fixtures", "xpts_pct"]
    column_config = {
        **BASE_COLUMN_CONFIG,
        **metric_column_config(metric_keys),
        "xpts_pct": st.column_config.ProgressColumn(
            "xPts/90 pct", min_value=0, max_value=100, format="%.0f",
            help=f"Percentile of xPts/90 among players in this position with {MIN_MINUTES_FOR_PER_90}+ minutes.",
        ),
    }

    st.caption(f"{len(table)} players. Select a row to open the player page. Hover a column name for its meaning.")
    event = st.dataframe(
        table,
        column_order=columns,
        column_config=column_config,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=f"table_{position}",
        height=min(600, 36 * (len(table) + 1) + 4),
    )
    if event.selection.rows:
        player_id = int(table.iloc[event.selection.rows[0]]["id"])
        st.switch_page(PLAYER_PAGE, query_params={"player": str(player_id)})

    st.download_button(
        "Download table as CSV",
        filtered.to_csv(index=False).encode("utf-8"),
        file_name=f"fpl-{position.lower()}.csv",
        mime="text/csv",
    )


def _charts(position: str, filtered: pd.DataFrame) -> None:
    x_key, y_key = POSITION_QUADRANT[position]
    chart_data = for_display(filtered, [k for k in (x_key, y_key) if k in METRICS])
    x_label = METRICS[x_key].label if x_key in METRICS else "Price (£m)"
    y_label = METRICS[y_key].label
    x_fmt = ".0f" if x_key in METRICS and METRICS[x_key].percent else (".1f" if x_key == "price" else ".2f")
    y_fmt = ".0f" if METRICS[y_key].percent else ".2f"

    left, right = st.columns([1.25, 1], gap="large")
    with left:
        st.subheader(f"{y_label} vs {x_label}")
        st.caption(QUADRANT_NOTES[position] + " Dashed lines are medians. Labels mark the top 8 by xPts/90.")
        st.altair_chart(
            quadrant_chart(chart_data, x_key, y_key, x_label, y_label, POSITION_COLORS[position],
                           x_format=x_fmt, y_format=y_fmt),
            width="stretch",
        )
    with right:
        st.subheader("Where the points come from")
        top = filtered.nlargest(15, "total_points")
        st.caption("Top 15 by total points. Deductions go left of 0.")
        if top.empty:
            st.caption("No players match the filters.")
        else:
            st.altair_chart(points_source_chart(top), width="stretch")


def render(position: str) -> None:
    players = load_players_or_stop()
    position_players = players[players["position"] == position]
    filtered = apply_filters(position_players)

    st.title(POSITION_NAMES[position])
    st.caption(POSITION_INTROS[position])

    _kpi_row(position, filtered)
    if filtered.empty:
        st.info("No players match the filters. Lower the minimum minutes or widen the price range in the sidebar.")
        return
    _player_table(position, position_players, filtered)
    _charts(position, filtered)


def goalkeepers() -> None:
    render("GK")


def defenders() -> None:
    render("DEF")


def midfielders() -> None:
    render("MID")


def forwards() -> None:
    render("FWD")
