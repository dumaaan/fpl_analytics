"""A position page, built as three bands: decide, evidence, explain.

Each band has a deliberately different density. The first is one sentence and
one picture of the whole field. The second is the table, which is the page.
The third explains where the points came from.
"""

import pandas as pd
import streamlit as st

from dashboard.catalog import (
    FIXTURE_SIGNAL,
    METRICS,
    POSITION_ALL_COLUMNS,
    POSITION_NAMES,
    POSITION_QUADRANT,
    POSITION_TABLE_COLUMNS,
    QUADRANT_NOTES,
)
from dashboard.charts import (
    POSITION_COLORS,
    points_earned_chart,
    points_given_chart,
    points_per_match_strip,
    quadrant_chart,
)
from dashboard.common import (
    BASE_COLUMN_CONFIG,
    apply_filters,
    ease_cell_style,
    fixture_run,
    for_display,
    load_players_or_stop,
    metric_column_config,
)
from dashboard.queries import get_player_gameweeks, get_team_fixtures

PLAYER_PAGE = "views/player.py"
TEAM_NAMES = {"ARS": "Arsenal", "AVL": "Aston Villa", "BHA": "Brighton", "BOU": "Bournemouth",
              "BRE": "Brentford", "BUR": "Burnley", "CHE": "Chelsea", "CRY": "Crystal Palace",
              "EVE": "Everton", "FUL": "Fulham", "LEE": "Leeds", "LIV": "Liverpool",
              "MCI": "Manchester City", "MUN": "Manchester United", "NEW": "Newcastle",
              "NFO": "Nottingham Forest", "SUN": "Sunderland", "TOT": "Tottenham",
              "WHU": "West Ham", "WOL": "Wolves"}


def _last_six(player_ids: pd.Series) -> pd.Series:
    history = get_player_gameweeks()
    if history.empty:
        return pd.Series([[] for _ in player_ids], index=player_ids.index)
    per_gw = history.groupby(["player_id", "round"])["total_points"].sum().reset_index()
    last = per_gw.groupby("player_id")["total_points"].apply(lambda s: s.tail(6).tolist())
    return player_ids.map(last).apply(lambda v: v if isinstance(v, list) else [])


def _easiest_run(position: str, n_gws: int = 5) -> str | None:
    fixtures = get_team_fixtures()
    if fixtures.empty:
        return None
    value_col, _, _ = FIXTURE_SIGNAL[position]
    first = int(fixtures["window_start"].iloc[0])
    totals = (fixtures[fixtures["event"] < first + n_gws]
              .groupby("team_short")[value_col].sum().sort_values(ascending=False))
    if st.session_state.get("f_teams"):
        totals = totals[totals.index.isin(st.session_state["f_teams"])]
    return None if totals.empty else totals.index[0]


def _decide(position: str, filtered: pd.DataFrame) -> None:
    """One sentence that answers the page, then the whole field on one scale."""
    st.title(POSITION_NAMES[position])
    rated = filtered.dropna(subset=["points_per_match"])
    if rated.empty:
        st.info("No players match the filters. Lower the minimum minutes or widen the price range.")
        return

    best = rated.nlargest(1, "points_per_match").iloc[0]
    cheap = rated[rated["price"] <= rated["price"].quantile(0.4)].nlargest(1, "points_per_match")
    run = _easiest_run(position)
    noun = POSITION_NAMES[position].lower().rstrip("s")
    parts = [f"**{best['web_name']}** (£{best['price']:.1f}m) has returned "
             f"**{best['points_per_match']:.1f} points a match**, more than any other {noun}."]
    if not cheap.empty and cheap.iloc[0]["web_name"] != best["web_name"]:
        c = cheap.iloc[0]
        parts.append(f"The best cheaper option is **{c['web_name']}** at £{c['price']:.1f}m, "
                     f"on {c['points_per_match']:.1f} a match.")
    if run:
        parts.append(f"**{TEAM_NAMES.get(run, run)}** have the easiest next five fixtures.")
    st.markdown(" ".join(parts))

    st.altair_chart(points_per_match_strip(rated), width="stretch")
    st.caption(f"Every rated {POSITION_NAMES[position].lower().rstrip('s')} on one scale. "
               "Points per match counts weeks a player missed as zero, so it covers how well he "
               "plays and how often. The dashed line is the typical player.")


def _evidence(position: str, filtered: pd.DataFrame) -> None:
    show_all = st.toggle("Show every column", key=f"all_cols_{position}",
                         help="The seven columns here answer most questions. This adds every "
                              "metric the pipeline computes.")
    metric_keys = (POSITION_ALL_COLUMNS if show_all else POSITION_TABLE_COLUMNS)[position]

    table = filtered.sort_values(["points_per_match", "total_points"], ascending=False,
                                 na_position="last").reset_index(drop=True)
    table["last_6"] = _last_six(table["id"])
    runs, ease = fixture_run(table["team_short"])
    table = pd.concat([for_display(table, metric_keys), runs], axis=1)

    columns = ["web_name", "team_short", "price", *metric_keys, *runs.columns, "last_6", "status_label"]
    config = {
        **BASE_COLUMN_CONFIG,
        **metric_column_config(metric_keys),
        **{c: st.column_config.TextColumn(c, help="Opponent and venue. The colour is how hard the "
                                                  "fixture is for this position.") for c in runs.columns},
    }
    styled = table.style.apply(
        lambda frame: pd.DataFrame(
            {c: ease[c].map(ease_cell_style) if c in ease else "" for c in frame.columns},
            index=frame.index),
        axis=None)

    st.caption(f"{len(table)} players, best first. Select a row to open that player. "
               "Hover any column name for what it means.")
    event = st.dataframe(
        styled, column_order=columns, column_config=config, hide_index=True,
        on_select="rerun", selection_mode="single-row", key=f"table_{position}",
        height=min(640, 36 * (len(table) + 1) + 4),
    )
    if event.selection.rows:
        st.switch_page(PLAYER_PAGE, query_params={"player": str(int(table.iloc[event.selection.rows[0]]["id"]))})

    st.download_button("Download this table", filtered.to_csv(index=False).encode("utf-8"),
                       file_name=f"fpl-{position.lower()}.csv", mime="text/csv")


def _explain(position: str, filtered: pd.DataFrame) -> None:
    top = filtered.nlargest(10, "total_points")
    if top.empty:
        return
    st.subheader("Where the points came from")
    left, right = st.columns(2, gap="large")
    with left:
        st.caption("**Earned** — points won by playing well.")
        st.altair_chart(points_earned_chart(top), width="stretch")
    with right:
        st.caption("**Given** — points for turning up, and for the team keeping it tight.")
        st.altair_chart(points_given_chart(top), width="stretch")

    x_key, y_key = POSITION_QUADRANT[position]
    rated = filtered.dropna(subset=["points_per_match"])
    above = rated[rated["points_per_match"] >= rated["points_per_match"].median()]
    if above.empty:
        return
    chart_data = for_display(above, [k for k in (x_key, y_key) if k in METRICS])
    x_label = METRICS[x_key].label if x_key in METRICS else "Price (£m)"
    y_label = METRICS[y_key].label
    st.subheader(f"{y_label} against {x_label}")
    st.caption(QUADRANT_NOTES[position] + " Only players above the median are plotted, and the "
                                          "best eight are named.")
    st.altair_chart(
        quadrant_chart(chart_data, x_key, y_key, x_label, y_label, POSITION_COLORS[position],
                       rank_field="points_per_match",
                       x_format=".0f" if x_key in METRICS and METRICS[x_key].percent else
                       (".1f" if x_key == "price" else ".2f"),
                       y_format=".0f" if METRICS[y_key].percent else ".2f"),
        width="stretch")


def render(position: str) -> None:
    players = load_players_or_stop()
    filtered = apply_filters(players[players["position"] == position])

    _decide(position, filtered)
    if filtered.dropna(subset=["points_per_match"]).empty:
        return
    st.divider()
    _evidence(position, filtered)
    st.divider()
    _explain(position, filtered)


def goalkeepers() -> None:
    render("GK")


def defenders() -> None:
    render("DEF")


def midfielders() -> None:
    render("MID")


def forwards() -> None:
    render("FWD")
