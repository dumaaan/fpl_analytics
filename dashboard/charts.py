import altair as alt
import pandas as pd

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90
from fpl_analytics.transform.scoring import POINT_SOURCES

# Validated categorical palette (dataviz skill references/palette.md).
# Fixed order — colors are assigned to entities, never cycled by rank.
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

POSITION_ORDER = ["GK", "DEF", "MID", "FWD"]
POSITION_COLORS = {"GK": "#2a78d6", "DEF": "#1baf7a", "MID": "#eda100", "FWD": "#008300"}


def _is_dark_theme() -> bool:
    try:
        import streamlit as st

        return st.context.theme.type == "dark"
    except Exception:  # noqa: BLE001 - outside a Streamlit run
        return False


def text_color() -> str:
    """Ink for text marks. Altair's default text is black, which vanishes on
    Streamlit's dark theme, so follow the viewer's current theme."""
    return "#ffffff" if _is_dark_theme() else "#0b0b0b"


def surface_color() -> str:
    """Streamlit's page background, for the thin gap between stacked segments."""
    return "#0e1117" if _is_dark_theme() else "#ffffff"


SERIES_1 = CATEGORICAL[0]
SERIES_2 = CATEGORICAL[1]
MUTED = "#898781"

POINT_SOURCE_LABELS = {
    "appearance": "Appearance",
    "clean_sheets": "Clean sheets",
    "defcon": "DefCon",
    "goals": "Goals",
    "assists": "Assists",
    "saves": "Saves",
    "bonus": "Bonus",
    "deductions": "Deductions",
}
POINT_SOURCE_SCALE = alt.Scale(
    domain=[POINT_SOURCE_LABELS[s] for s in POINT_SOURCES], range=CATEGORICAL[: len(POINT_SOURCES)]
)


SOURCE_LEGEND = alt.Legend(orient="right", title="Source")


def quadrant_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    color: str,
    rank_field: str = "xpts_per_90",
    n_labels: int = 8,
    x_format: str = ".2f",
    y_format: str = ".2f",
) -> alt.Chart:
    """Scatter with median guide lines and the top players labelled."""
    data = data.dropna(subset=[x, y])
    if data.empty:
        return alt.Chart(pd.DataFrame()).mark_point()

    tooltip = [
        alt.Tooltip("web_name:N", title="Player"),
        alt.Tooltip("team_short:N", title="Team"),
        alt.Tooltip("price:Q", title="£m", format=".1f"),
        alt.Tooltip(f"{x}:Q", title=x_title, format=x_format),
        alt.Tooltip(f"{y}:Q", title=y_title, format=y_format),
        alt.Tooltip("total_points:Q", title="Total points"),
    ]
    # Pad the domain (more on the right) so labels on edge points stay inside the plot.
    x_lo, x_hi = float(data[x].min()), float(data[x].max())
    y_lo, y_hi = float(data[y].min()), float(data[y].max())
    x_span, y_span = (x_hi - x_lo) or 1.0, (y_hi - y_lo) or 1.0
    x_enc = alt.X(f"{x}:Q", title=x_title, scale=alt.Scale(domain=[x_lo - 0.04 * x_span, x_hi + 0.18 * x_span]))
    y_enc = alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(domain=[y_lo - 0.05 * y_span, y_hi + 0.10 * y_span]))

    points = (
        alt.Chart(data)
        .mark_circle(opacity=0.75, color=color)
        .encode(
            x=x_enc,
            y=y_enc,
            size=alt.Size("total_points:Q", title="Total points", scale=alt.Scale(range=[25, 350])),
            tooltip=tooltip,
        )
    )
    medians = pd.DataFrame({"x": [data[x].median()], "y": [data[y].median()]})
    rule_x = alt.Chart(medians).mark_rule(strokeDash=[4, 4], color=MUTED).encode(x="x:Q")
    rule_y = alt.Chart(medians).mark_rule(strokeDash=[4, 4], color=MUTED).encode(y="y:Q")
    top = data.nlargest(n_labels, rank_field) if rank_field in data else data.head(0)
    labels = (
        alt.Chart(top)
        .mark_text(align="left", dx=8, dy=-7, fontSize=11, color=text_color())
        .encode(x=x_enc, y=y_enc, text="web_name:N")
    )
    return (points + rule_x + rule_y + labels).properties(height=400).interactive()


def points_source_chart(players: pd.DataFrame) -> alt.Chart:
    """Horizontal stacked bars: where each player's season points come from."""
    cols = [f"pts_{s}" for s in POINT_SOURCES]
    long_df = players.melt(id_vars=["web_name", "total_points"], value_vars=cols, var_name="source",
                           value_name="points")
    long_df["source_order"] = long_df["source"].map({c: i for i, c in enumerate(cols)})
    long_df["source"] = long_df["source"].str.removeprefix("pts_").map(POINT_SOURCE_LABELS)
    long_df = long_df[long_df["points"] != 0]
    order = players.sort_values("total_points", ascending=False)["web_name"].tolist()

    return (
        alt.Chart(long_df)
        .mark_bar(stroke=surface_color(), strokeWidth=1)
        .encode(
            y=alt.Y("web_name:N", sort=order, title=None, axis=alt.Axis(labelOverlap=False)),
            x=alt.X("points:Q", title="Season points"),
            color=alt.Color("source:N", scale=POINT_SOURCE_SCALE, title="Source",
                            legend=SOURCE_LEGEND),
            order=alt.Order("source_order:Q"),
            tooltip=[alt.Tooltip("web_name:N", title="Player"), alt.Tooltip("source:N", title="Source"),
                     alt.Tooltip("points:Q", title="Points")],
        )
        .properties(height=max(160, 26 * len(order)))
    )


def percentile_profile_chart(profile: pd.DataFrame) -> alt.Chart:
    """profile columns: metric, percentile, player. One or two players."""
    players = list(dict.fromkeys(profile["player"]))
    metric_order = list(dict.fromkeys(profile["metric"]))
    base = alt.Chart(profile).encode(
        y=alt.Y("metric:N", sort=metric_order, title=None, axis=alt.Axis(labelOverlap=False)),
        x=alt.X("percentile:Q", title=f"Percentile vs position ({MIN_MINUTES_FOR_PER_90}+ mins)", scale=alt.Scale(domain=[0, 100])),
    )
    bars = base.mark_bar(cornerRadiusEnd=3, height={"band": 0.8}).encode(
        color=alt.Color("player:N", scale=alt.Scale(domain=players, range=[SERIES_1, SERIES_2][: len(players)]),
                        title=None, legend=alt.Legend(orient="bottom") if len(players) > 1 else None),
        yOffset=alt.YOffset("player:N", sort=players),
        tooltip=[alt.Tooltip("player:N", title="Player"), alt.Tooltip("metric:N", title="Metric"),
                 alt.Tooltip("percentile:Q", title="Percentile", format=".0f"),
                 alt.Tooltip("value:N", title="Value")],
    )
    mid = alt.Chart(pd.DataFrame({"x": [50]})).mark_rule(strokeDash=[4, 4], color=MUTED).encode(x="x:Q")
    return (bars + mid).properties(height=max(220, (26 + 14 * len(players)) * len(metric_order)))


def gameweek_points_chart(history: pd.DataFrame) -> alt.Chart:
    """Points per gameweek, stacked by source. Games without minutes say DNP."""
    cols = [f"pts_{s}" for s in POINT_SOURCES]
    per_gw = history.groupby("round", as_index=False)[[*cols, "minutes", "total_points"]].sum()
    long_df = per_gw.melt(id_vars=["round"], value_vars=cols, var_name="source", value_name="points")
    long_df["source_order"] = long_df["source"].map({c: i for i, c in enumerate(cols)})
    long_df["source"] = long_df["source"].str.removeprefix("pts_").map(POINT_SOURCE_LABELS)
    long_df = long_df[long_df["points"] != 0]

    x = alt.X("round:O", title="Gameweek", sort=sorted(per_gw["round"].unique()))
    bars = (
        alt.Chart(long_df)
        .mark_bar(stroke=surface_color(), strokeWidth=1)
        .encode(
            x=x,
            y=alt.Y("points:Q", title="Points"),
            color=alt.Color("source:N", scale=POINT_SOURCE_SCALE, title="Source",
                            legend=SOURCE_LEGEND),
            order=alt.Order("source_order:Q"),
            tooltip=[alt.Tooltip("round:O", title="GW"), alt.Tooltip("source:N", title="Source"),
                     alt.Tooltip("points:Q", title="Points")],
        )
    )
    dnp = per_gw[per_gw["minutes"] == 0].assign(label="DNP", y=0.5)
    dnp_text = alt.Chart(dnp).mark_text(color=MUTED, fontSize=11, dy=-6).encode(x=x, y="y:Q", text="label:N")
    # Label each bar's total just above the positive part of its stack.
    per_gw["stack_top"] = per_gw[cols].clip(lower=0).sum(axis=1)
    totals = alt.Chart(per_gw[per_gw["minutes"] > 0]).mark_text(dy=-6, fontSize=11, color=text_color()).encode(
        x=x, y=alt.Y("stack_top:Q"), text="total_points:Q"
    )
    return (bars + dnp_text + totals).properties(height=300)


def minutes_chart(history: pd.DataFrame) -> alt.Chart:
    per_gw = history.groupby("round", as_index=False)["minutes"].sum()
    bars = alt.Chart(per_gw).mark_bar(color=SERIES_1, cornerRadiusEnd=3).encode(
        x=alt.X("round:O", title="Gameweek"),
        y=alt.Y("minutes:Q", title="Minutes", scale=alt.Scale(domain=[0, max(90, int(per_gw["minutes"].max()))])),
        tooltip=[alt.Tooltip("round:O", title="GW"), alt.Tooltip("minutes:Q", title="Minutes")],
    )
    sixty = alt.Chart(pd.DataFrame({"y": [60]})).mark_rule(strokeDash=[4, 4], color=MUTED).encode(y="y:Q")
    return (bars + sixty).properties(height=300)


def line_chart(data: pd.DataFrame, x: str, y: str, x_title: str, y_title: str, y_format: str = ",.0f") -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_line(point=True, color=SERIES_1, strokeWidth=2)
        .encode(
            x=alt.X(f"{x}:O", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=False), axis=alt.Axis(format=y_format)),
            tooltip=[alt.Tooltip(f"{x}:O", title=x_title), alt.Tooltip(f"{y}:Q", title=y_title, format=y_format)],
        )
        .properties(height=260)
    )


def multi_line_chart(
    data: pd.DataFrame, x: str, value_columns: dict[str, str], x_title: str, y_title: str,
    colors: list[str] | None = None,
) -> alt.Chart:
    """Several lines with the same unit on one axis (e.g. goals vs xG).
    value_columns maps column -> series label, in color order."""
    long_df = data.melt(id_vars=[x], value_vars=list(value_columns), var_name="series", value_name="value")
    long_df["series"] = long_df["series"].map(value_columns)
    labels = list(value_columns.values())
    return (
        alt.Chart(long_df)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X(f"{x}:O", title=x_title),
            y=alt.Y("value:Q", title=y_title, axis=alt.Axis(format="~f")),
            color=alt.Color("series:N", scale=alt.Scale(domain=labels, range=(colors or CATEGORICAL)[: len(labels)]),
                            title=None, legend=alt.Legend(orient="bottom")),
            strokeDash=alt.StrokeDash("series:N", legend=None),
            tooltip=[alt.Tooltip(f"{x}:O", title=x_title), alt.Tooltip("series:N", title="Series"),
                     alt.Tooltip("value:Q", title="Value", format=".2f")],
        )
        .properties(height=260)
    )
