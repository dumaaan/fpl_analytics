import altair as alt
import pandas as pd

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90
from fpl_analytics.transform.scoring import POINT_SOURCES

# Colour is a code, never decoration (Aicher's rule for Munich 1972). Three
# jobs only: ordered difficulty, points sources, and position. Everything else
# in the interface is grey.
EASE_SCALE = ["#2E7D6B", "#8ABFA5", "#DDD6C3", "#D98E4A", "#9E4228"]
EASE_SCALE_DARK = ["#2A7A68", "#6AA78D", "#B3AC98", "#C27C3C", "#903E24"]
# Text that stays legible on each step above.
EASE_TEXT = ["#FFFFFF", "#17181A", "#17181A", "#17181A", "#FFFFFF"]

# Points sources: goals are gold, deductions red, and what everyone is given is grey.
SOURCE_COLORS = {
    "appearance": "#C3C7C4", "clean_sheets": "#2E7D6B", "defcon": "#8ABFA5",
    "goals": "#C9A227", "assists": "#9C7C38", "saves": "#4E7A9E",
    "bonus": "#6E6A5E", "deductions": "#9E4228",
}
CATEGORICAL = list(SOURCE_COLORS.values())

POSITION_ORDER = ["GK", "DEF", "MID", "FWD"]
POSITION_COLORS = {"GK": "#4E7A9E", "DEF": "#2E7D6B", "MID": "#C9A227", "FWD": "#9C7C38"}


def _is_dark_theme() -> bool:
    try:
        import streamlit as st

        return st.context.theme.type == "dark"
    except Exception:  # noqa: BLE001 - outside a Streamlit run
        return False


def text_color() -> str:
    """Ink for text marks. Altair defaults to black, which vanishes on dark."""
    return "#F4F4F2" if _is_dark_theme() else "#17181A"


def surface_color() -> str:
    """The page ground, for the hairline gap between stacked segments."""
    return "#17181A" if _is_dark_theme() else "#F4F4F2"


def muted_color() -> str:
    return "#9A9DA3" if _is_dark_theme() else "#6B6E72"


def rule_color() -> str:
    return "#32353A" if _is_dark_theme() else "#D8D8D4"


def ease_colors() -> list[str]:
    return EASE_SCALE_DARK if _is_dark_theme() else EASE_SCALE


SERIES_1 = "#2E7D6B"
SERIES_2 = "#C9A227"
MUTED = "#8A8D92"

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
    domain=[POINT_SOURCE_LABELS[k] for k in POINT_SOURCES],
    range=[SOURCE_COLORS[k] for k in POINT_SOURCES],
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
        .mark_text(align="left", dx=8, dy=-8, fontSize=11, fontWeight=600,
                   color=text_color(), stroke=surface_color(), strokeWidth=3, strokeJoin="round")
        .encode(x=x_enc, y=y_enc, text="web_name:N")
    )
    return (points + rule_x + rule_y + labels).properties(height=400)


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


def points_per_match_strip(players: pd.DataFrame, highlight: int = 8,
                           value: str = "points_per_match") -> alt.Chart:
    """The whole field on one scale, so a number has somewhere to stand.

    A bar inside a table row can only compare a player with the axis. This
    shows where he sits among everyone, which is the question being asked.
    Dots are spread vertically only to stop them hiding each other; that axis
    carries no meaning. Nobody is named here, because the table directly below
    lists them in the same order.
    """
    data = players.dropna(subset=[value]).copy()
    if data.empty:
        return alt.Chart(pd.DataFrame({"x": []})).mark_point()

    ink, quiet = text_color(), muted_color()
    median = float(data[value].median())
    top = data.nlargest(highlight, value)
    # Deterministic spread, so the picture does not change between reruns.
    data["lane"] = [(i * 7) % 11 for i in range(len(data))]
    top = data.loc[top.index]

    x = alt.X(f"{value}:Q", title="Points per match",
              scale=alt.Scale(domain=[0, float(data[value].max()) * 1.05], nice=False),
              axis=alt.Axis(tickCount=8, grid=False, labelFontSize=11))
    y = alt.Y("lane:Q", title=None, axis=None, scale=alt.Scale(domain=[-1, 11]))
    tooltip = [alt.Tooltip("web_name:N", title="Player"), alt.Tooltip("team_short:N", title="Team"),
               alt.Tooltip("price:Q", title="Price", format=".1f"),
               alt.Tooltip(f"{value}:Q", title="Points per match", format=".2f"),
               alt.Tooltip("total_points:Q", title="Total points")]

    rest = alt.Chart(data).mark_circle(size=52, opacity=0.35, color=quiet).encode(x=x, y=y, tooltip=tooltip)
    best = alt.Chart(top).mark_circle(size=72, opacity=1, color=ink).encode(x=x, y=y, tooltip=tooltip)
    mid = alt.Chart(pd.DataFrame({"x": [median]})).mark_rule(color=quiet, strokeDash=[4, 4]).encode(x="x:Q")
    mid_text = alt.Chart(pd.DataFrame({"x": [median], "t": [f"typical {median:.1f}"]})).mark_text(
        align="left", dx=5, dy=-44, fontSize=10, color=quiet).encode(x="x:Q", text="t:N")

    return (rest + mid + mid_text + best).properties(height=110).configure_view(strokeWidth=0)


def _source_chart(players: pd.DataFrame, keys: list[str], title_order: list[str]) -> alt.Chart:
    cols = [f"pts_{k}" for k in keys]
    long = players.melt(id_vars=["web_name"], value_vars=cols, var_name="source", value_name="points")
    long["source"] = long["source"].str.removeprefix("pts_")
    long["order"] = long["source"].map({k: i for i, k in enumerate(keys)})
    long["source"] = long["source"].map(POINT_SOURCE_LABELS)
    long = long[long["points"] != 0]
    order = players.assign(_t=players[cols].sum(axis=1)).sort_values("_t", ascending=False)["web_name"].tolist()

    return (
        alt.Chart(long)
        .mark_bar(stroke=surface_color(), strokeWidth=1)
        .encode(
            y=alt.Y("web_name:N", sort=order, title=None, axis=alt.Axis(labelOverlap=False, labelFontSize=11)),
            x=alt.X("points:Q", title="Season points"),
            color=alt.Color("source:N", title=None,
                            scale=alt.Scale(domain=title_order,
                                            range=[SOURCE_COLORS[k] for k in keys]),
                            legend=alt.Legend(orient="bottom", direction="horizontal",
                                              symbolType="square", columns=3, labelLimit=120,
                                              labelFontSize=11, padding=4)),
            order=alt.Order("order:Q"),
            tooltip=[alt.Tooltip("web_name:N", title="Player"), alt.Tooltip("source:N", title="Source"),
                     alt.Tooltip("points:Q", title="Points")],
        )
        .properties(height=max(150, 30 * len(order)))
    )


EARNED_SOURCES = ["goals", "assists", "bonus"]
GIVEN_SOURCES = ["appearance", "clean_sheets", "defcon"]


def points_earned_chart(players: pd.DataFrame) -> alt.Chart:
    """Points won by playing well. Half of the old eight-segment stack."""
    return _source_chart(players, EARNED_SOURCES, [POINT_SOURCE_LABELS[k] for k in EARNED_SOURCES])


def points_given_chart(players: pd.DataFrame) -> alt.Chart:
    """Points for turning up, and for the team keeping it tight."""
    return _source_chart(players, GIVEN_SOURCES, [POINT_SOURCE_LABELS[k] for k in GIVEN_SOURCES])
