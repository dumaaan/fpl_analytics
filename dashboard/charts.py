import altair as alt
import pandas as pd

# Validated categorical palette (dataviz skill references/palette.md).
# Fixed order — colors are assigned to positions, never cycled by rank.
POSITION_ORDER = ["GK", "DEF", "MID", "FWD"]
POSITION_COLORS = ["#2a78d6", "#1baf7a", "#eda100", "#008300"]

SINGLE_SERIES_COLOR = "#2a78d6"
SECOND_SERIES_COLOR = "#e34948"

# Status palette for fixture difficulty (good -> critical), fixed and never
# reused for categorical series.
DIFFICULTY_COLORS = {
    1: "#0ca30c",
    2: "#0ca30c",
    3: "#fab219",
    4: "#ec835a",
    5: "#d03b3b",
}

# Text color chosen per fill for legibility (never rely on background alone —
# the numeric rating is always shown as text too).
DIFFICULTY_TEXT_COLORS = {
    1: "#ffffff",
    2: "#ffffff",
    3: "#0b0b0b",
    4: "#0b0b0b",
    5: "#ffffff",
}


def position_color_scale() -> alt.Scale:
    return alt.Scale(domain=POSITION_ORDER, range=POSITION_COLORS)


def scatter_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    title: str,
    color_field: str | None = "position",
    size_field: str | None = None,
    tooltip: list[str] | None = None,
) -> alt.Chart:
    encodings = {
        "x": alt.X(f"{x}:Q", title=x_title),
        "y": alt.Y(f"{y}:Q", title=y_title),
    }
    if color_field:
        encodings["color"] = alt.Color(
            f"{color_field}:N", scale=position_color_scale(), title="Position"
        )
    else:
        encodings["color"] = alt.value(SINGLE_SERIES_COLOR)
    if size_field:
        encodings["size"] = alt.Size(f"{size_field}:Q", legend=None)
    if tooltip:
        encodings["tooltip"] = tooltip

    return (
        alt.Chart(data)
        .mark_circle(opacity=0.75)
        .encode(**encodings)
        .properties(title=title, height=360)
        .interactive()
    )


def line_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    title: str,
) -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_line(point=True, color=SINGLE_SERIES_COLOR)
        .encode(
            x=alt.X(f"{x}:O", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=[x, y],
        )
        .properties(title=title, height=280)
    )


def dual_line_chart(
    data: pd.DataFrame,
    x: str,
    y1: str,
    y1_label: str,
    y2: str,
    y2_label: str,
    x_title: str,
    y_title: str,
    title: str,
) -> alt.Chart:
    """Two lines, same unit/scale, one axis (e.g. goals scored vs expected goals)."""
    long_df = data.melt(
        id_vars=[x],
        value_vars=[y1, y2],
        var_name="series",
        value_name="value",
    )
    long_df["series"] = long_df["series"].map({y1: y1_label, y2: y2_label})

    return (
        alt.Chart(long_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:O", title=x_title),
            y=alt.Y("value:Q", title=y_title),
            color=alt.Color(
                "series:N",
                scale=alt.Scale(
                    domain=[y1_label, y2_label],
                    range=[SINGLE_SERIES_COLOR, SECOND_SERIES_COLOR],
                ),
                title=None,
            ),
            tooltip=["series", "value"],
        )
        .properties(title=title, height=280)
    )
