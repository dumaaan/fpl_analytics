import pandas as pd
import streamlit as st

from dashboard.catalog import METRICS
from dashboard.queries import get_all_players, get_last_refresh
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90

POSITIONS = ["GK", "DEF", "MID", "FWD"]
NO_DATA_MESSAGE = "No data yet. Run `poetry run fpl-pipeline`, then reload this page."

# Fixture ease, 1 = easiest ... 5 = hardest: a diverging blue <-> red scale with
# a neutral middle. Each step carries its own text color, and the number or
# percentage is always printed in the cell too.
EASE_COLORS = {
    1: ("#256abf", "#ffffff"),
    2: ("#9ec5f4", "#0b0b0b"),
    3: ("#f0efec", "#0b0b0b"),
    4: ("#f4b3b2", "#0b0b0b"),
    5: ("#c53a3a", "#ffffff"),
}


def format_utc(timestamp: object, fmt: str) -> str:
    """DuckDB returns TIMESTAMPTZ values in the machine's local time zone.
    Convert to UTC before formatting so "UTC" labels are true."""
    ts = pd.Timestamp(timestamp)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.strftime(fmt)


def load_players_or_stop() -> pd.DataFrame:
    players = get_all_players()
    if players.empty:
        st.warning(NO_DATA_MESSAGE)
        st.stop()
    return players


def render_sidebar_filters(players: pd.DataFrame) -> None:
    """Global filters. Rendered by the app entry point on every page, so the
    values stay the same when the user moves between pages."""
    st.sidebar.header("Filters")
    if players.empty:
        st.sidebar.caption(NO_DATA_MESSAGE)
        return

    price_min, price_max = float(players["price"].min()), float(players["price"].max())
    st.sidebar.slider(
        "Price (£m)", price_min, price_max, (price_min, price_max), step=0.1, key="f_price",
    )
    max_minutes = int(players["minutes"].max())
    st.sidebar.slider(
        "Minimum minutes", 0, max(max_minutes, MIN_MINUTES_FOR_PER_90), min(MIN_MINUTES_FOR_PER_90, max_minutes),
        step=30, key="f_minutes",
        help=f"Per-90 rates need {MIN_MINUTES_FOR_PER_90}+ minutes. Players below that show empty rates.",
    )
    st.sidebar.multiselect("Teams", sorted(players["team_short"].dropna().unique()), key="f_teams",
                           placeholder="All teams")
    st.sidebar.toggle("Available players only", key="f_available",
                      help="Hide players who are injured, suspended, or less than 75% likely to play.")

    last_refresh = get_last_refresh()
    if last_refresh is not None:
        st.sidebar.caption(f"Data updated {format_utc(last_refresh, '%d %b %Y, %H:%M UTC')}")


def apply_filters(players: pd.DataFrame, use_minutes: bool = True) -> pd.DataFrame:
    state = st.session_state
    mask = pd.Series(True, index=players.index)
    if "f_price" in state:
        low, high = state["f_price"]
        mask &= players["price"].between(low - 1e-9, high + 1e-9)
    if use_minutes and "f_minutes" in state:
        mask &= players["minutes"] >= state["f_minutes"]
    if state.get("f_teams"):
        mask &= players["team_short"].isin(state["f_teams"])
    if state.get("f_available"):
        mask &= players["availability"] >= 0.75
    return players[mask]


def position_percentiles(position_players: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """0-100 percentile of each metric among players of the same position with
    enough minutes for per-90 rates. Players below the floor get no percentile."""
    pool = position_players[position_players["minutes"] >= MIN_MINUTES_FOR_PER_90]
    out = pd.DataFrame(index=position_players.index)
    for key in keys:
        ascending = METRICS[key].higher_is_better
        out[key] = (pool[key].rank(pct=True, ascending=ascending) * 100).reindex(position_players.index)
    return out


def metric_column_config(keys: list[str]) -> dict:
    config = {}
    for key in keys:
        metric = METRICS[key]
        if metric.text:
            config[key] = st.column_config.TextColumn(metric.label, help=metric.help)
        else:
            config[key] = st.column_config.NumberColumn(metric.label, help=metric.help, format=metric.fmt)
    return config


def for_display(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Copy with 0-1 rates scaled to 0-100 so printf formats can show %."""
    out = df.copy()
    for key in keys:
        if METRICS[key].percent:
            out[key] = out[key] * 100
    return out


BASE_COLUMN_CONFIG = {
    "web_name": st.column_config.TextColumn("Player", pinned=True),
    "team_short": st.column_config.TextColumn("Team"),
    "price": st.column_config.NumberColumn("£m", format="%.1f"),
    "status_label": st.column_config.TextColumn("Status"),
    "games_played": st.column_config.NumberColumn("GP", format="%d", help=METRICS["games_played"].help),
    "next_fixtures": st.column_config.TextColumn(
        "Next 5 GWs", help="HOME opponents in capitals, away in lower case. + marks a double, — a blank."
    ),
    "last_6": st.column_config.BarChartColumn("Last 6 GWs", help="Points in each of the last 6 gameweeks.",
                                              y_min=0, y_max=20),
}


def ease_cell_style(rating: float | None) -> str:
    if rating is None or pd.isna(rating):
        return ""
    bg, fg = EASE_COLORS[int(min(5, max(1, round(rating))))]
    return f"background-color: {bg}; color: {fg};"


def ease_legend() -> None:
    chips = "".join(
        f'<span style="background:{bg};color:{fg};padding:2px 10px;border-radius:3px;margin-right:4px;'
        f'font-size:0.8rem;">{n}</span>'
        for n, (bg, fg) in EASE_COLORS.items()
    )
    st.markdown(f"{chips} <span style='font-size:0.85rem;opacity:.75'>1 easiest → 5 hardest</span>",
                unsafe_allow_html=True)
