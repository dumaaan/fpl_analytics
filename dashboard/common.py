import pandas as pd
import streamlit as st

from dashboard.catalog import METRICS
from dashboard.queries import get_all_players, get_last_refresh, get_team_fixtures
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90
from fpl_analytics.refresh import is_stale, read_last_run

POSITIONS = ["GK", "DEF", "MID", "FWD"]
NO_DATA_MESSAGE = "No data yet. Run `poetry run fpl-pipeline`, then reload this page."

# Fixture ease, 1 = easiest ... 5 = hardest. Lightness steps as well as hue,
# so the scale survives colour-blindness and greyscale. The cell always prints
# the opponent too, so colour is never the only signal.
EASE_COLORS = {
    1: ("#2E7D6B", "#FFFFFF"),
    2: ("#8ABFA5", "#17181A"),
    3: ("#DDD6C3", "#17181A"),
    4: ("#D98E4A", "#17181A"),
    5: ("#9E4228", "#FFFFFF"),
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
    render_refresh_warnings(last_refresh)


def render_refresh_warnings(last_refresh) -> None:
    """Warn when the daily refresh failed or has not run, so stale data is never silent."""
    last_success = None
    if last_refresh is not None:
        last_success = pd.Timestamp(last_refresh)
        last_success = last_success.tz_localize("UTC") if last_success.tzinfo is None else last_success.tz_convert("UTC")
    last_run = read_last_run()
    if last_run and last_run.get("status") == "failed" and last_run.get("finished"):
        finished = pd.Timestamp(last_run["finished"])
        if last_success is None or finished > last_success:
            st.sidebar.error(
                f"The last data refresh failed ({format_utc(finished, '%d %b, %H:%M UTC')}): {last_run['error']} "
                "The pages show the previous data. See `poetry run fpl-schedule status`."
            )
            return
    if last_success is not None and is_stale(last_success.to_pydatetime()):
        hours = (pd.Timestamp.now(tz="UTC") - last_success).total_seconds() / 3600
        st.sidebar.warning(
            f"The data is {hours:.0f} hours old. The daily refresh may not have run: check "
            "`poetry run fpl-schedule status`, or refresh now with `poetry run fpl-pipeline`."
        )


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


def fixture_run(team_shorts: pd.Series, n_gws: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The next `n_gws` gameweeks as (text, ease) frames, one column per gameweek.

    Text reads "COV (H)". Home and away are spelled out rather than coded by
    letter case, which was easy to miss. A double gameweek joins with "+",
    a blank shows a dash.
    """
    fixtures = get_team_fixtures()
    empty = pd.DataFrame(index=team_shorts.index)
    if fixtures.empty:
        return empty, empty
    first = int(fixtures["window_start"].iloc[0])
    events = list(range(first, first + n_gws))
    window = fixtures[fixtures["event"].isin(events)]

    text, ease = {}, {}
    for event in events:
        rows = window[window["event"] == event]
        by_team = rows.groupby("team_short")
        label = by_team.apply(
            lambda g: " + ".join(f"{o} ({'H' if h else 'A'})"
                                 for o, h in zip(g["opponent_short"], g["is_home"])),
            include_groups=False)
        text[f"GW{event}"] = team_shorts.map(label).fillna("—")
        ease[f"GW{event}"] = team_shorts.map(by_team["defence_ease"].mean())
    return pd.DataFrame(text, index=team_shorts.index), pd.DataFrame(ease, index=team_shorts.index)
