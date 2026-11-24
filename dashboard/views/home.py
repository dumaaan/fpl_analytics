import pandas as pd
import streamlit as st

from dashboard.catalog import METRICS, POSITION_NAMES
from dashboard.common import POSITIONS, apply_filters, format_utc, load_players_or_stop
from dashboard.queries import get_events
from fpl_analytics.config import MIN_MINUTES_FOR_PER_90

PLAYER_PAGE = "views/player.py"

players = load_players_or_stop()
events = get_events()
name_by_id = players.set_index("id")["web_name"]

next_event = events[events["is_next"].astype(bool)] if not events.empty else events
last_finished = events[events["finished"].astype(bool)].tail(1) if not events.empty else events
next_gw = int(next_event.iloc[0]["id"]) if not next_event.empty else None

st.title(f"Gameweek {next_gw}" if next_gw else "This gameweek")


def _countdown(deadline: pd.Timestamp) -> str:
    delta = deadline - pd.Timestamp.now(tz="UTC")
    if delta.total_seconds() <= 0:
        return "Deadline passed"
    days, hours = delta.days, delta.seconds // 3600
    return f"in {days} d {hours} h" if days else f"in {hours} h {(delta.seconds % 3600) // 60} min"


kpis = st.columns(4)
if not next_event.empty:
    deadline = pd.Timestamp(next_event.iloc[0]["deadline_time"])
    kpis[0].metric("Transfer deadline", format_utc(deadline, "%a %d %b"),
                   f"{format_utc(deadline, '%H:%M UTC')} · {_countdown(deadline)}",
                   delta_color="off", delta_arrow="off", border=True)
if not last_finished.empty:
    last = last_finished.iloc[0]
    gw = int(last["id"])
    kpis[1].metric(f"GW{gw} average score", int(last["average_entry_score"]), border=True)
    kpis[2].metric(f"GW{gw} highest score", int(last["highest_score"]), border=True)
    captain = name_by_id.get(last["most_captained"], "—")
    kpis[3].metric(f"GW{gw} most captained", captain, border=True)

# --- Best picks ------------------------------------------------------------
st.subheader(f"Best picks for GW{next_gw}" if next_gw else "Best picks")
st.caption(
    f"Top 5 by points per match for each position \u2014 total points divided by the matches their club "
    f"has played, so missed weeks count as zero. Players need {MIN_MINUTES_FOR_PER_90}+ minutes and a 75%+ "
    "chance to play. Sidebar filters apply. Select a row to open that player."
)
eligible = apply_filters(players, use_minutes=False)
eligible = eligible[(eligible["minutes"] >= MIN_MINUTES_FOR_PER_90) & (eligible["availability"] >= 0.75)]

pick_cols = [*st.columns(2, gap="large"), *st.columns(2, gap="large")]
for col, position in zip(pick_cols, POSITIONS):
    with col:
        st.markdown(f"**{POSITION_NAMES[position]}**")
        top = (eligible[eligible["position"] == position]
               .dropna(subset=["points_per_match"]).nlargest(5, "points_per_match").reset_index(drop=True))
        event = st.dataframe(
            top,
            column_order=["web_name", "team_short", "price", "points_per_match", "next_fixtures"],
            column_config={
                "web_name": st.column_config.TextColumn("Player"),
                "team_short": st.column_config.TextColumn("Team"),
                "price": st.column_config.NumberColumn("£m", format="%.1f"),
                "points_per_match": st.column_config.NumberColumn(
                    "Per match", format="%.2f", help=METRICS["points_per_match"].help),
                "next_fixtures": st.column_config.TextColumn(
                    "Next 5 GWs", help="HOME opponents in capitals, away in lower case. + marks a double, — a blank."
                ),
            },
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=f"picks_{position}",
        )
        if event.selection.rows:
            st.switch_page(PLAYER_PAGE, query_params={"player": str(int(top.iloc[event.selection.rows[0]]["id"]))})

# --- Price changes ---------------------------------------------------------
st.subheader("Price changes tonight")
st.caption(
    "FPL's own projection for tonight's price update. At +100% the price should rise by £0.1m; "
    "at −100% it should fall. Values change through the day as managers make transfers."
)
price_cols = {
    "web_name": st.column_config.TextColumn("Player"),
    "team_short": st.column_config.TextColumn("Team"),
    "position": st.column_config.TextColumn("Pos"),
    "price": st.column_config.NumberColumn("£m", format="%.1f"),
    "price_change_tonight_pct": st.column_config.NumberColumn("Progress", format="%.0f%%",
                                                              help=METRICS["price_change_tonight_pct"].help),
    "transfers_net_event": st.column_config.NumberColumn("Net transfers", format="%+d",
                                                         help="Transfers in minus transfers out this gameweek."),
}
market = apply_filters(players, use_minutes=False).dropna(subset=["price_change_tonight_pct"])
up, down = st.columns(2, gap="large")
with up:
    st.markdown("**Rising**")
    rising = market[market["price_change_tonight_pct"] > 0].nlargest(10, "price_change_tonight_pct")
    st.dataframe(rising, column_order=list(price_cols), column_config=price_cols, hide_index=True)
with down:
    st.markdown("**Falling**")
    falling = market[market["price_change_tonight_pct"] < 0].nsmallest(10, "price_change_tonight_pct")
    st.dataframe(falling, column_order=list(price_cols), column_config=price_cols, hide_index=True)

# --- Transfers -------------------------------------------------------------
st.subheader(f"Transfers for GW{next_gw}" if next_gw else "Transfers this gameweek")
transfer_cols = {
    "web_name": st.column_config.TextColumn("Player"),
    "team_short": st.column_config.TextColumn("Team"),
    "position": st.column_config.TextColumn("Pos"),
    "price": st.column_config.NumberColumn("£m", format="%.1f"),
    "price_change_start": st.column_config.NumberColumn("£ since start", format="%+.1f"),
    "selected_by_percent": st.column_config.NumberColumn("Owned", format="%.1f%%"),
}
t_in, t_out = st.columns(2, gap="large")
with t_in:
    st.markdown("**Most transferred in**")
    most_in = market.nlargest(10, "transfers_in_event")
    st.dataframe(
        most_in, column_order=[*transfer_cols, "transfers_in_event"], hide_index=True,
        column_config={**transfer_cols, "transfers_in_event": st.column_config.NumberColumn("In", format="%d")},
    )
with t_out:
    st.markdown("**Most transferred out**")
    most_out = market.nlargest(10, "transfers_out_event")
    st.dataframe(
        most_out, column_order=[*transfer_cols, "transfers_out_event"], hide_index=True,
        column_config={**transfer_cols, "transfers_out_event": st.column_config.NumberColumn("Out", format="%d")},
    )
