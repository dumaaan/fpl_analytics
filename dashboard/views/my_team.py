import pandas as pd
import requests
import streamlit as st

from dashboard.catalog import METRICS
from dashboard.common import load_players_or_stop
from dashboard.queries import get_events
from fpl_analytics.advisor import (
    HORIZON_COLUMNS,
    best_lineup,
    build_squad,
    candidate_pool,
    estimate_free_transfers,
    player_verdicts,
    purchase_costs,
    recommend,
    search_transfers,
    with_fallback_projection,
)
from fpl_analytics.config import ENTRY_PICKS_URL, MAX_FREE_TRANSFERS, MIN_GAIN_PER_GW
from fpl_analytics.fetch.client import fetch_json
from fpl_analytics.fetch.entry import fetch_team

PLAYER_PAGE = "views/player.py"
POSITION_ORDER = {"GK": 0, "DEF": 1, "MID": 2, "FWD": 3}
HORIZON_LABELS = {1: "Next GW", 3: "Next 3 GWs", 5: "Next 5 GWs"}
HORIZON_PHRASES = {1: "next gameweek", 3: "next 3 gameweeks", 5: "next 5 gameweeks"}
# Status palette (good / warning / critical), always shown with the word too.
VERDICT_STYLES = {
    "Keep": "background-color: #0ca30c; color: #ffffff;",
    "Watch": "background-color: #fab219; color: #0b0b0b;",
    "Sell": "background-color: #d03b3b; color: #ffffff;",
}


def money(tenths: float) -> str:
    return f"£{tenths / 10:.1f}m"


@st.cache_data(ttl=600, show_spinner="Loading the team from FPL...")
def load_team(team_id: int, event: int) -> dict:
    data = fetch_team(team_id, event)
    result = {"entry": data.entry, "picks": data.picks, "transfers": data.transfers, "history": data.history,
              "free_hit_reverted": False}
    # After a Free Hit the squad goes back to the one before it.
    if data.picks.get("active_chip") == "freehit" and event > 1:
        result["picks"] = fetch_json(ENTRY_PICKS_URL.format(team_id=team_id, event=event - 1))
        result["picks"]["entry_history"]["bank"] = data.picks["entry_history"]["bank"]
        result["free_hit_reverted"] = True
    return result


st.title("My team")

players = load_players_or_stop()
events = get_events()
current = events[events["is_current"].astype(bool)] if not events.empty else events
if current.empty:
    st.info("The season has not started yet. Your team shows here after the Gameweek 1 deadline.")
    st.stop()
current_gw = int(current.iloc[0]["id"])
next_rows = events[events["is_next"].astype(bool)]
next_gw = int(next_rows.iloc[0]["id"]) if not next_rows.empty else None

# --- Team ID ---------------------------------------------------------------
query_team = st.query_params.get("team", "")
if "team_id_input" not in st.session_state:
    st.session_state["team_id_input"] = query_team
team_text = st.text_input(
    "FPL team ID",
    key="team_id_input",
    placeholder="For example 1234567",
    help="Open your Points page on the FPL website. The number after /entry/ in the address is your team ID.",
).strip()

if not team_text:
    st.info(
        "Enter your FPL team ID to rate your squad and find transfers. "
        "To find it, open your **Points** page on fantasy.premierleague.com. "
        "The address looks like `.../entry/1234567/event/4`: the ID is `1234567`. "
        "The page reads only public data. It never asks for your password."
    )
    st.stop()
if not team_text.isdigit():
    st.error("A team ID has digits only. Copy the number after /entry/ in your Points page address.")
    st.stop()
team_id = int(team_text)
if query_team != team_text:
    st.query_params["team"] = team_text

try:
    team = load_team(team_id, current_gw)
except requests.HTTPError as exc:
    if exc.response is not None and exc.response.status_code == 404:
        st.error(f"FPL has no team with ID {team_id}, or the team has no picks for GW{current_gw}. Check the number.")
    else:
        st.error(f"FPL returned an error ({exc}). Try again in a few minutes.")
    st.stop()
except requests.RequestException:
    st.error("Could not reach the FPL website. Check your internet connection and try again.")
    st.stop()

entry, picks, history = team["entry"], team["picks"], team["history"]
chips = history.get("chips", [])
bank = int(picks["entry_history"]["bank"])
squad_value = int(picks["entry_history"]["value"])

start_costs = dict(zip(players["id"], (players["now_cost"] - players["cost_change_start"]).astype(int)))
pick_ids = [p["element"] for p in picks["picks"]]
purchase = purchase_costs(pick_ids, team["transfers"], chips, start_costs)
squad = build_squad(picks["picks"], players, purchase)
if len(squad) < len(pick_ids):
    st.warning("Some of your players are not in the local data. Run `poetry run fpl-pipeline` and reload.")

# --- Header and controls ---------------------------------------------------
st.subheader(entry.get("name", f"Team {team_id}"))
caption = (f"Squad at the GW{current_gw} deadline. Transfers you made after that deadline show here "
           f"after the GW{next_gw} deadline." if next_gw else f"Squad at the GW{current_gw} deadline.")
if team["free_hit_reverted"]:
    caption += f" You played Free Hit in GW{current_gw}, so this is your GW{current_gw - 1} squad, which comes back."
st.caption(caption)

estimated_ft = estimate_free_transfers(history.get("current", []), chips)
header = st.columns(5)
header[0].metric("Overall rank", f"{entry.get('summary_overall_rank') or 0:,}", border=True)
header[1].metric(f"GW{current_gw} points", picks["entry_history"]["points"], border=True)
header[2].metric("Bank", money(bank), border=True)
header[3].metric("Squad value", money(squad_value), border=True,
                 help="FPL's value uses current prices. Your selling prices can be lower; see the squad table.")
with header[4]:
    free_transfers = st.number_input(
        "Free transfers", min_value=0, max_value=MAX_FREE_TRANSFERS, value=estimated_ft, step=1,
        key=f"free_transfers_{team_id}",
        help="Estimated from your transfer history and chips. Correct it if FPL shows a different number.",
    )

horizon = st.segmented_control("Plan for", list(HORIZON_LABELS), format_func=HORIZON_LABELS.get, default=3,
                               key="team_horizon") or 3
value_col = HORIZON_COLUMNS[horizon]
value_label = f"xPts {HORIZON_LABELS[horizon].replace('Next', 'next')}"

# --- Analysis --------------------------------------------------------------
squad = with_fallback_projection(squad, "xpts_next_gw", 1)
if value_col != "xpts_next_gw":
    squad = with_fallback_projection(squad, value_col, horizon)
next_lineup = best_lineup(list(zip(squad["id"], squad["position"], squad["xpts_next_gw"].astype(float))))
pool = candidate_pool(players, squad, value_col)
singles, pairs = search_transfers(squad, pool, bank, value_col, int(free_transfers))
plan = recommend(singles, pairs, horizon)
rated = player_verdicts(squad, singles, value_col, horizon, next_lineup)
names = players.set_index("id")
threshold = MIN_GAIN_PER_GW * horizon


def label(pid: int) -> str:
    row = names.loc[pid]
    return f"{row['web_name']} ({row['team_short']}, {row['position']})"


# --- Recommended move ------------------------------------------------------
st.subheader("Recommended move")
st.caption(
    f"Moves are scored by the change in your best lineup's projected points over the {HORIZON_PHRASES[horizon]}, "
    f"with the captain counted twice and the bench at 10%. Each extra transfer costs 4 points. "
    f"A move needs at least {threshold:.1f} xPts after hits to be recommended."
)


def plan_card(title: str, p) -> None:
    with st.container(border=True):
        st.markdown(f"**{title}**")
        for out_id, in_id in zip(p.outs, p.ins):
            out_row = squad[squad["id"] == out_id].iloc[0]
            in_row = names.loc[in_id]
            st.markdown(
                f"Sell **{out_row['web_name']}** ({out_row['team_short']}, {money(out_row['sell_cost'])}) "
                f"→ buy **{in_row['web_name']}** ({in_row['team_short']}, {money(in_row['now_cost'])})"
            )
        cols = st.columns(4)
        cols[0].metric("xPts gained", f"{p.gain:+.1f}")
        cols[1].metric("Hit", f"−{p.hit}" if p.hit else "0")
        cols[2].metric("Net gain", f"{p.net:+.1f}")
        cols[3].metric("Bank after", money(p.bank_after))


if plan is None:
    st.success(
        f"Save your transfer. No move in your budget adds {threshold:.1f}+ projected points "
        f"over the {HORIZON_PHRASES[horizon]} after hits."
    )
    if singles:
        plan_card("Best single move, for reference", singles[0])
else:
    plan_card(f"Make {len(plan.outs)} transfer{'s' if len(plan.outs) > 1 else ''}", plan)

# --- Squad verdicts ----------------------------------------------------------
st.subheader("Squad ratings")
st.caption(
    f"Sell: unlikely to play, or an upgrade in budget adds {threshold + 4:.1f}+ xPts, enough to be worth a −4 hit. "
    f"Watch: a doubt, few starts, under 270 minutes, a price fall tonight, or an upgrade of {threshold:.1f}+ xPts. "
    "Role is the best lineup for the next gameweek. Select a row to open the player page."
)
rated["pos_order"] = rated["position"].map(POSITION_ORDER)
rated["role_order"] = rated["role"].map({"Captain": 0, "Vice": 1, "Starter": 2, "Bench": 3})
table = rated.sort_values(["role_order", "pos_order", "xpts_next_gw"], ascending=[True, True, False]).reset_index(drop=True)
table["sell_price"] = table["sell_cost"] / 10
table["bought_price"] = table["purchase_cost"] / 10
table["now_price"] = table["now_cost"] / 10

columns = ["role", "position", "web_name", "team_short", "verdict", "reasons", "xpts_next_gw", value_col,
           "sell_price", "bought_price", "now_price", "points_last_4", "status_label", "next_fixtures"]
if value_col == "xpts_next_gw":
    columns.remove(value_col)
styled = table.style.apply(lambda col: [VERDICT_STYLES.get(v, "") for v in col], subset=["verdict"])
event = st.dataframe(
    styled,
    column_order=columns,
    column_config={
        "role": st.column_config.TextColumn("Role"),
        "position": st.column_config.TextColumn("Pos"),
        "web_name": st.column_config.TextColumn("Player", pinned=True),
        "team_short": st.column_config.TextColumn("Team"),
        "verdict": st.column_config.TextColumn("Verdict"),
        "reasons": st.column_config.TextColumn("Why", width="large"),
        "xpts_next_gw": st.column_config.NumberColumn("xPts next GW", format="%.1f", help=METRICS["xpts_next_gw"].help),
        value_col: st.column_config.NumberColumn(value_label, format="%.1f"),
        "sell_price": st.column_config.NumberColumn("Sell £m", format="%.1f",
                                                    help="What FPL pays if you sell: you keep half of any rise."),
        "bought_price": st.column_config.NumberColumn("Bought £m", format="%.1f"),
        "now_price": st.column_config.NumberColumn("Now £m", format="%.1f"),
        "points_last_4": st.column_config.NumberColumn("Pts last 4 GWs", format="%d"),
        "status_label": st.column_config.TextColumn("Status"),
        "next_fixtures": st.column_config.TextColumn(
            "Next 5 GWs", help="HOME opponents in capitals, away in lower case. + marks a double, — a blank."),
    },
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="squad_table",
    height=36 * (len(table) + 1) + 4,
)
if event.selection.rows:
    st.switch_page(PLAYER_PAGE, query_params={"player": str(int(table.iloc[event.selection.rows[0]]["id"]))})

# --- Replacement options -----------------------------------------------------
st.subheader("Replacement options")
flagged = rated.sort_values(["verdict_rank", "best_gain"], ascending=False)
out_options = flagged["id"].astype(int).tolist()
verdict_by_id = dict(zip(rated["id"], rated["verdict"]))
out_id = st.selectbox("Player to replace", out_options,
                      format_func=lambda pid: f"{label(pid)} · {verdict_by_id[pid]}", key=f"replace_{team_id}")
out_row = squad[squad["id"] == out_id].iloc[0]
options = [p for p in singles if p.outs[0] == out_id][:8]
st.caption(
    f"Budget for this move: {money(bank + out_row['sell_cost'])} (bank + selling price). "
    "Only players with a 75%+ chance to play and a projection are shown. The 3-per-club rule applies."
)
if not options:
    st.caption("No affordable replacement in this position.")
else:
    option_rows = []
    for p in options:
        row = names.loc[p.ins[0]]
        option_rows.append({
            "id": p.ins[0], "Player": row["web_name"], "Team": row["team_short"], "£m": row["now_cost"] / 10,
            value_label: row[value_col], "xPts gained": p.gain, "Net after hits": p.net,
            "Bank after £m": p.bank_after / 10, "Next 5 GWs": row["next_fixtures"], "Status": row["status_label"],
        })
    options_df = pd.DataFrame(option_rows)
    pick = st.dataframe(
        options_df,
        column_order=[c for c in options_df.columns if c != "id"],
        column_config={
            "£m": st.column_config.NumberColumn(format="%.1f"),
            value_label: st.column_config.NumberColumn(format="%.1f"),
            "xPts gained": st.column_config.NumberColumn(format="%+.1f",
                                                         help="Change in your best lineup's projected points."),
            "Net after hits": st.column_config.NumberColumn(format="%+.1f"),
            "Bank after £m": st.column_config.NumberColumn(format="%.1f"),
        },
        hide_index=True, on_select="rerun", selection_mode="single-row", key=f"options_{team_id}",
    )
    if pick.selection.rows:
        st.switch_page(PLAYER_PAGE, query_params={"player": str(int(options_df.iloc[pick.selection.rows[0]]["id"])),
                                                  "compare": str(int(out_id))})

if pairs:
    with st.expander("Best two-transfer moves"):
        pair_rows = [{
            "Sell": " + ".join(names.loc[i, "web_name"] for i in p.outs),
            "Buy": " + ".join(names.loc[i, "web_name"] for i in p.ins),
            "xPts gained": p.gain, "Hit": -p.hit, "Net": p.net, "Bank after £m": p.bank_after / 10,
        } for p in pairs[:8]]
        st.dataframe(
            pd.DataFrame(pair_rows), hide_index=True,
            column_config={
                "xPts gained": st.column_config.NumberColumn(format="%+.1f"),
                "Hit": st.column_config.NumberColumn(format="%d"),
                "Net": st.column_config.NumberColumn(format="%+.1f"),
                "Bank after £m": st.column_config.NumberColumn(format="%.1f"),
            },
        )

# --- Lineup ----------------------------------------------------------------
if next_gw:
    st.subheader(f"Suggested lineup for GW{next_gw}")
    d, m, f = next_lineup.formation
    st.caption(f"Formation {d}-{m}-{f}, picked by projected points for GW{next_gw} with your current squad.")
    next_xpts = dict(zip(squad["id"], squad["xpts_next_gw"]))  # includes FPL fallback values
    lineup_cols = st.columns([2, 1], gap="large")
    with lineup_cols[0]:
        for pos in ["GK", "DEF", "MID", "FWD"]:
            starters = [pid for pid in next_lineup.starters if names.loc[pid, "position"] == pos]
            parts = []
            for pid in starters:
                tag = " (C)" if pid == next_lineup.captain else " (V)" if pid == next_lineup.vice_captain else ""
                parts.append(f"{names.loc[pid, 'web_name']}{tag} {next_xpts[pid]:.1f}")
            st.markdown(f"**{pos}** · " + " · ".join(parts))
    with lineup_cols[1]:
        bench = [f"{i + 1}. {names.loc[pid, 'web_name']}" for i, pid in enumerate(next_lineup.bench)]
        st.markdown("**Bench order** · " + " · ".join(bench))
