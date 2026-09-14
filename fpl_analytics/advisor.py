"""Team advisor: rate a squad and search for the best transfers.

All prices here are in FPL's tenths of £1m (55 = £5.5m), like the API, so
budget checks never meet float rounding.
"""

from dataclasses import dataclass, field
from itertools import combinations

import numpy as np
import pandas as pd

from fpl_analytics.config import (
    BENCH_WEIGHT,
    FORMATIONS,
    MAX_FREE_TRANSFERS,
    MAX_PLAYERS_PER_TEAM,
    MIN_GAIN_PER_GW,
    MIN_MINUTES_FOR_PER_90,
    TRANSFER_HIT_COST,
)

HORIZON_COLUMNS = {1: "xpts_next_gw", 3: "xpts_next_3gw", 5: "xpts_next_5gw"}
CHIPS_THAT_KEEP_FREE_TRANSFERS = {"wildcard", "freehit"}


# --- Prices and free transfers ----------------------------------------------

def selling_cost(now_cost: int, purchase_cost: int) -> int:
    """FPL sell price: a fall costs you in full; a rise gives you half,
    rounded down to the nearest £0.1m."""
    if now_cost <= purchase_cost:
        return now_cost
    return purchase_cost + (now_cost - purchase_cost) // 2


def purchase_costs(player_ids: list[int], transfers: list[dict], chips: list[dict],
                   start_costs: dict[int, int]) -> dict[int, int]:
    """Price paid for each player in the squad.

    The latest transfer in of the player gives the price. Free Hit transfers
    are skipped: the squad goes back after that gameweek. Players never
    transferred in were in the first squad and cost their start price.
    """
    free_hit_events = {c["event"] for c in chips if c.get("name") == "freehit"}
    paid: dict[int, int] = {}
    for transfer in sorted(transfers, key=lambda t: t.get("time", "")):
        if transfer.get("event") in free_hit_events:
            continue
        paid[transfer["element_in"]] = transfer["element_in_cost"]
    return {pid: paid.get(pid, start_costs.get(pid, 0)) for pid in player_ids}


def estimate_free_transfers(history_rows: list[dict], chips: list[dict]) -> int:
    """Free transfers for the next deadline, rebuilt from gameweek history.

    Gameweek-1 squads are free. After each later gameweek: unused transfers
    carry over, plus 1, up to MAX_FREE_TRANSFERS. Transfers made on a Wildcard
    or Free Hit do not use free transfers. FPL can give extra transfers in
    special cases, so the page lets the user correct this number.
    """
    chip_by_event = {c["event"]: c.get("name") for c in chips}
    rows = sorted(history_rows, key=lambda r: r["event"])
    free = 1
    for i, row in enumerate(rows):
        if i == 0:
            free = 1
            continue
        used = 0 if chip_by_event.get(row["event"]) in CHIPS_THAT_KEEP_FREE_TRANSFERS else row["event_transfers"]
        free = min(MAX_FREE_TRANSFERS, max(0, free - used) + 1)
    return free


# --- Squad and lineup --------------------------------------------------------

def build_squad(picks: list[dict], players: pd.DataFrame, purchase: dict[int, int]) -> pd.DataFrame:
    """One row per squad player with current and selling prices in tenths."""
    ids = [p["element"] for p in picks]
    squad = players[players["id"].isin(ids)].copy()
    order = {p["element"]: p["position"] for p in picks}
    squad["pick_order"] = squad["id"].map(order)
    squad["now_cost"] = squad["now_cost"].astype(int)
    squad["purchase_cost"] = squad["id"].map(purchase).astype(int)
    squad["sell_cost"] = [selling_cost(n, p) for n, p in zip(squad["now_cost"], squad["purchase_cost"])]
    captain = next((p["element"] for p in picks if p.get("is_captain")), None)
    squad["was_captain"] = squad["id"] == captain
    return squad.sort_values("pick_order").reset_index(drop=True)


def with_fallback_projection(squad: pd.DataFrame, value_col: str, horizon: int) -> pd.DataFrame:
    """Fill missing projections for squad players under the per-90 minutes floor
    with FPL's own expected points (ep_next) times the horizon. Without this,
    a new signing counts as 0 and every replacement looks like a big upgrade.
    `fallback_<value_col>` marks the filled rows."""
    out = squad.copy()
    missing = out[value_col].isna()
    out[f"fallback_{value_col}"] = missing
    out.loc[missing, value_col] = out.loc[missing, "ep_next"].fillna(0.0) * horizon
    return out


@dataclass
class Lineup:
    starters: list[int]
    bench: list[int]
    captain: int | None
    vice_captain: int | None
    formation: tuple[int, int, int]
    score: float


def best_lineup(members: list[tuple[int, str, float]]) -> Lineup:
    """Highest-scoring legal XI for (id, position, projected points) tuples.

    Score = starters + captain again (double points) + BENCH_WEIGHT x bench.
    Missing projections count as 0.
    """
    by_pos: dict[str, list[tuple[float, int]]] = {"GK": [], "DEF": [], "MID": [], "FWD": []}
    for pid, pos, value in members:
        by_pos.setdefault(pos, []).append((0.0 if value is None or np.isnan(value) else float(value), pid))
    for group in by_pos.values():
        group.sort(key=lambda item: -item[0])

    best: Lineup | None = None
    for formation in FORMATIONS:
        need = {"GK": 1, "DEF": formation[0], "MID": formation[1], "FWD": formation[2]}
        if any(len(by_pos[pos]) < n for pos, n in need.items()):
            continue
        starters = [item for pos, n in need.items() for item in by_pos[pos][:n]]
        bench_items = [item for pos, n in need.items() for item in by_pos[pos][n:]]
        starters.sort(key=lambda item: -item[0])
        captain_value = starters[0][0] if starters else 0.0
        score = sum(v for v, _ in starters) + captain_value + BENCH_WEIGHT * sum(v for v, _ in bench_items)
        if best is None or score > best.score + 1e-9:
            gk_bench = [item for item in by_pos["GK"][1:]]
            outfield_bench = sorted((i for i in bench_items if i not in gk_bench), key=lambda item: -item[0])
            best = Lineup(
                starters=[pid for _, pid in starters],
                bench=[pid for _, pid in gk_bench + outfield_bench],
                captain=starters[0][1] if starters else None,
                vice_captain=starters[1][1] if len(starters) > 1 else None,
                formation=formation,
                score=score,
            )
    if best is None:
        return Lineup([], [pid for pid, _, _ in members], None, None, (0, 0, 0), 0.0)
    return best


def _members(squad: pd.DataFrame, value_col: str) -> list[tuple[int, str, float]]:
    return list(zip(squad["id"].astype(int), squad["position"], squad[value_col].astype(float)))


# --- Transfer search ---------------------------------------------------------

@dataclass
class TransferPlan:
    outs: list[int]
    ins: list[int]
    gain: float  # projected points gained by the best lineup, before hits
    hit: int  # points paid for transfers above the free ones
    bank_after: int
    lineup_score: float
    net: float = field(init=False)

    def __post_init__(self) -> None:
        # Float noise from summing lineups would print as "-0.0".
        if abs(self.gain) < 1e-9:
            self.gain = 0.0
        self.net = self.gain - self.hit


def _team_counts_ok(team_ids: list[int]) -> bool:
    counts: dict[int, int] = {}
    for team in team_ids:
        counts[team] = counts.get(team, 0) + 1
        if counts[team] > MAX_PLAYERS_PER_TEAM:
            return False
    return True


def candidate_pool(players: pd.DataFrame, squad: pd.DataFrame, value_col: str,
                   min_availability: float = 0.75) -> pd.DataFrame:
    """Players you could buy: not owned, likely to play, with a projection."""
    pool = players[
        ~players["id"].isin(squad["id"])
        & (players["availability"] >= min_availability)
        & players[value_col].notna()
    ].copy()
    pool["now_cost"] = pool["now_cost"].astype(int)
    return pool


def search_transfers(
    squad: pd.DataFrame,
    pool: pd.DataFrame,
    bank: int,
    value_col: str,
    free_transfers: int,
    candidates_per_out: int = 15,
    pairs_from_top: int = 40,
) -> tuple[list[TransferPlan], list[TransferPlan]]:
    """Best single transfers and best pairs of transfers.

    Every plan is scored by re-picking the best lineup, so a cheap bench
    player swapped for another bench player gains almost nothing, and the
    budget (bank + selling prices) and 3-per-club rules always hold.
    Pairs are searched among the strongest single moves to stay fast.
    """
    base_members = _members(squad, value_col)
    base_score = best_lineup(base_members).score
    squad_rows = squad.to_dict("records")
    pool_by_pos = {pos: group.sort_values(value_col, ascending=False) for pos, group in pool.groupby("position")}
    squad_teams = [int(r["team"]) for r in squad_rows]

    def hit_for(n: int) -> int:
        return max(0, n - free_transfers) * TRANSFER_HIT_COST

    singles: list[TransferPlan] = []
    moves: list[tuple[int, dict, float]] = []  # (squad index, incoming row, gain)
    for i, out in enumerate(squad_rows):
        budget = bank + out["sell_cost"]
        options = pool_by_pos.get(out["position"])
        if options is None:
            continue
        options = options[options["now_cost"] <= budget]
        taken = 0
        for incoming in options.to_dict("records"):
            teams = squad_teams.copy()
            teams[i] = int(incoming["team"])
            if not _team_counts_ok(teams):
                continue
            members = base_members.copy()
            members[i] = (int(incoming["id"]), incoming["position"], float(incoming[value_col]))
            lineup = best_lineup(members)
            gain = lineup.score - base_score
            singles.append(TransferPlan([out["id"]], [incoming["id"]], gain, hit_for(1),
                                        budget - incoming["now_cost"], lineup.score))
            moves.append((i, incoming, gain))
            taken += 1
            if taken >= candidates_per_out:
                break
    singles.sort(key=lambda p: -p.net)

    pairs: list[TransferPlan] = []
    top_moves = sorted(moves, key=lambda m: -m[2])[:pairs_from_top]
    for (i, in_a, _), (j, in_b, _) in combinations(top_moves, 2):
        if i == j or in_a["id"] == in_b["id"]:
            continue
        bank_after = bank + squad_rows[i]["sell_cost"] + squad_rows[j]["sell_cost"] - in_a["now_cost"] - in_b["now_cost"]
        if bank_after < 0:
            continue
        teams = squad_teams.copy()
        teams[i], teams[j] = int(in_a["team"]), int(in_b["team"])
        if not _team_counts_ok(teams):
            continue
        members = base_members.copy()
        members[i] = (int(in_a["id"]), in_a["position"], float(in_a[value_col]))
        members[j] = (int(in_b["id"]), in_b["position"], float(in_b[value_col]))
        lineup = best_lineup(members)
        pairs.append(TransferPlan([squad_rows[i]["id"], squad_rows[j]["id"]], [in_a["id"], in_b["id"]],
                                  lineup.score - base_score, hit_for(2), bank_after, lineup.score))
    pairs.sort(key=lambda p: -p.net)
    return singles, pairs


def recommend(singles: list[TransferPlan], pairs: list[TransferPlan], horizon: int,
              min_gain_per_gw: float = MIN_GAIN_PER_GW) -> TransferPlan | None:
    """The plan to make now, or None to save the transfer. A plan must gain
    `min_gain_per_gw` per gameweek of the horizon after hits."""
    threshold = min_gain_per_gw * horizon
    best_single = singles[0] if singles and singles[0].net >= threshold else None
    best_pair = pairs[0] if pairs and pairs[0].net >= threshold else None
    # A second transfer must earn its own threshold on top of the single move,
    # because a saved transfer has value next week.
    if best_pair and (best_single is None or best_pair.net >= best_single.net + threshold):
        return best_pair
    return best_single


# --- Verdicts ----------------------------------------------------------------

SELL, WATCH, KEEP = "Sell", "Watch", "Keep"


def player_verdicts(squad: pd.DataFrame, singles: list[TransferPlan], value_col: str, horizon: int,
                    lineup: Lineup) -> pd.DataFrame:
    """Keep / Watch / Sell for each squad player, with plain reasons.

    Sell: unlikely to play, or the best replacement in budget gains enough to
    be worth a points hit (threshold + TRANSFER_HIT_COST). Watch: a doubt, few
    starts, no own projection, a price fall tonight, or an upgrade worth a
    free transfer (threshold). So Sell stays rare enough to act on.
    """
    threshold = MIN_GAIN_PER_GW * horizon
    sell_threshold = threshold + TRANSFER_HIT_COST
    fallback_col = f"fallback_{value_col}"
    best_gain: dict[int, float] = {}
    for plan in singles:
        out = plan.outs[0]
        best_gain[out] = max(best_gain.get(out, -np.inf), plan.gain)

    rows = []
    for player in squad.to_dict("records"):
        reasons: list[str] = []
        level = 0  # 0 keep, 1 watch, 2 sell

        availability = player["availability"]
        if availability <= 0.25:
            reasons.append(f"{player['status_label']}: unlikely to play")
            level = 2
        elif availability < 0.75:
            reasons.append(f"{player['status_label']}")
            level = max(level, 1)

        if player.get(fallback_col, False) or pd.isna(player[value_col]):
            reasons.append(f"Under {MIN_MINUTES_FOR_PER_90} minutes: rated with FPL's own expected points")
            level = max(level, 1)
        elif pd.notna(player["start_rate"]) and player["start_rate"] < 0.5:
            reasons.append(f"Starts only {player['start_rate']:.0%} of matches")
            level = max(level, 1)

        gain = best_gain.get(player["id"])
        if gain is not None and gain >= sell_threshold:
            reasons.append(f"An upgrade in budget adds {gain:.1f} xPts, worth even a hit")
            level = 2
        elif gain is not None and gain >= threshold:
            reasons.append(f"An upgrade in budget adds {gain:.1f} xPts")
            level = max(level, 1)

        if pd.notna(player["price_change_tonight_pct"]) and player["price_change_tonight_pct"] <= -80:
            reasons.append("Price likely to fall tonight")
            level = max(level, 1)

        if not reasons:
            reasons.append("No better option in budget")
        rows.append({
            "id": player["id"],
            "verdict": [KEEP, WATCH, SELL][level],
            "verdict_rank": level,
            "reasons": " · ".join(reasons),
            "best_gain": gain,
            "role": ("Captain" if player["id"] == lineup.captain
                     else "Vice" if player["id"] == lineup.vice_captain
                     else "Starter" if player["id"] in lineup.starters else "Bench"),
        })
    return squad.merge(pd.DataFrame(rows), on="id", how="left")


# --- Transfers already made ----------------------------------------------------

@dataclass(frozen=True)
class MadeTransfer:
    out_id: int
    in_id: int


def apply_transfers(squad: pd.DataFrame, players: pd.DataFrame, bank: int,
                    transfers: list[MadeTransfer]) -> tuple[pd.DataFrame, int, list[str]]:
    """Apply transfers the manager already made, in order.

    Incoming players cost their current price, which is also their selling
    price. Returns (new squad, bank after, problems). A problem means FPL
    would not allow the transfers, so the caller should not use the result.
    """
    by_id = players.set_index("id")
    new = squad.copy()
    problems: list[str] = []
    for transfer in transfers:
        if transfer.out_id not in set(new["id"]) or transfer.in_id not in by_id.index:
            problems.append("A transfer uses a player who is not in the squad or not in the data.")
            continue
        out_row = new[new["id"] == transfer.out_id].iloc[0]
        incoming = by_id.loc[transfer.in_id]
        if transfer.in_id in set(new["id"]):
            problems.append(f"{incoming['web_name']} is already in the squad.")
            continue
        if incoming["position"] != out_row["position"]:
            problems.append(f"{out_row['web_name']} is a {out_row['position']} and {incoming['web_name']} is a "
                            f"{incoming['position']}. A transfer must keep the position.")
            continue
        row = incoming.to_dict() | {
            "id": transfer.in_id,
            "now_cost": int(incoming["now_cost"]),
            "purchase_cost": int(incoming["now_cost"]),
            "sell_cost": int(incoming["now_cost"]),
            "pick_order": out_row.get("pick_order"),
            "was_captain": False,
        }
        bank += int(out_row["sell_cost"]) - int(incoming["now_cost"])
        new = pd.concat([new[new["id"] != transfer.out_id], pd.DataFrame([row])], ignore_index=True)

    if bank < 0:
        problems.append(f"These transfers cost £{-bank / 10:.1f}m more than the bank and selling prices allow. "
                        "Prices may have changed since the last data refresh.")
    counts = new["team"].value_counts()
    for team, count in counts[counts > MAX_PLAYERS_PER_TEAM].items():
        short = new.loc[new["team"] == team, "team_short"].iloc[0] if "team_short" in new else team
        problems.append(f"The squad would have {count} players from {short}. The limit is {MAX_PLAYERS_PER_TEAM}.")
    return new.sort_values("pick_order").reset_index(drop=True), bank, problems


def transfer_hit(n_transfers: int, free_transfers: int) -> int:
    return max(0, n_transfers - free_transfers) * TRANSFER_HIT_COST


def squad_change(before: pd.DataFrame, after: pd.DataFrame, value_col: str, hit: int) -> dict:
    """Best-lineup projected points before and after, and the net change after the hit."""
    score_before = best_lineup(_members(before, value_col)).score
    score_after = best_lineup(_members(after, value_col)).score
    change = score_after - score_before
    return {"before": score_before, "after": score_after, "change": change, "hit": hit, "net": change - hit}
