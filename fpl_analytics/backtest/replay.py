"""Decision replay: play a season week by week with a transfer policy and
score the real points, so policies (and advisor thresholds) can be compared.

Each week at the deadline the policy sees only past matches, may make
transfers under FPL's rules (budget from selling prices, 3 per club, free
transfers and 4-point hits), then picks its best lineup and captain.
Actual points use automatic substitutions and the vice-captain rule.
Chips are not played.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from fpl_analytics.advisor import (
    best_lineup,
    candidate_pool,
    recommend,
    search_transfers,
    selling_cost,
    with_fallback_projection,
)
from fpl_analytics.backtest.data import Season
from fpl_analytics.backtest.projectors import as_of, model_projector
from fpl_analytics.transform.projection import DEFAULT_SETTINGS, ModelSettings
from fpl_analytics.config import MAX_FREE_TRANSFERS, MAX_PLAYERS_PER_TEAM

SQUAD_SHAPE = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}
STARTING_BUDGET = 1000  # £100.0m in tenths
MIN_COUNT = {"DEF": 3, "MID": 2, "FWD": 1}
PLAN_HORIZON = 3


@dataclass
class Policy:
    name: str
    min_gain_per_gw: float | None  # None = never transfer


@dataclass
class TeamState:
    squad: list[int]
    purchase: dict[int, int]
    bank: int
    free_transfers: int = 1
    total: int = 0
    transfers: int = 0
    hits: int = 0
    weekly: list[dict] = field(default_factory=list)


DEFAULT_POLICIES = [
    Policy("hold (no transfers)", None),
    Policy("advisor, 0.0 xPts/GW", 0.0),
    Policy("advisor, 0.25 xPts/GW", 0.25),
    Policy("advisor, 0.5 xPts/GW (current)", 0.5),
    Policy("advisor, 1.0 xPts/GW", 1.0),
]


def price_table(season: Season) -> pd.DataFrame:
    """Price in tenths per player (rows) per gameweek (columns), carried forward."""
    prices = season.history.groupby(["player_id", "round"])["price"].last().unstack()
    prices = prices.reindex(columns=range(1, season.last_complete_gw + 1)).ffill(axis=1)
    return (prices * 10).round()


def template_squad(season: Season, gw: int, prices: pd.DataFrame, seed: int | None = None) -> list[int]:
    """A realistic start: the most-owned players before `gw` that fit the
    squad shape, the 3-per-club rule and the £100m budget. With a seed, each
    player's ownership is scaled by a random factor (0.5-1.5), which gives a
    different but still realistic squad for each seed."""
    before = season.history[season.history["round"] == gw - 1]
    owners = before.groupby("player_id").agg(selected=("selected", "max"), position=("position", "last"),
                                             team=("team_id", "last"))
    owners["cost"] = prices[gw].reindex(owners.index)
    owners = owners.dropna(subset=["cost"])
    if seed is not None:
        owners["selected"] = owners["selected"] * np.random.default_rng(seed).uniform(0.5, 1.5, len(owners))
    owners = owners.sort_values("selected", ascending=False)
    cheapest = owners.groupby("position")["cost"].min()

    squad, spent, filled, clubs = [], 0, {p: 0 for p in SQUAD_SHAPE}, {}
    for pid, row in owners.iterrows():
        pos = row["position"]
        if filled.get(pos, 99) >= SQUAD_SHAPE.get(pos, 0) or clubs.get(row["team"], 0) >= MAX_PLAYERS_PER_TEAM:
            continue
        # Keep enough money to fill every other open slot at the cheapest price.
        open_after = {p: SQUAD_SHAPE[p] - filled[p] - (p == pos) for p in SQUAD_SHAPE}
        reserve = sum(n * cheapest[p] for p, n in open_after.items())
        if spent + row["cost"] + reserve > STARTING_BUDGET:
            continue
        squad.append(int(pid))
        spent += row["cost"]
        filled[pos] += 1
        clubs[row["team"]] = clubs.get(row["team"], 0) + 1
        if len(squad) == sum(SQUAD_SHAPE.values()):
            break
    return squad


def score_week(lineup, positions: dict[int, str], points: dict[int, int], minutes: dict[int, int]) -> int:
    """Real points for a lineup: automatic substitutions in bench order (keeping
    a legal formation), captain doubled, or vice-captain if the captain did not play."""
    starters = list(lineup.starters)
    bench = list(lineup.bench)

    def legal(team: list[int]) -> bool:
        counts = {p: 0 for p in SQUAD_SHAPE}
        for pid in team:
            counts[positions[pid]] += 1
        return counts["GK"] == 1 and all(counts[p] >= n for p, n in MIN_COUNT.items())

    for i, pid in enumerate(list(starters)):
        if minutes.get(pid, 0) > 0:
            continue
        for sub in bench:
            if minutes.get(sub, 0) <= 0:
                continue
            if (positions[pid] == "GK") != (positions[sub] == "GK"):
                continue
            trial = starters.copy()
            trial[i] = sub
            if legal(trial):
                starters = trial
                bench.remove(sub)
                break

    total = sum(points.get(pid, 0) for pid in starters)
    if lineup.captain in starters and minutes.get(lineup.captain, 0) > 0:
        total += points.get(lineup.captain, 0)
    elif lineup.vice_captain in starters and minutes.get(lineup.vice_captain, 0) > 0:
        total += points.get(lineup.vice_captain, 0)
    return total


def _player_frame(ctx, projections: pd.DataFrame, prices: pd.DataFrame, gw: int) -> pd.DataFrame:
    frame = pd.DataFrame({
        "id": projections.index.astype(int),
        "position": ctx.positions.reindex(projections.index).to_numpy(),
        "team": ctx.teams.reindex(projections.index).to_numpy(),
        "xpts_next_gw": projections["h1"].to_numpy(),
        "xpts_next_3gw": projections["h3"].to_numpy(),
        "start_rate": projections["play_prob"].to_numpy(),
    })
    frame["now_cost"] = frame["id"].map(prices[gw])
    frame = frame.dropna(subset=["now_cost", "position", "team"])
    frame["now_cost"] = frame["now_cost"].astype(int)
    frame["availability"] = 1.0
    frame["status_label"] = "Available"
    frame["price_change_tonight_pct"] = float("nan")
    frame["ep_next"] = 0.0  # no FPL expected points in history: unprojected players count as 0
    return frame


def replay(season: Season, start_gw: int, end_gw: int, policies: list[Policy] = DEFAULT_POLICIES,
           starts: int = 5, progress: bool = True,
           settings: ModelSettings = DEFAULT_SETTINGS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run every policy from `starts` different starting squads (the first is
    the pure most-owned template). Returns (summary, weekly): the summary has
    the mean, lowest and highest season total over the starting squads."""
    prices = price_table(season)
    projections_by_gw = {}
    weekly_frames = []
    for start in range(starts):
        if progress:
            print(f"  starting squad {start + 1}/{starts}", flush=True)
        seed = None if start == 0 else start
        weekly = _replay_one(season, start_gw, end_gw, policies, prices, template_squad(season, start_gw, prices, seed),
                             projections_by_gw, settings)
        weekly_frames.append(weekly.assign(start=start + 1))
    weekly = pd.concat(weekly_frames, ignore_index=True)
    totals = weekly.groupby(["policy", "start"]).agg(points=("points", "sum"), transfers=("transfers", "sum"),
                                                     hit_points=("hit", "sum")).reset_index()
    n_gws = end_gw - start_gw + 1
    summary = totals.groupby("policy").agg(
        mean_points=("points", "mean"), lowest=("points", "min"), highest=("points", "max"),
        transfers=("transfers", "mean"), hit_points=("hit_points", "mean"),
    ).reset_index()
    hold = summary.loc[summary["policy"].str.startswith("hold"), "mean_points"]
    summary["points_per_gw"] = summary["mean_points"] / n_gws
    summary["vs_hold"] = summary["mean_points"] - (hold.iloc[0] if len(hold) else np.nan)
    wins = totals.pivot(index="start", columns="policy", values="points")
    summary["best_in_starts"] = summary["policy"].map((wins.idxmax(axis=1)).value_counts()).fillna(0).astype(int)
    return summary.sort_values("mean_points", ascending=False), weekly


def _replay_one(season: Season, start_gw: int, end_gw: int, policies: list[Policy], prices: pd.DataFrame,
                first_squad: list[int], projections_by_gw: dict, settings: ModelSettings) -> pd.DataFrame:
    first_cost = int(sum(prices.at[p, start_gw] for p in first_squad))
    states = {
        p.name: TeamState(squad=list(first_squad), purchase={pid: int(prices.at[pid, start_gw]) for pid in first_squad},
                          bank=STARTING_BUDGET - first_cost)
        for p in policies
    }

    for gw in range(start_gw, end_gw + 1):
        if gw not in projections_by_gw:
            ctx = as_of(season, gw)
            projections_by_gw[gw] = _player_frame(ctx, model_projector(ctx, settings), prices, gw)
        players = projections_by_gw[gw]
        week = season.history[season.history["round"] == gw]
        points = week.groupby("player_id")["total_points"].sum().to_dict()
        minutes = week.groupby("player_id")["minutes"].sum().to_dict()

        for policy in policies:
            state = states[policy.name]
            squad = players[players["id"].isin(state.squad)].copy()
            squad["purchase_cost"] = squad["id"].map(state.purchase)
            squad["sell_cost"] = [selling_cost(n, p) for n, p in zip(squad["now_cost"], squad["purchase_cost"])]
            squad = with_fallback_projection(squad, "xpts_next_gw", 1)
            squad = with_fallback_projection(squad, "xpts_next_3gw", PLAN_HORIZON)

            made, hit = 0, 0
            if policy.min_gain_per_gw is not None and len(squad) == len(state.squad):
                pool = candidate_pool(players, squad, "xpts_next_3gw")
                singles, pairs = search_transfers(squad, pool, state.bank, "xpts_next_3gw", state.free_transfers)
                plan = recommend(singles, pairs, PLAN_HORIZON, min_gain_per_gw=policy.min_gain_per_gw)
                if plan is not None:
                    for out_id, in_id in zip(plan.outs, plan.ins):
                        state.squad[state.squad.index(out_id)] = in_id
                        state.purchase.pop(out_id, None)
                        state.purchase[in_id] = int(players.loc[players["id"] == in_id, "now_cost"].iloc[0])
                    state.bank = plan.bank_after
                    made, hit = len(plan.outs), plan.hit
                    squad = players[players["id"].isin(state.squad)].copy()
                    squad = with_fallback_projection(squad, "xpts_next_gw", 1)

            positions = dict(zip(squad["id"], squad["position"]))
            lineup = best_lineup(list(zip(squad["id"], squad["position"], squad["xpts_next_gw"].astype(float))))
            week_points = score_week(lineup, positions, points, minutes) - hit
            state.total += week_points
            state.transfers += made
            state.hits += hit
            state.free_transfers = min(MAX_FREE_TRANSFERS, max(0, state.free_transfers - made) + 1)
            state.weekly.append({"policy": policy.name, "gw": gw, "points": week_points, "transfers": made,
                                 "hit": hit, "bank": state.bank})
    return pd.DataFrame([row for s in states.values() for row in s.weekly])
