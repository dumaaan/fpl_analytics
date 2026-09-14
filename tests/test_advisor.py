import numpy as np
import pandas as pd

from fpl_analytics.advisor import (
    KEEP,
    SELL,
    WATCH,
    best_lineup,
    candidate_pool,
    estimate_free_transfers,
    player_verdicts,
    purchase_costs,
    recommend,
    search_transfers,
    selling_cost,
    with_fallback_projection,
)

VALUE = "xpts_next_3gw"


def test_selling_cost_keeps_half_of_a_rise_rounded_down():
    assert selling_cost(now_cost=58, purchase_cost=55) == 56  # +0.3 -> +0.1
    assert selling_cost(now_cost=57, purchase_cost=55) == 56
    assert selling_cost(now_cost=53, purchase_cost=55) == 53  # falls cost in full


def test_purchase_costs_skip_free_hit_transfers():
    transfers = [
        {"element_in": 10, "element_in_cost": 60, "event": 2, "time": "2026-08-20"},
        {"element_in": 10, "element_in_cost": 70, "event": 3, "time": "2026-08-30"},  # Free Hit week
    ]
    chips = [{"name": "freehit", "event": 3}]
    paid = purchase_costs([10, 11], transfers, chips, start_costs={10: 55, 11: 45})
    assert paid == {10: 60, 11: 45}


def test_free_transfers_roll_over_and_chips_do_not_use_them():
    history = [
        {"event": 1, "event_transfers": 0},
        {"event": 2, "event_transfers": 0},  # rolled: 2 for GW3
        {"event": 3, "event_transfers": 6},  # Wildcard: still 2, +1 = 3
        {"event": 4, "event_transfers": 4},  # 1 hit week: max(0, 3-4)+1 = 1
    ]
    assert estimate_free_transfers(history, [{"name": "wildcard", "event": 3}]) == 1
    assert estimate_free_transfers(history[:3], [{"name": "wildcard", "event": 3}]) == 3
    many = [{"event": e, "event_transfers": 0} for e in range(1, 12)]
    assert estimate_free_transfers(many, []) == 5


def _squad_members(values: dict[str, list[float]]) -> list[tuple[int, str, float]]:
    members, pid = [], 1
    for pos, pos_values in values.items():
        for v in pos_values:
            members.append((pid, pos, v))
            pid += 1
    return members


def test_best_lineup_picks_a_legal_formation_and_captain():
    members = _squad_members({
        "GK": [4.0, 1.0],
        "DEF": [5.0, 4.0, 3.0, 1.0, 0.5],
        "MID": [9.0, 6.0, 5.0, 2.0, 1.0],
        "FWD": [7.0, 6.5, 0.0],
    })
    lineup = best_lineup(members)
    assert len(lineup.starters) == 11 and len(lineup.bench) == 4
    # A 5th midfielder (1.0) beats a 3rd forward (0.0), so 3-5-2 wins.
    assert lineup.formation == (3, 5, 2)
    assert lineup.captain == 8  # the 9.0 midfielder
    assert lineup.bench[0] == 2  # backup goalkeeper first on the bench
    starters_value = 4 + 5 + 4 + 3 + 9 + 6 + 5 + 2 + 1 + 7 + 6.5
    bench_value = 1.0 + 1.0 + 0.5 + 0.0
    assert np.isclose(lineup.score, starters_value + 9.0 + 0.1 * bench_value)


def _players() -> pd.DataFrame:
    rows = []
    pid = 1
    spec = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}
    for pos, n in spec.items():
        for k in range(n):
            rows.append(dict(id=pid, web_name=f"{pos}{k}", position=pos, team=pid, now_cost=50, availability=1.0,
                             status_label="Available", start_rate=1.0, price_change_tonight_pct=0.0,
                             **{VALUE: 6.0}))
            pid += 1
    squad = pd.DataFrame(rows)
    squad["sell_cost"] = 50
    squad.loc[squad["id"] == 13, VALUE] = 1.0  # a weak starting forward
    market = pd.DataFrame([
        dict(id=100, web_name="StarFWD", position="FWD", team=50, now_cost=55, availability=1.0, **{VALUE: 12.0}),
        dict(id=101, web_name="PriceyFWD", position="FWD", team=51, now_cost=90, availability=1.0, **{VALUE: 15.0}),
        dict(id=102, web_name="InjuredFWD", position="FWD", team=52, now_cost=50, availability=0.0, **{VALUE: 14.0}),
        dict(id=103, web_name="SameClubFWD", position="FWD", team=1, now_cost=50, availability=1.0, **{VALUE: 13.0}),
    ])
    return squad, market


def test_transfer_search_respects_budget_availability_and_club_limit():
    squad, market = _players()
    # Club 1 already has 3 squad players (ids 1, 2, 3).
    squad.loc[squad["id"].isin([2, 3]), "team"] = 1
    pool = candidate_pool(market, squad, VALUE)
    assert 102 not in pool["id"].tolist()  # injured players are not suggested
    singles, pairs = search_transfers(squad, pool, bank=5, value_col=VALUE, free_transfers=1)
    best = singles[0]
    assert best.outs == [13] and best.ins == [100]  # 101 is over budget, 103 breaks the 3-per-club rule
    assert best.bank_after == 0
    base = best_lineup(list(zip(squad["id"], squad["position"], squad[VALUE]))).score
    assert np.isclose(best.gain, best.lineup_score - base) and best.gain > 0
    assert all(103 not in p.ins for p in singles)


def test_hits_and_recommendation_threshold():
    squad, market = _players()
    pool = candidate_pool(market, squad, VALUE)
    singles, pairs = search_transfers(squad, pool, bank=5, value_col=VALUE, free_transfers=0)
    assert singles[0].hit == 4 and np.isclose(singles[0].net, singles[0].gain - 4)
    plan = recommend(singles, pairs, horizon=3)
    assert plan is not None and 100 in plan.ins
    # A pair is chosen only when it beats the best single by the threshold.
    if len(plan.ins) == 2:
        assert plan.net >= singles[0].net + 0.5 * 3
    assert recommend(singles, pairs, horizon=30) is None  # gain too small for a long horizon


def test_verdicts_flag_injured_and_replaceable_players():
    squad, market = _players()
    squad.loc[squad["id"] == 5, ["availability", "status_label"]] = [0.0, "Injured"]
    pool = candidate_pool(market, squad, VALUE)
    singles, _ = search_transfers(squad, pool, bank=5, value_col=VALUE, free_transfers=1)
    lineup = best_lineup(list(zip(squad["id"], squad["position"], squad[VALUE])))
    verdicts = player_verdicts(squad, singles, VALUE, horizon=3, lineup=lineup).set_index("id")
    assert verdicts.loc[5, "verdict"] == SELL
    assert verdicts.loc[13, "verdict"] == SELL
    assert verdicts.loc[1, "verdict"] in (KEEP, WATCH)


def test_missing_projection_uses_fpl_expected_points():
    squad = pd.DataFrame({"id": [1, 2], "ep_next": [3.0, None], VALUE: [np.nan, 7.0]})
    filled = with_fallback_projection(squad, VALUE, horizon=3)
    assert filled[VALUE].tolist() == [9.0, 7.0]
    assert filled[f"fallback_{VALUE}"].tolist() == [True, False]


def test_small_upgrade_is_watch_not_sell():
    squad, market = _players()
    squad.loc[squad["id"] == 13, VALUE] = 5.0  # slightly weaker forward
    market = market[market["id"] == 100].assign(**{VALUE: 8.0})
    pool = candidate_pool(market, squad, VALUE)
    singles, _ = search_transfers(squad, pool, bank=5, value_col=VALUE, free_transfers=1)
    lineup = best_lineup(list(zip(squad["id"], squad["position"], squad[VALUE])))
    verdicts = player_verdicts(squad, singles, VALUE, horizon=3, lineup=lineup).set_index("id")
    gain = max(p.gain for p in singles if p.outs == [13])
    assert 1.5 <= gain < 5.5
    assert verdicts.loc[13, "verdict"] == WATCH


def _market_with_names(market: pd.DataFrame) -> pd.DataFrame:
    return market.assign(team_short=market["team"].astype(str), status_label="Available", start_rate=1.0,
                         price_change_tonight_pct=0.0, ep_next=0.0)


def test_apply_transfers_updates_squad_bank_and_prices():
    from fpl_analytics.advisor import MadeTransfer, apply_transfers
    squad, market = _players()
    squad["team_short"] = squad["team"].astype(str)
    squad["pick_order"] = range(1, 16)
    players = pd.concat([squad, _market_with_names(market)], ignore_index=True)
    new, bank, problems = apply_transfers(squad, players, bank=5, transfers=[MadeTransfer(13, 100)])
    assert problems == []
    assert 13 not in new["id"].tolist() and 100 in new["id"].tolist()
    assert bank == 5 + 50 - 55
    bought = new.set_index("id").loc[100]
    assert bought["purchase_cost"] == 55 and bought["sell_cost"] == 55


def test_apply_transfers_reports_illegal_moves():
    from fpl_analytics.advisor import MadeTransfer, apply_transfers
    squad, market = _players()
    squad["team_short"] = squad["team"].astype(str)
    squad["pick_order"] = range(1, 16)
    players = pd.concat([squad, _market_with_names(market)], ignore_index=True)
    _, _, over_budget = apply_transfers(squad, players, bank=0, transfers=[MadeTransfer(13, 101)])
    assert any("more than the bank" in p for p in over_budget)
    _, _, wrong_position = apply_transfers(squad, players, bank=100, transfers=[MadeTransfer(1, 100)])
    assert any("must keep the position" in p for p in wrong_position)
    squad.loc[squad["id"].isin([2, 3]), "team"] = 1
    players = pd.concat([squad, _market_with_names(market)], ignore_index=True)
    _, _, club = apply_transfers(squad, players, bank=100, transfers=[MadeTransfer(13, 103)])
    assert any("The limit is 3" in p for p in club)


def test_squad_change_counts_the_hit():
    from fpl_analytics.advisor import MadeTransfer, apply_transfers, squad_change, transfer_hit
    squad, market = _players()
    squad["team_short"] = squad["team"].astype(str)
    squad["pick_order"] = range(1, 16)
    players = pd.concat([squad, _market_with_names(market)], ignore_index=True)
    new, _, _ = apply_transfers(squad, players, bank=5, transfers=[MadeTransfer(13, 100)])
    assert transfer_hit(1, free_transfers=1) == 0 and transfer_hit(2, free_transfers=1) == 4
    result = squad_change(squad, new, VALUE, hit=4)
    assert result["change"] > 0
    assert np.isclose(result["net"], result["change"] - 4)
