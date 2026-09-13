from concurrent.futures import ThreadPoolExecutor, as_completed

from fpl_analytics.config import ELEMENT_SUMMARY_URL, PLAYER_HISTORY_MAX_WORKERS
from fpl_analytics.fetch.client import fetch_json, save_raw_json


def _fetch_one(player_id: int) -> tuple[int, list[dict]]:
    url = ELEMENT_SUMMARY_URL.format(player_id=player_id)
    data = fetch_json(url)
    return player_id, data.get("history", [])


def fetch_all_player_history(player_ids: list[int]) -> dict[int, list[dict]]:
    """Fetch this-season gameweek history for every player.

    One API call per player, run concurrently with a small worker pool to
    avoid hammering the FPL API. Returns {player_id: [gameweek_row, ...]}.
    """
    history_by_player: dict[int, list[dict]] = {}
    total = len(player_ids)
    completed = 0

    with ThreadPoolExecutor(max_workers=PLAYER_HISTORY_MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, pid): pid for pid in player_ids}
        for future in as_completed(futures):
            player_id = futures[future]
            try:
                pid, history = future.result()
                history_by_player[pid] = history
            except Exception as exc:  # noqa: BLE001 - log and continue
                print(f"  Warning: failed to fetch history for player {player_id}: {exc}")
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"  Fetched player history: {completed}/{total}")

    save_raw_json("player_history", {str(k): v for k, v in history_by_player.items()})
    return history_by_player
