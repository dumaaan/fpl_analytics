from dataclasses import dataclass

from fpl_analytics.config import ENTRY_HISTORY_URL, ENTRY_PICKS_URL, ENTRY_TRANSFERS_URL, ENTRY_URL
from fpl_analytics.fetch.client import fetch_json


@dataclass
class TeamData:
    """A manager's public FPL data. Nothing here needs a login.

    The picks are the squad at the last deadline. Transfers made since then
    for the next gameweek are private until that deadline passes.
    """

    entry: dict
    picks: dict
    transfers: list[dict]
    history: dict


def fetch_team(team_id: int, event: int) -> TeamData:
    """Fetch one team's entry, its picks for `event`, its transfers and history.
    Personal data: not saved to data/raw."""
    return TeamData(
        entry=fetch_json(ENTRY_URL.format(team_id=team_id)),
        picks=fetch_json(ENTRY_PICKS_URL.format(team_id=team_id, event=event)),
        transfers=fetch_json(ENTRY_TRANSFERS_URL.format(team_id=team_id)),
        history=fetch_json(ENTRY_HISTORY_URL.format(team_id=team_id)),
    )
