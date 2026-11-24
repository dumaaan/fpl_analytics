"""The dashboard entry point.

`main()` runs the whole app, so it works from either entry file: this one
locally (`poetry run fpl-dashboard`), or `streamlit_app.py` at the repository
root on Streamlit Community Cloud. Streamlit re-runs the entry file on every
interaction, so all Streamlit calls live inside `main()`.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # so the packages import without being installed
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st  # noqa: E402

from dashboard.common import render_sidebar_filters  # noqa: E402
from dashboard.queries import get_all_players  # noqa: E402
from dashboard.views import positions  # noqa: E402
from fpl_analytics.datasource import ensure_database  # noqa: E402

VIEWS = Path(__file__).resolve().parent / "views"


def main() -> None:
    st.set_page_config(page_title="FPL Analytics", page_icon="⚽", layout="wide")
    # Hosted: fetch the database the scheduled refresh published. Local: no-op.
    ensure_database()

    navigation = st.navigation(
        {
            "This week": [
                st.Page(VIEWS / "home.py", title="Gameweek", icon=":material/today:", default=True),
                st.Page(VIEWS / "my_team.py", title="My team", icon=":material/groups:", url_path="my-team"),
            ],
            "Positions": [
                st.Page(positions.goalkeepers, title="Goalkeepers", icon=":material/sports_handball:",
                        url_path="goalkeepers"),
                st.Page(positions.defenders, title="Defenders", icon=":material/shield:", url_path="defenders"),
                st.Page(positions.midfielders, title="Midfielders", icon=":material/swap_calls:",
                        url_path="midfielders"),
                st.Page(positions.forwards, title="Forwards", icon=":material/sports_soccer:",
                        url_path="forwards"),
            ],
            "Tools": [
                st.Page(VIEWS / "player.py", title="Player detail", icon=":material/person_search:",
                        url_path="player"),
                st.Page(VIEWS / "fixtures.py", title="Fixture planner", icon=":material/calendar_month:",
                        url_path="fixtures"),
            ],
        }
    )
    render_sidebar_filters(get_all_players())
    navigation.run()


if __name__ == "__main__":  # Streamlit runs the entry file as "__main__"
    main()
