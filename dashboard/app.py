import streamlit as st

st.set_page_config(page_title="FPL Analytics", page_icon="⚽", layout="wide")

from dashboard.common import render_sidebar_filters  # noqa: E402
from dashboard.queries import get_all_players  # noqa: E402
from dashboard.views import positions  # noqa: E402

navigation = st.navigation(
    {
        "This week": [
            st.Page("views/home.py", title="Gameweek", icon=":material/today:", default=True),
            st.Page("views/my_team.py", title="My team", icon=":material/groups:", url_path="my-team"),
        ],
        "Positions": [
            st.Page(positions.goalkeepers, title="Goalkeepers", icon=":material/sports_handball:",
                    url_path="goalkeepers"),
            st.Page(positions.defenders, title="Defenders", icon=":material/shield:", url_path="defenders"),
            st.Page(positions.midfielders, title="Midfielders", icon=":material/swap_calls:",
                    url_path="midfielders"),
            st.Page(positions.forwards, title="Forwards", icon=":material/sports_soccer:", url_path="forwards"),
        ],
        "Tools": [
            st.Page("views/player.py", title="Player detail", icon=":material/person_search:", url_path="player"),
            st.Page("views/fixtures.py", title="Fixture planner", icon=":material/calendar_month:",
                    url_path="fixtures"),
        ],
    }
)

render_sidebar_filters(get_all_players())
navigation.run()
