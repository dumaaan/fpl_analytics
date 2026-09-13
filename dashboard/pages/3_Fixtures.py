import html

import pandas as pd
import streamlit as st

from dashboard.charts import DIFFICULTY_COLORS, DIFFICULTY_TEXT_COLORS
from dashboard.queries import get_fixtures, get_teams

st.set_page_config(page_title="Fixtures - FPL Analytics", layout="wide")
st.title("Fixture Difficulty Ticker")
st.caption("Color and number both show difficulty (1 = easiest, 5 = hardest).")

teams = get_teams()
fixtures = get_fixtures()

if teams.empty or fixtures.empty:
    st.warning("No data yet. Run `python -m fpl_analytics.pipeline` first.")
    st.stop()

team_name_by_id = dict(zip(teams["id"], teams["short_name"]))

home_side = fixtures.rename(
    columns={"team_h": "team_id", "team_a": "opponent_id", "team_h_difficulty": "difficulty"}
).assign(is_home=True)[["team_id", "opponent_id", "difficulty", "is_home", "event", "finished"]]

away_side = fixtures.rename(
    columns={"team_a": "team_id", "team_h": "opponent_id", "team_a_difficulty": "difficulty"}
).assign(is_home=False)[["team_id", "opponent_id", "difficulty", "is_home", "event", "finished"]]

by_team = pd.concat([home_side, away_side], ignore_index=True).sort_values(["team_id", "event"])

NEXT_N = 5

rows_html = []
for team_id, team_short in sorted(team_name_by_id.items(), key=lambda x: x[1]):
    upcoming = by_team[(by_team["team_id"] == team_id) & (~by_team["finished"])].head(NEXT_N)
    cells = []
    for _, fx in upcoming.iterrows():
        opponent = html.escape(str(team_name_by_id.get(fx["opponent_id"], "?")))
        venue = "H" if fx["is_home"] else "A"
        difficulty = int(fx["difficulty"])
        bg = DIFFICULTY_COLORS.get(difficulty, "#898781")
        fg = DIFFICULTY_TEXT_COLORS.get(difficulty, "#0b0b0b")
        cells.append(
            f'<td style="background:{bg};color:{fg};padding:8px 10px;'
            f'text-align:center;border-radius:4px;font-size:13px;">'
            f"{opponent} ({venue})<br><b>{difficulty}</b></td>"
        )
    while len(cells) < NEXT_N:
        cells.append('<td style="padding:8px 10px;color:#898781;">—</td>')
    rows_html.append(
        f'<tr><td style="padding:8px 10px;font-weight:600;">{html.escape(team_short)}</td>'
        + "".join(cells)
        + "</tr>"
    )

header_cells = "".join(f'<th style="padding:6px 10px;">Next {i+1}</th>' for i in range(NEXT_N))
table_html = f"""
<table style="width:100%;border-collapse:separate;border-spacing:4px;">
  <thead><tr><th></th>{header_cells}</tr></thead>
  <tbody>{''.join(rows_html)}</tbody>
</table>
"""

st.markdown(table_html, unsafe_allow_html=True)
