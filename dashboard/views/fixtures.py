import pandas as pd
import streamlit as st

from dashboard.common import NO_DATA_MESSAGE, ease_cell_style, ease_legend
from dashboard.queries import get_team_fixtures

MODES = {
    "Clean-sheet chance": dict(value="cs_prob", rating="defence_ease", total="Exp. clean sheets",
                               ascending=False, for_positions="GK and DEF"),
    "Goal threat": dict(value="xg_for", rating="attack_ease", total="Exp. team goals",
                        ascending=False, for_positions="MID and FWD"),
    "FPL difficulty": dict(value="fpl_difficulty", rating="fpl_difficulty", total="Avg difficulty",
                           ascending=True, for_positions="a quick check"),
}

st.title("Fixture planner")

fixtures = get_team_fixtures()
if fixtures.empty:
    st.warning(NO_DATA_MESSAGE)
    st.stop()

controls = st.columns([1, 2])
with controls[0]:
    horizon = st.segmented_control("Gameweeks", [3, 5, 8], default=5, key="fx_horizon") or 5
with controls[1]:
    mode_name = st.segmented_control("Rate fixtures by", list(MODES), default="Clean-sheet chance",
                                     key="fx_mode") or "Clean-sheet chance"
mode = MODES[mode_name]

st.caption(
    f"Use this view for {mode['for_positions']}. Teams are sorted by the best run first. "
    "Each cell names the opponent and whether the match is home (H) or away (A). A dash is a "
    "blank gameweek. "
    "Clean-sheet chance and goal threat come from each team's xG for and against this season, "
    "blended with FPL fixture difficulty while samples are small."
)
ease_legend()

first_event = int(fixtures["window_start"].iloc[0])
events = list(range(first_event, first_event + horizon))
window = fixtures[fixtures["event"].isin(events)]


def _cell_text(rows: pd.DataFrame) -> str:
    parts = []
    for _, fx in rows.iterrows():
        opponent = f"{fx['opponent_short']} ({'H' if fx['is_home'] else 'A'})"
        if mode["value"] == "cs_prob":
            parts.append(f"{opponent} {fx['cs_prob']:.0%}")
        elif mode["value"] == "xg_for":
            parts.append(f"{opponent} {fx['xg_for']:.1f}")
        else:
            parts.append(f"{opponent} {int(fx['fpl_difficulty'])}")
    return " + ".join(parts)


text_rows, rating_rows, totals = {}, {}, {}
for team, team_rows in window.groupby("team_short"):
    texts, ratings = {}, {}
    for event in events:
        rows = team_rows[team_rows["event"] == event]
        column = f"GW{event}"
        texts[column] = "—" if rows.empty else _cell_text(rows)
        ratings[column] = None if rows.empty else rows[mode["rating"]].mean()
    text_rows[team] = texts
    rating_rows[team] = ratings
    values = team_rows[mode["value"]]
    totals[team] = values.mean() if mode["ascending"] else values.sum()

grid = pd.DataFrame.from_dict(text_rows, orient="index")
ratings = pd.DataFrame.from_dict(rating_rows, orient="index")
grid.insert(0, mode["total"], pd.Series(totals))
grid = grid.sort_values(mode["total"], ascending=mode["ascending"])
ratings = ratings.reindex(grid.index)
grid.index.name = "Team"


def _styles(frame: pd.DataFrame) -> pd.DataFrame:
    styles = pd.DataFrame("", index=frame.index, columns=frame.columns)
    for column in ratings.columns:
        styles[column] = ratings[column].map(ease_cell_style)
    return styles


styled = grid.style.apply(_styles, axis=None).format({mode["total"]: "{:.2f}" if not mode["ascending"] else "{:.1f}"})
st.dataframe(styled, height=36 * (len(grid) + 1) + 4, width="stretch")
