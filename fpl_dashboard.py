import streamlit as st
import pandas as pd
import duckdb
import altair as alt

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect('fpl_api_data.db')

conn = get_connection()

# Load data
@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM fpl_player_stats", conn)
    numeric_columns = ['price', 'selected_by_percent', 'form', 'points_per_game', 'total_points', 
                       'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
                       'influence', 'creativity', 'threat', 'ict_index', 'expected_goals', 
                       'expected_assists', 'goals_per_90', 'assists_per_90', 'xG_per_90', 'xA_per_90',
                       'points_per_million', 'points_per_90', 'bonus', 'bps', 'saves',
                       'value_added', 'consistency_score', 'start_percentage', 'goal_involvement_per_90',
                       'xGI_per_90', 'goal_involvement_overperformance', 'clean_sheet_percentage',
                       'saves_per_90', 'goals_conceded_per_90', 'bonus_points_per_90', 'bps_per_90',
                       'ict_index_per_90', 'total_contribution_per_90', 'minutes_per_goal_involvement',
                       'points_per_minute', 'card_rate']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

data = load_data()

# Dashboard title
st.title("Fantasy Premier League Insights Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
position = st.sidebar.selectbox("Select Position", ["All"] + sorted(data["position"].unique()))
min_price = st.sidebar.slider("Minimum Price", float(data["price"].min()), float(data["price"].max()), float(data["price"].min()))
min_points = st.sidebar.slider("Minimum Total Points", int(data["total_points"].min()), int(data["total_points"].max()), int(data["total_points"].min()))
min_minutes = st.sidebar.slider("Minimum Games Played", float((data["minutes"]/90).min()), float((data["minutes"]/90).max()), float((data["minutes"]/90).min()))

# Filter data
filtered_data = data[
    (data["position"] == position if position != "All" else True) &
    (data["price"] >= min_price) &
    (data["total_points"] >= min_points) &
    (data["minutes"] >= min_minutes * 90)
]

# 1. Value Analysis
st.header("Value Analysis")

value_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='price:Q',
    y='total_points:Q',
    color='position:N',
    size='value_added:Q',
    tooltip=['web_name', 'team', 'price', 'total_points', 'value_added', 'points_per_million']
).properties(
    title="Player Value: Total Points vs Price (Size: Value Added)"
).interactive()

st.altair_chart(value_chart, use_container_width=True)

st.subheader("Top 10 Players by Value Added")
top_value_players = filtered_data.sort_values('value_added', ascending=False).head(10)
st.table(top_value_players[['web_name', 'team', 'position', 'price', 'value_added', 'points_per_million', 'total_points']])

# 2. Performance vs Expectation
st.header("Performance vs Expectation")

goals_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='xGI_per_90:Q',
    y='goal_involvement_per_90:Q',
    color='position:N',
    size='minutes:Q',
    tooltip=['web_name', 'team', 'xGI_per_90', 'goal_involvement_per_90', 'goal_involvement_overperformance', 'minutes']
).properties(
    title="Expected Goal Involvement vs Actual Goal Involvement per 90"
).interactive()

st.altair_chart(goals_chart, use_container_width=True)

# 3. Form and Consistency
st.header("Form and Consistency")

form_consistency_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='consistency_score:Q',
    y='form:Q',
    color='position:N',
    size='total_points:Q',
    tooltip=['web_name', 'team', 'consistency_score', 'form', 'total_points', 'start_percentage']
).properties(
    title="Player Form vs Consistency (Size: Total Points)"
).interactive()

st.altair_chart(form_consistency_chart, use_container_width=True)

# 4. Goalkeeper Special
if position == "GK" or position == "All":
    st.header("Goalkeeper Special")

    gk_data = filtered_data[filtered_data['position'] == 'GK']
    gk_chart = alt.Chart(gk_data).mark_circle().encode(
        x='saves_per_90:Q',
        y='clean_sheet_percentage:Q',
        size='total_points:Q',
        color='team:N',
        tooltip=['web_name', 'team', 'saves_per_90', 'clean_sheet_percentage', 'goals_conceded_per_90', 'total_points']
    ).properties(
        title="Goalkeeper Performance: Saves per 90 vs Clean Sheet %"
    ).interactive()

    st.altair_chart(gk_chart, use_container_width=True)

# 5. Differential Picks
st.header("Differential Picks")

differential_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='selected_by_percent:Q',
    y='total_points:Q',
    color='position:N',
    size='value_added:Q',
    tooltip=['web_name', 'team', 'selected_by_percent', 'total_points', 'value_added', 'points_per_million']
).properties(
    title="Differential Picks: Total Points vs Selection % (Size: Value Added)"
).interactive()

st.altair_chart(differential_chart, use_container_width=True)

# 6. Bonus Point Analysis
st.header("Bonus Point Analysis")

bonus_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='bps_per_90:Q',
    y='bonus_points_per_90:Q',
    color='position:N',
    size='total_points:Q',
    tooltip=['web_name', 'team', 'bps_per_90', 'bonus_points_per_90', 'total_points']
).properties(
    title="Bonus Points per 90 vs BPS per 90"
).interactive()

st.altair_chart(bonus_chart, use_container_width=True)

# 7. Overall Contribution
st.header("Overall Contribution")

contribution_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='ict_index_per_90:Q',
    y='total_contribution_per_90:Q',
    color='position:N',
    size='total_points:Q',
    tooltip=['web_name', 'team', 'ict_index_per_90', 'total_contribution_per_90', 'total_points']
).properties(
    title="Overall Contribution: ICT Index per 90 vs Total Contribution per 90"
).interactive()

st.altair_chart(contribution_chart, use_container_width=True)

# 8. Risk Analysis
st.header("Risk Analysis")

risk_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='card_rate:Q',
    y='minutes_per_goal_involvement:Q',
    color='position:N',
    size='total_points:Q',
    tooltip=['web_name', 'team', 'card_rate', 'minutes_per_goal_involvement', 'total_points']
).properties(
    title="Risk Analysis: Card Rate vs Minutes per Goal Involvement"
).interactive()

st.altair_chart(risk_chart, use_container_width=True)

# Display last updated time
last_updated = data['last_updated'].max()
st.sidebar.text(f"Data last updated: {last_updated}")

# Close the connection
conn.close()