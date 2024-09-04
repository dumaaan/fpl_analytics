import streamlit as st
import pandas as pd
import duckdb
import altair as alt

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect('fpl_api_data.db')

conn = get_connection()

@st.cache_data
def load_data():
    try:
        df = pd.read_sql("SELECT * FROM fpl_player_stats", conn)
        # Convert relevant columns to numeric
        numeric_columns = ['price', 'selected_by_percent', 'form', 'points_per_game', 'total_points', 
                           'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
                           'influence', 'creativity', 'threat', 'ict_index', 'expected_goals', 
                           'expected_assists', 'goals_per_90', 'assists_per_90', 'xG_per_90', 'xA_per_90',
                           'points_per_million', 'points_per_90']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

data = load_data()

if data.empty:
    st.write("No data available. Please check your database connection and data.")
    st.stop()

# Dashboard title
st.title("Fantasy Premier League Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
position = st.sidebar.selectbox("Select Position", ["All"] + sorted(data["position"].unique()))
min_price = st.sidebar.slider("Minimum Price", float(data["price"].min()), float(data["price"].max()), float(data["price"].min()))
min_points = st.sidebar.slider("Minimum Total Points", int(data["total_points"].min()), int(data["total_points"].max()), int(data["total_points"].min()))

# Filter data
filtered_data = data[
    (data["position"] == position if position != "All" else True) &
    (data["price"] >= min_price) &
    (data["total_points"] >= min_points)
]

# Top performers
st.header("Top Performers")
top_performers = filtered_data.sort_values("total_points", ascending=False).head(10)
st.dataframe(top_performers[["web_name", "team", "position", "total_points", "price", "points_per_million"]])

# Form vs Price scatter plot
st.header("Form vs Price")
form_price_chart = alt.Chart(filtered_data).mark_circle().encode(
    x='price:Q',
    y='form:Q',
    color='position:N',
    size='total_points:Q',
    tooltip=['web_name:N', 'team:N', 'price:Q', 'form:Q', 'total_points:Q']
).interactive()
st.altair_chart(form_price_chart, use_container_width=True)

# Player selection by position
st.header("Player Selection by Position")
try:
    selection_data = data.groupby("position").agg({
        "id": "count",
        "selected_by_percent": lambda x: pd.to_numeric(x, errors='coerce').mean()
    }).reset_index()
    
    if selection_data['selected_by_percent'].isnull().all():
        st.write("No valid data available for player selection percentages.")
    else:
        selection_chart = alt.Chart(selection_data).mark_bar().encode(
            x='position:N',
            y='selected_by_percent:Q',
            color='position:N',
            tooltip=['position:N', 'id:Q', 'selected_by_percent:Q']
        )
        st.altair_chart(selection_chart, use_container_width=True)
except Exception as e:
    st.write(f"An error occurred while processing player selection data: {str(e)}")
    st.write("Skipping player selection chart.")

# Performance metrics by position
st.header("Performance Metrics by Position")
metrics_data = data.groupby("position").agg({
    "total_points": "mean",
    "goals_per_90": "mean",
    "assists_per_90": "mean",
    "xG_per_90": "mean",
    "xA_per_90": "mean"
}).reset_index()
st.dataframe(metrics_data.round(2))

# Top value players
st.header("Top Value Players")
value_players = filtered_data.sort_values("points_per_million", ascending=False).head(10)
st.dataframe(value_players[["web_name", "team", "position", "total_points", "price", "points_per_million"]])

# Expected Goals vs Actual Goals
st.header("Expected Goals vs Actual Goals")
xg_vs_goals = alt.Chart(filtered_data).mark_circle().encode(
    x='expected_goals:Q',
    y='goals_scored:Q',
    color='position:N',
    size='minutes:Q',
    tooltip=['web_name:N', 'team:N', 'expected_goals:Q', 'goals_scored:Q', 'minutes:Q']
).interactive()
st.altair_chart(xg_vs_goals, use_container_width=True)

# Player Influence
st.header("Player Influence")
influence_chart = alt.Chart(filtered_data).mark_bar().encode(
    x='web_name:N',
    y='influence:Q',
    color='position:N',
    tooltip=['web_name:N', 'team:N', 'influence:Q', 'creativity:Q', 'threat:Q']
).transform_window(
    rank='rank(influence)',
    sort=[alt.SortField('influence', order='descending')]
).transform_filter(
    alt.datum.rank <= 20
).interactive()
st.altair_chart(influence_chart, use_container_width=True)

# Last Updated
last_updated = data['last_updated'].max()
st.sidebar.text(f"Data last updated: {last_updated}")

# Close the connection
conn.close()