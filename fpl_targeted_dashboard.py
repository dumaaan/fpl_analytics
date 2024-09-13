import streamlit as st
import pandas as pd
import altair as alt
import duckdb
import numpy as np
from typing import List, Dict, Any

# Configuration
CONFIG: Dict[str, Any] = {
    'db_name': 'fpl_api_data.db',
    'min_minutes': 90,
    'top_n_players': 20,
    'chart_width': 800,
    'chart_height': 500
}

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect(CONFIG['db_name'])

# Load and preprocess data
@st.cache_data
def load_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM fpl_player_stats", conn)
    numeric_columns = [
        'price', 'total_points', 'minutes', 'goals_scored', 'assists', 
        'expected_goals', 'expected_assists', 'selected_by_percent', 
        'form', 'points_per_game', 'bonus', 'bps', 'influence', 
        'creativity', 'threat', 'ict_index', 'yellow_cards', 'red_cards',
        'starts', 'points_per_million', 'clean_sheets', 'goals_conceded', 'saves'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate additional metrics
    df['goal_overperformance'] = df['goals_scored'] - df['expected_goals']
    df['assist_overperformance'] = df['assists'] - df['expected_assists']
    df['points_per_90'] = df['total_points'] / (df['minutes'] / 90)
    df['cards'] = df['yellow_cards'] + 2 * df['red_cards']
    df['goals_per_90'] = df['goals_scored'] / (df['minutes'] / 90)
    df['assists_per_90'] = df['assists'] / (df['minutes'] / 90)
    df['goal_involvement_per_90'] = (df['goals_scored'] + df['assists']) / (df['minutes'] / 90)
    df['xGI_per_90'] = (df['expected_goals'] + df['expected_assists']) / (df['minutes'] / 90)
    df['clean_sheet_percentage'] = df['clean_sheets'] / (df['minutes'] / 90)
    df['goals_conceded_per_90'] = df['goals_conceded'] / (df['minutes'] / 90)
    df['saves_per_90'] = df['saves'] / (df['minutes'] / 90)
    df['bonus_points_per_90'] = df['bonus'] / (df['minutes'] / 90)
    df['bps_per_90'] = df['bps'] / (df['minutes'] / 90)
    df['card_rate'] = df['cards'] / (df['minutes'] / 90)
    
    # New value metric: Points per game per Cost
    df['value_ppg_per_cost'] = df['points_per_game'] / df['price']
    
    # Consistency metrics
    df['coefficient_of_variation'] = df.groupby('position')['points_per_game'].transform(lambda x: x.std() / x.mean())
    df['reliable_starter_score'] = df['starts'] / df['minutes'] * 90
    
    # Team reliance metrics
    team_goals = df.groupby('team')['goals_scored'].transform('sum')
    team_assists = df.groupby('team')['assists'].transform('sum')
    df['team_reliance_goals'] = df['goals_scored'] / team_goals
    df['team_reliance_assists'] = df['assists'] / team_assists
    
    return df

# Utility functions for tables and charts
def create_table(data: pd.DataFrame, columns: List[str], sort_by: str = 'total_points', ascending: bool = False) -> None:
    sorted_data = data.sort_values(sort_by, ascending=ascending)
    st.dataframe(sorted_data[columns], use_container_width=True)

def create_value_analysis_chart(data: pd.DataFrame) -> alt.Chart:
    return alt.Chart(data).mark_circle().encode(
        x=alt.X('price:Q', title='Price'),
        y=alt.Y('value_ppg_per_cost:Q', title='Points per Game per £1m'),
        color='position:N',
        size='total_points:Q',
        tooltip=['web_name', 'team', 'price', 'value_ppg_per_cost', 'total_points']
    ).properties(
        width=CONFIG['chart_width'],
        height=CONFIG['chart_height'],
        title="Value Analysis: Points per Game per £1m vs Price"
    ).interactive()

def create_form_and_momentum_chart(data: pd.DataFrame) -> alt.Chart:
    return alt.Chart(data).mark_circle().encode(
        x=alt.X('form:Q', title='Form'),
        y=alt.Y('points_per_90:Q', title='Points per 90'),
        color='position:N',
        size='minutes:Q',
        tooltip=['web_name', 'team', 'form', 'points_per_90', 'minutes', 'total_points']
    ).properties(
        width=CONFIG['chart_width'],
        height=CONFIG['chart_height'],
        title="Form and Momentum: Current Form vs Points per 90"
    ).interactive()

# Main dashboard function
def run_dashboard():
    st.set_page_config(layout="wide")
    st.title("Enhanced FPL Insights Dashboard")
    
    try:
        data = load_data()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return

    # Enhanced filtering
    st.sidebar.header("Filters")
    position = st.sidebar.multiselect("Select Positions", options=["All"] + sorted(data["position"].unique()), default="All")
    teams = st.sidebar.multiselect("Select Teams", options=["All"] + sorted(data["team"].unique()), default="All")
    price_range = st.sidebar.slider("Price Range", float(data["price"].min()), float(data["price"].max()), (float(data["price"].min()), float(data["price"].max())))
    min_points = st.sidebar.number_input("Minimum Total Points", min_value=0, value=0)
    
    # Apply filters
    filtered_data = data[
        (data["position"].isin(position) if "All" not in position else True) &
        (data["team"].isin(teams) if "All" not in teams else True) &
        (data["price"].between(price_range[0], price_range[1])) &
        (data["total_points"] >= min_points) &
        (data["minutes"] >= CONFIG['min_minutes'])
    ]

    if filtered_data.empty:
        st.warning("No players match the selected filters. Please adjust your criteria.")
        return

    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Attacking Stats", "Defensive Stats", "Value Metrics"])

    with tab1:
        st.header("Overview")
        overview_columns = ['web_name', 'team', 'position', 'price', 'total_points', 'points_per_game', 'minutes', 'form']
        create_table(filtered_data, overview_columns)
        
        st.subheader("Key Visualizations")
        col1, col2 = st.columns(2)
        with col1:
            st.altair_chart(create_value_analysis_chart(filtered_data), use_container_width=True)
        with col2:
            st.altair_chart(create_form_and_momentum_chart(filtered_data), use_container_width=True)

    with tab2:
        st.header("Attacking Stats")
        attacking_columns = ['web_name', 'team', 'position', 'goals_scored', 'assists', 'expected_goals', 'expected_assists', 'goal_involvement_per_90', 'xGI_per_90']
        create_table(filtered_data, attacking_columns, sort_by='goals_scored')

    with tab3:
        st.header("Defensive Stats")
        defensive_columns = ['web_name', 'team', 'position', 'clean_sheets', 'goals_conceded', 'saves', 'yellow_cards', 'red_cards']
        create_table(filtered_data, defensive_columns, sort_by='clean_sheets')

    with tab4:
        st.header("Value Metrics")
        value_columns = ['web_name', 'team', 'position', 'price', 'total_points', 'points_per_game', 'value_ppg_per_cost', 'points_per_90']
        create_table(filtered_data, value_columns, sort_by='value_ppg_per_cost')

    # Player Comparison Feature
    st.header("Player Comparison")
    players_to_compare = st.multiselect("Select players to compare", options=data['web_name'].tolist(), max_selections=3)
    if players_to_compare:
        comparison_data = data[data['web_name'].isin(players_to_compare)]
        
        # Create a unique list of columns for comparison
        comparison_columns = list(dict.fromkeys(
            overview_columns +
            attacking_columns[3:] +
            defensive_columns[3:] +
            value_columns[3:]
        ))
        
        st.dataframe(comparison_data[comparison_columns])
    else:
        st.info("Select up to three players to compare their stats.")

    # Key Statistical Indicators
    st.header("Key Statistical Indicators")
    stats_to_show = ['total_points', 'points_per_game', 'value_ppg_per_cost', 'goals_scored', 'assists', 'clean_sheets']
    for stat in stats_to_show:
        col1, col2, col3 = st.columns(3)
        col1.metric(f"Average {stat}", f"{filtered_data[stat].mean():.2f}")
        col2.metric(f"Median {stat}", f"{filtered_data[stat].median():.2f}")
        col3.metric(f"90th Percentile {stat}", f"{filtered_data[stat].quantile(0.9):.2f}")

    # Data Export Feature
    st.download_button(
        label="Download data as CSV",
        data=filtered_data.to_csv(index=False).encode('utf-8'),
        file_name="fpl_data.csv",
        mime="text/csv",
    )

if __name__ == "__main__":
    run_dashboard()