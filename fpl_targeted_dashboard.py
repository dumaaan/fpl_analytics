import streamlit as st
import pandas as pd
import altair as alt
import requests
from typing import List, Dict, Any

# Configuration
CONFIG: Dict[str, Any] = {
    'min_minutes': 90,
    'top_n_players': 20,
    'chart_width': 800,
    'chart_height': 500
}

TEAM_MAPPING = {
    1: 'Arsenal',
    2: 'Aston Villa',
    3: 'Bournemouth',
    4: 'Brentford',
    5: 'Brighton',
    6: 'Chelsea',
    7: 'Crystal Palace',
    8: 'Everton',
    9: 'Fulham',
    10: 'Ipswich',
    11: 'Leicester',
    12: 'Liverpool',
    13: 'Man City',
    14: 'Man Utd',
    15: 'Newcastle',
    16: 'Nottingham Forest',
    17: 'Southampton',
    18: 'Tottenham',
    19: 'West Ham',
    20: 'Wolves'
}

# Fetch data from FPL API
@st.cache_data
def load_data():
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    data = response.json()
    
    df = pd.DataFrame(data['elements'])
    df['team'] = df['team'].map(TEAM_MAPPING).fillna('Unknown')  # Replace team ID with name, fill any missing with 'Unknown'
    
    # Convert relevant columns to numeric
    numeric_columns = [
        'cost_change_event', 'cost_change_event_fall', 'cost_change_start',
        'cost_change_start_fall', 'dreamteam_count', 'event_points', 'form',
        'in_dreamteam', 'now_cost', 'points_per_game', 'selected_by_percent',
        'total_points', 'transfers_in', 'transfers_in_event', 'transfers_out',
        'transfers_out_event', 'value_form', 'value_season', 'minutes',
        'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
        'own_goals', 'penalties_saved', 'penalties_missed', 'yellow_cards',
        'red_cards', 'saves', 'bonus', 'bps', 'influence', 'creativity',
        'threat', 'ict_index', 'starts', 'expected_goals', 'expected_assists',
        'expected_goal_involvements', 'expected_goals_conceded'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate additional metrics
    df['price'] = df['now_cost'] / 10
    df['points_per_million'] = df['total_points'] / df['price']
    df['points_per_90'] = df['total_points'] / (df['minutes'] / 90)
    df['goals_per_90'] = df['goals_scored'] / (df['minutes'] / 90)
    df['assists_per_90'] = df['assists'] / (df['minutes'] / 90)
    df['goal_involvement_per_90'] = (df['goals_scored'] + df['assists']) / (df['minutes'] / 90)
    df['xGI_per_90'] = (df['expected_goals'] + df['expected_assists']) / (df['minutes'] / 90)
    df['clean_sheet_percentage'] = df['clean_sheets'] / (df['minutes'] / 90)
    df['goals_conceded_per_90'] = df['goals_conceded'] / (df['minutes'] / 90)
    df['value_ppg_per_cost'] = df['points_per_game'] / df['price']
    
    # Map element_type to position
    position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    df['position'] = df['element_type'].map(position_map)
    
    return df

def create_table(data: pd.DataFrame, columns: List[str], sort_by: str = 'total_points', ascending: bool = False) -> None:
    # Ensure unique columns
    unique_columns = list(dict.fromkeys(columns))
    
    
    # Check if sort_by column exists in the dataframe
    if sort_by not in data.columns:
        st.warning(f"Sort column '{sort_by}' not found. Sorting by the first column instead.")
        sort_by = unique_columns[0]
    
    sorted_data = data.sort_values(sort_by, ascending=ascending)
    
    # Ensure all columns exist in the dataframe
    existing_columns = [col for col in unique_columns if col in sorted_data.columns]
    
    if len(existing_columns) != len(unique_columns):
        missing_columns = set(unique_columns) - set(existing_columns)
        st.warning(f"Some columns were not found in the data: {', '.join(missing_columns)}")
    
    st.dataframe(sorted_data[existing_columns], use_container_width=True)

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
        
        comparison_columns = [
            'web_name', 'team', 'position', 'price', 'total_points', 'points_per_game', 'minutes', 'form',
            'goals_scored', 'assists', 'expected_goals', 'expected_assists', 'goal_involvement_per_90', 'xGI_per_90',
            'clean_sheets', 'goals_conceded', 'saves', 'yellow_cards', 'red_cards',
            'value_ppg_per_cost', 'points_per_90'
        ]
        
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