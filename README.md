# FPL Analytics Dashboard

Welcome to the FPL (Fantasy Premier League) Analytics Dashboard! This dashboard is designed to help fantasy football enthusiasts make data-driven decisions to optimize their FPL strategies. The dashboard provides a comprehensive view of player statistics, performance metrics, and visualizations to enhance your FPL experience.

## Features

- **Interactive Data Tables**: Sort and view player statistics including goals, assists, points per game, and more.
- **Dynamic Filtering**: Filter players by position, team, price range, and minimum total points to narrow down your selections.
- **Visual Analytics**: Explore data through interactive charts that display value analysis and form momentum of players.
- **Player Comparison**: Select up to three players to compare their detailed performance statistics side by side.
- **Statistical Insights**: View key statistical indicators such as average, median, and 90th percentile values for various performance metrics.
- **Data Export**: Download the filtered player data as a CSV file for your own analysis.

## Dashboard Layout

- **Overview Tab**: Displays general player statistics and key visualizations.
- **Attacking Stats Tab**: Focuses on offensive metrics like goals scored and expected goals.
- **Defensive Stats Tab**: Shows data related to defensive efforts including clean sheets and goals conceded.
- **Value Metrics Tab**: Analyzes players' performance relative to their cost.

## Technology

This dashboard is built using Python with the following key libraries:
- **Streamlit**: For creating the web application.
- **Pandas**: For data manipulation and analysis.
- **Altair**: For generating interactive visualizations.
- **Requests**: For fetching data from the FPL API.

## Data Source

The data is sourced in real-time from the official Fantasy Premier League API, ensuring that the dashboard reflects the most current information available.

## Usage

To run the dashboard locally, you will need Python installed on your machine along with the required libraries. You can install the dependencies using:

```bash
pip install streamlit pandas altair requests
```
To start the application, run the following command in your terminal:

```
streamlit run app.py
```

Replace app.py with the path to the script file if your file name or directory is different.


