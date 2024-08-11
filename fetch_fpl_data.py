import requests
import pandas as pd
import duckdb
from typing import Dict, Any, List
from datetime import datetime

# Configuration
CONFIG: Dict[str, Any] = {
    "base_url": "https://fantasy.premierleague.com/api/",
    "endpoints": {
        "bootstrap-static": "bootstrap-static/",
        "fixtures": "fixtures/",
        "element-summary": "element-summary/{}/"
    },
    "db_name": "fpl_api_data.db"
}

def fetch_api_data(endpoint: str, element_id: int = None) -> Dict[str, Any]:
    """
    Fetch data from the FPL API.

    Args:
        endpoint: API endpoint to fetch data from
        element_id: Player ID for element-summary endpoint (optional)

    Returns:
        JSON response from the API
    """
    url = CONFIG["base_url"] + CONFIG["endpoints"][endpoint]
    if element_id:
        url = url.format(element_id)
    
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def create_table_from_dict(conn: duckdb.DuckDBPyConnection, table_name: str, data: Dict[str, Any]) -> None:
    """
    Create a table in DuckDB from a dictionary.

    Args:
        conn: DuckDB connection object
        table_name: Name of the table to create
        data: Dictionary containing the data
    """
    df = pd.DataFrame([data])
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    print(f"Table '{table_name}' created successfully.")

def create_table_from_list(conn: duckdb.DuckDBPyConnection, table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Create a table in DuckDB from a list of dictionaries.

    Args:
        conn: DuckDB connection object
        table_name: Name of the table to create
        data: List of dictionaries containing the data
    """
    if not data:
        print(f"No data available for table '{table_name}'. Table not created.")
        return

    df = pd.DataFrame(data)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    print(f"Table '{table_name}' created successfully.")

def process_bootstrap_static(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Process and store bootstrap-static data.
    """
    data = fetch_api_data("bootstrap-static")
    
    # Store events data
    create_table_from_list(conn, "events", data["events"])
    
    # Store game settings
    create_table_from_dict(conn, "game_settings", data["game_settings"])
    
    # Store phases
    create_table_from_list(conn, "phases", data["phases"])
    
    # Store teams
    create_table_from_list(conn, "teams", data["teams"])
    
    # Store elements (players)
    create_table_from_list(conn, "players", data["elements"])
    
    # Store element types
    create_table_from_list(conn, "element_types", data["element_types"])

def process_fixtures(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Process and store fixtures data.
    """
    data = fetch_api_data("fixtures")
    create_table_from_list(conn, "fixtures", data)

def process_player_history(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Process and store player history data for all players.
    """
    players = conn.execute("SELECT id FROM players").fetchall()
    all_history = []
    all_fixtures = []

    for player_id in players:
        try:
            data = fetch_api_data("element-summary", player_id[0])
            
            for history in data.get("history", []):
                history["player_id"] = player_id[0]
                all_history.append(history)
            
            for fixture in data.get("fixtures", []):
                fixture["player_id"] = player_id[0]
                all_fixtures.append(fixture)
        except Exception as e:
            print(f"Error processing player {player_id[0]}: {str(e)}")

    if all_history:
        create_table_from_list(conn, "player_history", all_history)
    else:
        print("No player history data available.")

    if all_fixtures:
        create_table_from_list(conn, "player_fixtures", all_fixtures)
    else:
        print("No player fixtures data available.")

def main() -> None:
    """
    Main function to orchestrate the data extraction and loading process.
    """
    conn = duckdb.connect(CONFIG["db_name"])
    
    try:
        # Create a snapshot timestamp
        snapshot_time = datetime.now().isoformat()
        
        # Process bootstrap-static data
        process_bootstrap_static(conn)
        
        # Process fixtures data
        process_fixtures(conn)
        
        # Process player history data
        process_player_history(conn)
        
        # Create a snapshot metadata table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS snapshot_metadata (
                snapshot_time TIMESTAMP,
                tables_updated ARRAY(VARCHAR)
            )
        """)
        
        # Record the snapshot metadata
        tables_updated = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        tables_updated = [table[0] for table in tables_updated if table[0] != 'snapshot_metadata']
        
        conn.execute("""
            INSERT INTO snapshot_metadata (snapshot_time, tables_updated)
            VALUES (?, ?)
        """, [snapshot_time, tables_updated])
        
        print(f"Snapshot completed at {snapshot_time}")
    
    except Exception as e:
        print(f"An error occurred during execution: {str(e)}")
    
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()