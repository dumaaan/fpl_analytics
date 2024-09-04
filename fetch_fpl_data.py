import requests
import pandas as pd
import duckdb
from typing import Dict, Any, List
from datetime import datetime
import json

# Configuration
CONFIG: Dict[str, Any] = {
    "base_url": "https://fantasy.premierleague.com/api/",
    "endpoints": {
        "bootstrap-static": "bootstrap-static/"
    },
    "db_name": "fpl_api_data.db"
}

def fetch_api_data(endpoint: str) -> Dict[str, Any]:
    """
    Fetch data from the FPL API.
    """
    url = CONFIG["base_url"] + CONFIG["endpoints"][endpoint]
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {str(e)}")
        return None

def create_table_from_list(conn: duckdb.DuckDBPyConnection, table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Create or replace a table in DuckDB from a list of dictionaries.
    """
    if not data:
        print(f"No data available for table '{table_name}'. Table not created.")
        return

    df = pd.DataFrame(data)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    print(f"Table '{table_name}' created/updated successfully.")

def main() -> None:
    """
    Main function to orchestrate the data extraction and loading process.
    """
    conn = duckdb.connect(CONFIG["db_name"])
    
    try:
        # Fetch bootstrap-static data
        data = fetch_api_data("bootstrap-static")
        
        if data:
            # Store players data
            create_table_from_list(conn, "players", data["elements"])
            
            # Store teams data
            create_table_from_list(conn, "teams", data["teams"])
            
            # Create a snapshot metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshot_metadata (
                    snapshot_time TIMESTAMP,
                    tables_updated VARCHAR
                )
            """)
            
            # Record the snapshot metadata
            snapshot_time = datetime.now().isoformat()
            tables_updated = json.dumps(["players", "teams"])
            
            conn.execute("""
                INSERT INTO snapshot_metadata (snapshot_time, tables_updated)
                VALUES (?, ?)
            """, [snapshot_time, tables_updated])
            
            print(f"Data updated successfully at {snapshot_time}")
        else:
            print("Failed to fetch data from FPL API")
    
    except Exception as e:
        print(f"An error occurred during execution: {str(e)}")
    
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()