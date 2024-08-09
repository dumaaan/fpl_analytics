import base64
import io
import os
import re
import time
from typing import List, Optional, Dict, Any

# Third-party imports
import pandas as pd
import duckdb
from github import Github, RateLimitExceededException
from tqdm import tqdm

# Configuration
CONFIG: Dict[str, Any] = {
    "repo_owner": "vaastav",
    "repo_name": "Fantasy-Premier-League",
    "seasons": ["2023-24", "2022-23"],
    "data_types": ["gw", "players", "understat"],
    "db_name": "fpl_data.db",
}


def download_file(repo: Any, file_path: str) -> Optional[pd.DataFrame]:
    """
    Download a file from GitHub and return its contents as a pandas DataFrame.

    Args:
        repo: GitHub repository object
        file_path: Path to the file in the repository

    Returns:
        DataFrame containing the file contents, or None if an error occurred
    """
    try:
        content_file = repo.get_contents(file_path)
        if content_file.encoding == "base64":
            file_content = base64.b64decode(content_file.content).decode("utf-8")
        else:
            file_content = content_file.decoded_content.decode("utf-8")
        return pd.read_csv(io.StringIO(file_content))
    except Exception as e:
        print(f"Error downloading or parsing {file_path}: {str(e)}")
        return None


def create_table(
    conn: duckdb.DuckDBConnection, table_name: str, df: pd.DataFrame
) -> None:
    """
    Create a table in DuckDB from a pandas DataFrame.

    Args:
        conn: DuckDB connection object
        table_name: Name of the table to create
        df: DataFrame containing the data to insert
    """
    safe_table_name = table_name.replace("-", "_")
    if table_exists(conn, safe_table_name):
        print(f'Table "{safe_table_name}" already exists. Skipping.')
        return
    try:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{safe_table_name}" AS SELECT * FROM df'
        )
        print(f'Table "{safe_table_name}" created successfully.')
    except Exception as e:
        print(f"Error creating table {safe_table_name}: {str(e)}")


def table_exists(conn: duckdb.DuckDBConnection, table_name: str) -> bool:
    """
    Check if a table exists in the DuckDB database.

    Args:
        conn: DuckDB connection object
        table_name: Name of the table to check

    Returns:
        True if the table exists, False otherwise
    """
    result = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table_name]
    )
    return len(result.fetchall()) > 0


def github_api_call(func: callable, *args: Any, **kwargs: Any) -> Any:
    """
    Make a GitHub API call with rate limit handling and exponential backoff.

    Args:
        func: GitHub API function to call
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the API call, or None if an error occurred
    """
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except RateLimitExceededException as e:
            reset_time = g.get_rate_limit().core.reset.timestamp()
            sleep_time = reset_time - time.time() + 1  # Add 1 second buffer
            print(f"Rate limit exceeded. Sleeping for {sleep_time:.2f} seconds.")
            time.sleep(max(sleep_time, 0))
        except Exception as e:
            print(f"Error in API call (attempt {attempt + 1}/{max_retries}): {str(e)}")
            time.sleep(2**attempt)  # Exponential backoff
    return None


def establish_github_connection(repo_owner: str, repo_name: str) -> Any:
    """
    Establish a connection to the GitHub repository.

    Args:
        repo_owner: Owner of the GitHub repository
        repo_name: Name of the GitHub repository

    Returns:
        GitHub repository object
    """
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable not set")
    g = Github(github_token)
    return g.get_repo(f"{repo_owner}/{repo_name}")


def process_season_data(
    repo: Any, conn: duckdb.DuckDBConnection, season: str, data_types: List[str]
) -> None:
    """
    Process data for a specific season.

    Args:
        repo: GitHub repository object
        conn: DuckDB connection object
        season: Season to process
        data_types: List of data types to process
    """
    for data_type in data_types:
        fetch_and_upload_table(repo, conn, season, data_type)


def fetch_and_upload_table(
    repo: Any, conn: duckdb.DuckDBConnection, season: str, data_type: str
) -> None:
    """
    Fetch data from GitHub and upload it to DuckDB.

    Args:
        repo: GitHub repository object
        conn: DuckDB connection object
        season: Season of the data
        data_type: Type of data to fetch and upload
    """
    if data_type == "gw":
        process_gw_data(repo, conn, season)
    elif data_type == "players":
        process_player_data(repo, conn, season)
    elif data_type == "understat":
        process_understat_data(repo, conn, season)
    else:
        print(f"Unknown data type: {data_type}")


def process_gw_data(repo: Any, conn: duckdb.DuckDBConnection, season: str) -> None:
    """Process gameweek data for a season."""
    gw_folder = f"data/{season}/gws"
    gw_contents = github_api_call(repo.get_contents, gw_folder)
    for content in tqdm(gw_contents, desc=f"Processing GW data for {season}"):
        if content.name.endswith(".csv"):
            table_name = f"gw_{season.replace('-', '_')}_{content.name.split('.')[0]}"
            if not table_exists(conn, table_name):
                df = download_file(repo, content.path)
                if df is not None:
                    create_table(conn, table_name, df)
            else:
                print(f"Skipping {content.name}, table already exists.")


def process_player_data(repo: Any, conn: duckdb.DuckDBConnection, season: str) -> None:
    """Process player data for a season."""
    players_folder = f"data/{season}/players"
    players_contents = github_api_call(repo.get_contents, players_folder)
    for player_folder in tqdm(
        players_contents, desc=f"Processing player data for {season}"
    ):
        player_name = re.sub(r"[^a-zA-Z0-9]", "_", player_folder.name)
        table_name = f"player_{season.replace('-', '_')}_{player_name}"
        if not table_exists(conn, table_name):
            gw_file = f"{players_folder}/{player_folder.name}/gw.csv"
            df = download_file(repo, gw_file)
            if df is not None:
                create_table(conn, table_name, df)
        else:
            print(f"Skipping {player_name}, table already exists.")


def process_understat_data(
    repo: Any, conn: duckdb.DuckDBConnection, season: str
) -> None:
    """Process understat data for a season."""
    understat_folder = f"data/{season}/understat"
    understat_contents = github_api_call(repo.get_contents, understat_folder)
    for content in tqdm(
        understat_contents, desc=f"Processing understat data for {season}"
    ):
        if content.name.endswith(".csv"):
            player_name = re.sub(r"[^a-zA-Z0-9]", "_", content.name.split(".")[0])
            table_name = f"understat_{season.replace('-', '_')}_{player_name}"
            if not table_exists(conn, table_name):
                df = download_file(repo, content.path)
                if df is not None:
                    create_table(conn, table_name, df)
            else:
                print(f"Skipping {content.name}, table already exists.")


def main() -> None:
    """Main function to orchestrate the data extraction and loading process."""
    repo = establish_github_connection(CONFIG["repo_owner"], CONFIG["repo_name"])
    conn = duckdb.connect(CONFIG["db_name"])

    try:
        for season in CONFIG["seasons"]:
            process_season_data(repo, conn, season, CONFIG["data_types"])
    finally:
        conn.commit()
        conn.close()

    print("All data has been successfully loaded into DuckDB.")


if __name__ == "__main__":
    main()
