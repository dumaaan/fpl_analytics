import base64
import requests
import pandas as pd
import duckdb
import os
import io
import time
import pickle
from github import Github, RateLimitExceededException
from tqdm import tqdm
import re


# Function to download file from GitHub
def download_file(repo, file_path):
    try:
        content_file = repo.get_contents(file_path)
        if content_file.encoding == "base64":
            file_content = base64.b64decode(content_file.content).decode("utf-8")
        else:
            # If not base64 encoded, assume it's already decoded
            file_content = content_file.decoded_content.decode("utf-8")
    except Exception as e:
        print(f"Error downloading {file_path}: {str(e)}")
        return None

    try:
        return pd.read_csv(io.StringIO(file_content))
    except Exception as e:
        print(f"Error parsing CSV for {file_path}: {str(e)}")
        return None


# Function to create table in DuckDB
def create_table(conn, table_name, df):
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


def table_exists(conn, table_name):
    result = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    return len(result.fetchall()) > 0


def github_api_call(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except RateLimitExceededException as e:
            reset_time = g.get_rate_limit().core.reset.timestamp()
            sleep_time = reset_time - time.time() + 1  # Add 1 second buffer
            print(f"Rate limit exceeded. Sleeping for {sleep_time:.2f} seconds.")
            time.sleep(max(sleep_time, 0))
        except Exception as e:
            print(f"Error in API call: {str(e)}")
            return None


# GitHub repository details
repo_owner = "vaastav"
repo_name = "Fantasy-Premier-League"
github_token = os.environ.get("GITHUB_TOKEN")  # Replace with your GitHub token

# Connect to DuckDB
conn = duckdb.connect("fpl_data.db")

# Initialize GitHub client
g = Github(github_token)
repo = g.get_repo(f"{repo_owner}/{repo_name}")

# Seasons to process
seasons = ["2023-24", "2022-23"]  # Add more seasons as needed

for season in seasons:
    print(f"Processing season {season}")

    # Process GW data
    gw_folder = f"data/{season}/gws"
    gw_contents = github_api_call(repo.get_contents, gw_folder)

    for content in tqdm(gw_contents, desc="Processing GW data"):
        if content.name.endswith(".csv"):
            table_name = f"gw_{season.replace('-', '_')}_{content.name.split('.')[0]}"
            if not table_exists(conn, table_name):
                df = download_file(repo, content.path)
                if df is not None:
                    create_table(conn, table_name, df)
            else:
                print(f"Skipping {content.name}, table already exists.")

    # Process player data
    players_folder = f"data/{season}/players"
    players_contents = repo.get_contents(players_folder)

    for player_folder in tqdm(players_contents, desc="Processing player data"):
        player_name = re.sub(r"[^a-zA-Z0-9]", "_", player_folder.name)
        table_name = f"player_{season.replace('-', '_')}_{player_name}"
        if not table_exists(conn, table_name):
            gw_file = f"{players_folder}/{player_folder.name}/gw.csv"
            try:
                df = download_file(repo, gw_file)
                if df is not None:
                    create_table(conn, table_name, df)
            except Exception as e:
                print(f"Error processing {player_name}: {str(e)}")
        else:
            print(f"Skipping {player_name}, table already exists.")

    # Process understat data
    understat_folder = f"data/{season}/understat"
    understat_contents = repo.get_contents(understat_folder)

    for content in tqdm(understat_contents, desc="Processing understat data"):
        if content.name.endswith(".csv"):
            player_name = re.sub(r"[^a-zA-Z0-9]", "_", content.name.split(".")[0])
            table_name = f"understat_{season.replace('-', '_')}_{player_name}"
            if not table_exists(conn, table_name):
                df = download_file(repo, content.path)
                if df is not None:
                    create_table(conn, table_name, df)
            else:
                print(f"Skipping {content.name}, table already exists.")

# Commit changes and close connection
conn.commit()
conn.close()

print("All data has been successfully loaded into DuckDB.")
