import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import os
import boto3

s3 = boto3.client("s3")


def upload_to_s3(file_name, object_name):
    bucket_name = "fpldatabucket"
    try:
        s3.upload_file(file_name, bucket_name, f"raw_data/{object_name}")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")


def get_tables(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "lxml")
    all_tables = soup.findAll("tbody")
    return all_tables


def get_team_links():
    tempo = get_tables("https://fbref.com/en/comps/9/Premier-League-Stats")
    team_data = {}
    for row in tempo[0].find_all("tr"):
        team_cell = row.find("td", {"data-stat": "team"})
        if team_cell and team_cell.a:
            team_name = team_cell.a.text
            team_link = "https://fbref.com" + team_cell.a["href"]
            team_data[team_name] = team_link
    return team_data


def rows_to_datafame(rows):
    headers = [th["data-stat"] for th in rows.find_all("tr")[0].select("th,td")]
    row_data = [
        [
            cell.a.get_text(strip=True) if cell.a else cell.get_text(strip=True)
            for cell in row.select("th, td")
        ]
        for row in rows.find_all("tr")
    ]
    return pd.DataFrame(row_data, columns=headers)


def save_fbref_files():
    indices_to_track = [0, 3, 4, 5, 8]
    team_data = get_team_links()
    save_path = os.path.expanduser("~/Downloads/testdata/")
    for k, v in team_data.items():
        team_raw = get_tables(v)
        dataframes = {
            index: rows_to_datafame(team_raw[index]) for index in indices_to_track
        }
        print(f"Saving file for team {k}")
        for idx, key in zip(
            indices_to_track,
            ["generic", "goalkeeper", "shooting", "passing", "defensive"],
        ):
            file_name = (
                f"{k.replace(' ','_')}_{date.today().strftime('%Y%m%d')}_{key}.csv"
            )
            local_file_path = os.path.expanduser(f"~/fpldata_local/{file_name}")
            dataframes[idx].to_csv(local_file_path)
            upload_to_s3(local_file_path, file_name)
    print("Saving fbref files is done.")


def save_and_update_fpl_data():
    # Get data from FPL
    base_url = "https://fantasy.premierleague.com/api/"
    r = requests.get(base_url + "bootstrap-static/").json()

    # Process and save players data
    players = pd.json_normalize(r["elements"])
    positions = pd.json_normalize(r["element_types"])
    players = players.merge(positions, left_on="element_type", right_on="id")
    player_file_name = f"players_data_{date.today().strftime('%Y%m%d')}.csv"
    local_file_path = os.path.expanduser(f"~/fpldata_local/{player_file_name}")
    players.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, player_file_name)

    # Process and save teams data
    teams = pd.json_normalize(r["teams"])
    # Update and save teams_ready dataframe
    teams_ready = pd.read_csv(
        os.path.expanduser("~/Downloads/epl-2023-GMTStandardTime.csv")
    )
    strength = teams[["id", "name", "strength"]]
    strength = strength.replace("Nott'm Forest", "Nottingham Forest")
    teams_ready = teams_ready.merge(
        strength, how="left", left_on="Home Team", right_on="name"
    ).merge(strength, how="left", left_on="Away Team", right_on="name")
    teams_ready = teams_ready.rename(
        columns={
            "id_x": "Home_team_id",
            "name_x": "Home_team_name",
            "strength_x": "Home_team_strength",
            "id_y": "Away_team_id",
            "name_y": "Away_team_name",
            "strength_y": "Away_team_strength",
        }
    )
    team_file_name = f"teams_ready_updated_{date.today().strftime('%Y%m%d')}.csv"
    local_file_path = os.path.expanduser(f"~/fpldata_local/{team_file_name}")
    teams_ready.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, team_file_name)

    print("FPL data has been uploaded")


if __name__ == "__main__":
    save_fbref_files()
    save_and_update_fpl_data()
