from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import os
import boto3
import random
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

s3 = boto3.client("s3")


def upload_to_s3(file_name, object_name):
    bucket_name = "fpldatabucket"
    try:
        s3.upload_file(file_name, bucket_name, f"raw_data/{object_name}")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")


def get_tables(url):
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.3',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1'
    }
    time_to_sleep = random.uniform(3.0, 6.0)  # Random time between 3 and 6 seconds
    time.sleep(time_to_sleep)
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, "lxml")
        all_tables = soup.findAll("tbody")
        return all_tables
    else:
        print(f"Failed to retrieve the webpage. Status code: {res.status_code}")


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
    indices_to_track = [0, 2, 3, 4, 5, 8]
    team_data = get_team_links()
    for k, v in team_data.items():
        team_raw = get_tables(v)
        dataframes = {
            index: rows_to_datafame(team_raw[index]) for index in indices_to_track
        }
        print(f"Saving file for team {k}")
        for idx, key in zip(
            indices_to_track,
            ["generic", "goalkeeper-basic", "goalkeeper", "shooting", "passing", "defensive"],
        ):
            if k == "Nott'ham Forest":
                file_name = (
                f'Nottingham_Forest_{date.today().strftime("%Y%m%d")}_{key}.csv'
            )
            else:
                file_name = (
                    f'{k.replace(" ","_")}_{date.today().strftime("%Y%m%d")}_{key}.csv'
                )
            local_file_path = os.path.expanduser(f"~/fpl_analytics/fpldata_local/{file_name}")
            if idx not in (2,3):
                dataframes[idx]['team_name'] = str(k.replace(' ','_'))
                dataframes[idx].to_csv(local_file_path, index=False)
            elif idx == 2:
                to_concat = dataframes[idx]
                to_concat.drop(['nationality','age','position','matches'], axis=1, inplace=True)
                print(to_concat)
            elif idx == 3:
                dataframes[idx] = dataframes[idx].merge(to_concat, on='player')
                dataframes[idx]['team_name'] = str(k.replace(' ','_'))
                print('saving goalkeeping data')
                dataframes[idx].to_csv(local_file_path, index=False)
                print(dataframes[idx])
            #upload_to_s3(local_file_path, file_name)
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
    local_file_path = os.path.expanduser(f"~/fpl_analytics/fpldata_local/{player_file_name}")
    players.to_csv(local_file_path, index=False)
    #upload_to_s3(local_file_path, player_file_name)

    # Process and save teams data
    teams = pd.json_normalize(r["teams"])
    # Update and save teams_ready dataframe
    teams_ready = pd.read_csv(
    os.path.expanduser("~/fpl_analytics/fpldata_local/epl-2023-GMTStandardTime.csv")
    )
    strength = teams[["id", "name", "strength"]]
    team_name_replace_dict = {
    'Luton': 'Luton Town',
    'Man City': 'Manchester City',
    'Man Utd': 'Manchester Utd',
    'Newcastle': 'Newcastle Utd',
    "Nott'm Forest": "Nottingham Forest",
    'Spurs': 'Tottenham'
   }
    strength = strength.replace(team_name_replace_dict)
    teams_ready['Home Team'] = teams_ready['Home Team'].replace(team_name_replace_dict)
    teams_ready['Away Team'] = teams_ready['Away Team'].replace(team_name_replace_dict)
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
    local_file_path = os.path.expanduser(f"~/fpl_analytics/fpldata_local/{team_file_name}")
    teams_ready.to_csv(local_file_path, index=False)
   #upload_to_s3(local_file_path, team_file_name)

    print("FPL data has been uploaded")

with DAG(
    'fetch_and_save_raw_fpl_data',
    default_args=default_args,
    description='A DAG to fetch and save FBRef and FPL data',
    schedule_interval=None,
    start_date=datetime(2023, 10, 14),
    catchup=False,
) as dag:

    save_fbref_task = PythonOperator(
        task_id='save_fbref_files',
        python_callable=save_fbref_files,
    )

    save_fpl_task = PythonOperator(
        task_id='save_and_update_fpl_data',
        python_callable=save_and_update_fpl_data,
    )

    [save_fbref_task, save_fpl_task]