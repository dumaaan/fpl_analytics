from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import os
import boto3
import random
import time
from unidecode import unidecode

def process_data(team_name,stats, **kwargs):
    ds_nodash = kwargs['ds_nodash']

    # Load Data
    fbref_file_path = "~/fpl_analytics/fpldata_local/{}_{}_{}.csv".format(team_name, ds_nodash, stats)
    fpl_file_path = "~/fpl_analytics/fpldata_local/players_data_{}.csv".format(ds_nodash)
    team_path = "~/fpl_analytics/fpldata_local/teams_ready_updated_{}.csv".format(ds_nodash)  # Replace with your actual file path

    fbref_df = pd.read_csv(fbref_file_path)
    fpl_df = pd.read_csv(fpl_file_path)
    team_df = pd.read_csv(team_path)

    team_df = team_df[['Home Team', 'Home_team_id']].drop_duplicates()
    team_df.columns = ['team_name','team_id']
    team_df['team_name'] = team_df['team_name'].str.replace(" ","_")

    # Filter FPL columns
    fpl_df_filtered = fpl_df.merge(team_df, left_on='team', right_on='team_id')
    fpl_df_filtered = fpl_df_filtered[fpl_df_filtered.status != 'u']
    fpl_df_filtered['web_name'] = fpl_df_filtered['web_name'].str.replace("."," ")
    fpl_df_filtered['merge_name'] = fpl_df_filtered['web_name'].str.split().str[-1].apply(unidecode)
    fpl_df_filtered = fpl_df_filtered[["singular_name", "chance_of_playing_this_round", "total_points", "points_per_game", "team_name","merge_name","web_name","first_name"]]

    # Create 'player' and 'ascii_player' columns for both DataFrames
    fbref_df['player_name'] = fbref_df['player'].str.split().str[-1].apply(unidecode)
    fbref_df['player_name_1'] = fbref_df['player'].str.split().str[0].apply(unidecode)
    fpl_df_filtered['player_name'] = fpl_df_filtered['merge_name'].apply(unidecode)
    fbref_df.loc[fbref_df['team_name'] == "Nott'ham_Forest", 'team_name'] = "Nottingham_Forest"

    # Merge on both 'player' and 'ascii_player'
    merged_df = pd.merge(fbref_df, fpl_df_filtered, on=['player_name','team_name'])
    merged_df1 = pd.merge(fbref_df, fpl_df_filtered, left_on=['player_name_1','team_name'], right_on=['player_name','team_name'])
    merged_df2 = pd.merge(fbref_df, fpl_df_filtered, left_on=['player_name','team_name'], right_on=['first_name','team_name'])
    merged_df = pd.concat([merged_df, merged_df1,merged_df2]).drop_duplicates()
    merged_df.drop(['merge_name', 'web_name', 'first_name', 'player_name_x', 'player_name', 'player_name_1', 'singular_name','player_name_y'], axis=1, inplace=True)

    return merged_df

def process_generic_data(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    team_list = get_team_list()
    df = process_concat_data('generic', team_list, **kwargs)
    local_file_path = get_local_file_path('generic', ds_nodash)
    local_file_path = os.path.expanduser(local_file_path)
    df.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, f'generic_{ds_nodash}.csv')

def process_shooting_data(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    team_list = get_team_list()
    df = process_concat_data('shooting', team_list, **kwargs)
    local_file_path = get_local_file_path('shooting', ds_nodash)
    local_file_path = os.path.expanduser(local_file_path)
    df.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, f'shooting_{ds_nodash}.csv')

def process_goalkeeper_data(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    team_list = get_team_list()
    df = process_concat_data('goalkeeper', team_list, **kwargs)
    local_file_path = get_local_file_path('goalkeeper', ds_nodash)
    local_file_path = os.path.expanduser(local_file_path)
    df.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, f'goalkeeper_{ds_nodash}.csv')

def process_passing_data(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    team_list = get_team_list()
    df = process_concat_data('passing', team_list, **kwargs)
    local_file_path = get_local_file_path('passing', ds_nodash)
    local_file_path = os.path.expanduser(local_file_path)
    df.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, f'passing_{ds_nodash}.csv')

def process_defensive_data(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    team_list = get_team_list()
    df = process_concat_data('defensive', team_list, **kwargs)
    local_file_path = get_local_file_path('defensive', ds_nodash)
    local_file_path = os.path.expanduser(local_file_path)
    df.to_csv(local_file_path, index=False)
    upload_to_s3(local_file_path, f'defensive_{ds_nodash}.csv')

def get_team_list():
    return ['Burnley', 'Arsenal', 'Bournemouth', 'Brighton', 'Everton', 'Sheffield_Utd', 'Newcastle_Utd', 'Brentford', 'Chelsea', 'Manchester_Utd', "Nottingham_Forest", 'Fulham', 'Liverpool', 'Wolves', 'Tottenham', 'Manchester_City', 'Aston_Villa', 'West_Ham', 'Crystal_Palace', 'Luton_Town']

def process_concat_data(data_type, team_list, **kwargs):
    return pd.concat([process_data(team, data_type, **kwargs) for team in team_list])

def get_local_file_path(data_type, ds_nodash):
    return f'~/fpl_analytics/fpldata_local/processed/{data_type}_{ds_nodash}.csv'



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
        s3.upload_file(file_name, bucket_name, f"processed_data/{object_name}")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")

with DAG(
    'process_fpl_data',
    default_args=default_args,
    description='A DAG to process FPL data',
    schedule_interval=None,
    start_date=datetime(2023, 10, 14),
    catchup=False,
) as dag:

    process_generic_data_task = PythonOperator(
        task_id='process_generic_data_task',
        python_callable=process_generic_data,
        provide_context=True
    )

    process_shooting_data_task = PythonOperator(
        task_id='process_shooting_data_task',
        python_callable=process_shooting_data,
        provide_context=True
    )

    process_goalkeeper_data_task = PythonOperator(
        task_id='process_goalkeeper_data_task',
        python_callable=process_goalkeeper_data,
        provide_context=True
    )

    process_passing_data_task = PythonOperator(
        task_id='process_passing_data_task',
        python_callable=process_passing_data,
        provide_context=True
    )

    process_defensive_data_task = PythonOperator(
        task_id='process_defensive_data_task',
        python_callable=process_defensive_data,
        provide_context=True
    )

    [process_generic_data_task,process_shooting_data_task,process_goalkeeper_data_task,process_passing_data_task,process_defensive_data_task]