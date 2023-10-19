from datetime import date
import logging
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from unidecode import unidecode


BASE_PATH = '~/fpl_analytics/fpldata_local/'
SPREADSHEET_ID = '14DvmMG8WVFc2EXddOdrImpjdX7jjKOJe3XhZQjZ3g3Y'


def initialize_sheets_api():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds_file = os.path.expanduser('~/fpl_analytics/optical-branch-230209-9ceda70fb8b8.json')
    credentials = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    return gspread.authorize(credentials)


def upload_to_google_sheet(df, sheet_name):
    client = initialize_sheets_api()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(sheet_name)
    worksheet.clear()
    worksheet.append_row(df.columns.tolist())
    worksheet.append_rows(df.values.tolist())


def load_dataframe(file_path):
    return pd.read_csv(os.path.expanduser(file_path))

def preprocess_team_df(team_df):
    team_df = team_df[['Home Team', 'Home_team_id']].drop_duplicates()
    team_df.columns = ['team_name', 'team_id']
    team_df['team_name'] = team_df['team_name'].str.replace(" ", "_")
    return team_df

def preprocess_fpl_df(fpl_df, team_df):
    fpl_df = fpl_df.merge(team_df, left_on='team', right_on='team_id')
    fpl_df = fpl_df[fpl_df.status != 'u']
    fpl_df['web_name'] = fpl_df['web_name'].str.replace(".", " ")
    fpl_df['merge_name'] = fpl_df['web_name'].str.split().str[-1].apply(unidecode)
    fpl_df['player_name'] = fpl_df['merge_name'].apply(unidecode)
    fpl_df['now_cost'] = fpl_df['now_cost'] / 10
    return fpl_df[["singular_name", "chance_of_playing_this_round", "total_points", "points_per_game","now_cost","clean_sheets","goals_conceded","expected_goals_conceded","expected_goals_conceded_per_90","ict_index", "team_name", "merge_name", "web_name", "first_name","player_name"]]

def preprocess_fbref_df(fbref_df):
    fbref_df['player_name'] = fbref_df['player'].str.split().str[-1].apply(unidecode)
    fbref_df['player_name_1'] = fbref_df['player'].str.split().str[0].apply(unidecode)
    fbref_df.loc[fbref_df['team_name'] == "Nott'ham_Forest", 'team_name'] = "Nottingham_Forest"
    return fbref_df

def merge_dataframes(fbref_df, fpl_df):
    merge_on_team_player = pd.merge(fbref_df, fpl_df, on=['player_name', 'team_name'])
    merge_on_team_player_1 = pd.merge(fbref_df, fpl_df, left_on=['player_name_1', 'team_name'], right_on=['player_name', 'team_name'])
    merge_on_team_first_name = pd.merge(fbref_df, fpl_df, left_on=['player_name', 'team_name'], right_on=['first_name', 'team_name'])
    
    merged_df = pd.concat([merge_on_team_player, merge_on_team_player_1, merge_on_team_first_name]).drop_duplicates()
    merged_df.drop(['merge_name', 'web_name', 'first_name', 'player_name_x', 'player_name', 'player_name_1', 'position', 'player_name_y'], axis=1, inplace=True)
    
    return merged_df

def process_data(team_name, stats):
    ds_nodash = date.today().strftime('%Y%m%d')
    logging.info("Processing data...")

    fbref_file_path = "~/fpl_analytics/fpldata_local/{}_{}_{}.csv".format(team_name, ds_nodash, stats)
    fpl_file_path = "~/fpl_analytics/fpldata_local/players_data_{}.csv".format(ds_nodash)
    team_path = "~/fpl_analytics/fpldata_local/teams_ready_updated_{}.csv".format(ds_nodash)

    fbref_df = load_dataframe(fbref_file_path)
    fpl_df = load_dataframe(fpl_file_path)
    team_df = load_dataframe(team_path)

    team_df = preprocess_team_df(team_df)
    fpl_df = preprocess_fpl_df(fpl_df, team_df)
    fbref_df = preprocess_fbref_df(fbref_df)

    return merge_dataframes(fbref_df, fpl_df)

def process_data_type(data_type):
    ds_nodash = date.today().strftime('%Y%m%d')
    team_list = get_team_list()
    df = pd.concat([process_data(team, data_type) for team in team_list])
    df.fillna(0, inplace=True)
    df.drop(['nationality','age','matches','chance_of_playing_this_round'], axis=1, inplace=True)
    local_file_path = os.path.expanduser(f"{BASE_PATH}processed/{data_type}_{ds_nodash}.csv")
    df.drop_duplicates(inplace=True)
    df.to_csv(local_file_path, index=False)
    to_exclude = ((df['player'] == 'Gabriel Martinelli') & (df['singular_name'] != 'Midfielder')) \
                | ((df['player'] == 'Toti Gomes') & (df['singular_name'] != 'Defender')) \
                | ((df['player'] == 'Santiago Bueno') & (df['singular_name'] != 'Defender')) \
                | ((df['player'] == 'Toti Gomes') & (df['singular_name'] != 'Defender')) \
                | ((df['player'] == 'Hugo Bueno') & (df['total_points'] == 0)) \
                | ((df['player'] == 'Adam Davies') & (df['singular_name'] != 'Goalkeeper')) \
                | ((df['player'] == 'Gabriel Jesus') & (df['singular_name'] != 'Forward')) \
                | ((df['player'] == 'Jacob Murphy') & (df['singular_name'] != 'Midfielder')) \
                | ((df['player'] == 'Tom Davies') & (df['singular_name'] != 'Midfielder'))
    df = df[~to_exclude]
    upload_to_google_sheet(df, data_type)


def get_team_list():
    return ['Burnley', 'Arsenal', 'Bournemouth', 'Brighton', 'Everton', 'Sheffield_Utd', 'Newcastle_Utd',
            'Brentford', 'Chelsea', 'Manchester_Utd', "Nottingham_Forest", 'Fulham', 'Liverpool', 'Wolves',
            'Tottenham', 'Manchester_City', 'Aston_Villa', 'West_Ham', 'Crystal_Palace', 'Luton_Town']


if __name__ == '__main__':
    for data_type in ['generic', 'shooting', 'goalkeeper', 'defensive', 'passing']:
        process_data_type(data_type)