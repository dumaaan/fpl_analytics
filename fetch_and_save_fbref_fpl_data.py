import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import os

def get_tables(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'lxml')
    all_tables = soup.findAll("tbody")
    return all_tables

def get_team_links():
    tempo = get_tables('https://fbref.com/en/comps/9/Premier-League-Stats')
    team_data = {}
    for row in tempo[0].find_all("tr"):
        team_cell = row.find("td", {"data-stat": "team"})
        if team_cell and team_cell.a:
            team_name = team_cell.a.text
            team_link = 'https://fbref.com' + team_cell.a['href']
            team_data[team_name] = team_link
    return team_data

def rows_to_datafame(rows):
    headers = [th['data-stat'] for th in rows.find_all("tr")[0].select('th,td')]
    row_data = [[cell.a.get_text(strip=True) if cell.a else cell.get_text(strip=True) for cell in row.select("th, td")] for row in rows.find_all("tr")]
    return pd.DataFrame(row_data, columns=headers)

def save_fbref_files():
    indices_to_track = [0,3,4,5,8]
    team_data = get_team_links()
    save_path = os.path.expanduser('~/Downloads/testdata/')
    for k, v in team_data.items():
        team_raw = get_tables(v)
        dataframes = {index: rows_to_datafame(team_raw[index]) for index in indices_to_track}
        print(f"Saving file for team {k}")
        for idx, key in zip(indices_to_track, ['generic', 'goalkeeper', 'shooting', 'passing', 'defensive']):
            dataframes[idx].to_csv(os.path.join(save_path, f"{k.replace(' ', '_')}_{date.today().strftime('%Y%m%d')}_{key}.csv"))
    print("Saving fbref files is done.")

def save_and_update_fpl_data():
    # Get data from FPL
    base_url = 'https://fantasy.premierleague.com/api/'
    r = requests.get(base_url+'bootstrap-static/').json()

    # Process and save players data
    players = pd.json_normalize(r['elements'])
    positions = pd.json_normalize(r['element_types'])
    players = players.merge(positions, left_on='element_type', right_on='id')
    players.to_csv(os.path.expanduser(f'~/Downloads/testdata/players_data_{date.today().strftime("%Y%m%d")}.csv'), index=False)
    
    # Process and save teams data
    teams = pd.json_normalize(r['teams'])
    # Update and save teams_ready dataframe
    teams_ready =  pd.read_csv(os.path.expanduser('~/Downloads/epl-2023-GMTStandardTime.csv'))
    strength = teams[['id','name','strength']]
    strength = strength.replace("Nott'm Forest","Nottingham Forest")
    teams_ready = teams_ready.merge(strength, how='left', left_on='Home Team', right_on='name').merge(strength, how='left', left_on='Away Team', right_on='name')
    teams_ready = teams_ready.rename(columns={
        'id_x': 'Home_team_id',
        'name_x': 'Home_team_name',
        'strength_x': 'Home_team_strength',
        'id_y': 'Away_team_id',
        'name_y': 'Away_team_name',
        'strength_y': 'Away_team_strength'
    })
    teams_ready.to_csv(os.path.expanduser(f'~/Downloads/testdata/teams_ready_updated_{date.today().strftime("%Y%m%d")}.csv'), index=False)

if __name__ == '__main__':
    save_fbref_files()
    save_and_update_fpl_data()
