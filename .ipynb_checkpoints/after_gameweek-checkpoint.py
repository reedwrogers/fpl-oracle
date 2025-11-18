import soccerdata as sd
import pandas as pd
from datetime import datetime
import requests
from thefuzz import process
import warnings

curr_gameweek = 12

def get_players_with_points(gameweek=curr_gameweek):
    """
    Returns FPL players + their total points for a specific gameweek.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    data = response.json()

    players = pd.DataFrame(data['elements'])
    teams = {team['id']: team['name'] for team in data['teams']}
    players['team_name'] = players['team'].map(teams)

    positions = {pos['id']: pos['singular_name'] for pos in data['element_types']}
    players['position'] = players['element_type'].map(positions)

    players_df = players[['id', 'first_name', 'second_name', 'team_name', 'position', 'now_cost']].copy()
    players_df['full_name'] = players_df['first_name'] + " " + players_df['second_name']

    points_list = []
    for pid in players_df['id']:
        p_url = f"https://fantasy.premierleague.com/api/element-summary/{pid}/"
        p_data = requests.get(p_url).json()

        history = p_data.get("history", [])
        gw_record = next((gw for gw in history if gw["round"] == gameweek), None)

        points_list.append(gw_record["total_points"] if gw_record else 0)

    players_df["gw_points"] = points_list
    
    return players_df[['full_name','gw_points']]

df_ = get_players_with_points()
X = pd.read_csv(f"/home/tars/Projects/fpl-oracle/X_{curr_gameweek}.csv")
filtered = df_[df_['full_name'].isin(X['full_name'])]

filtered.to_csv(f'/home/tars/Projects/fpl-oracle/data/y_{curr_gameweek}.csv', index=False)