import requests
import pandas as pd
from pathlib import Path
import pandas as pd
from datetime import datetime
import requests
from thefuzz import process
import warnings
import re
from understatapi import UnderstatClient

def get_gameweeks_seen(data_dir="/data"):
    """
    Returns a sorted list of gameweek numbers found as X_<gw>.csv in data_dir
    """
    pattern = re.compile(r"X_(\d+)\.csv$")
    gameweeks = []

    for path in Path(data_dir).iterdir():
        match = pattern.match(path.name)
        if match:
            gameweeks.append(int(match.group(1)))

    return sorted(gameweeks)

def get_next_gameweek():
    """
    Returns the next FPL gameweek number based on current date.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()

    events = pd.DataFrame(data["events"])

    next_gw = events.loc[events["is_next"], "id"]

    if next_gw.empty:
        raise ValueError("No upcoming gameweek found")

    return int(next_gw.iloc[0]) 

curr_gameweek = get_next_gameweek()

def get_fixtures(week_wanted):
    """
    Grabs the list of games for the specified gameweek from the FPL API.
    Returns DataFrame with home_team, away_team, and week columns.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()
    
    teams = pd.DataFrame(data["teams"])
    team_map = dict(zip(teams["id"], teams["name"]))
    
    fixtures_url = "https://fantasy.premierleague.com/api/fixtures/"
    fixtures_data = requests.get(fixtures_url).json()
    fixtures = pd.DataFrame(fixtures_data)
    
    fixtures = fixtures[fixtures["event"] == week_wanted]
    
    fixtures["home_team"] = fixtures["team_h"].map(team_map)
    fixtures["away_team"] = fixtures["team_a"].map(team_map)
    fixtures["week"] = fixtures["event"]
    
    return fixtures[["home_team", "away_team", "week"]]


def get_fpl_defensive_stats():
    """
    Gets tackles and clearances/blocks/interceptions per 90 for all FPL players from their season history.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    data = response.json()
    
    players = pd.DataFrame(data['elements'])
    
    defensive_stats = []
    
    for pid in players['id']:
        try:
            p_url = f"https://fantasy.premierleague.com/api/element-summary/{pid}/"
            p_data = requests.get(p_url).json()
            
            history = p_data.get("history", [])
            
            if not history:
                continue
            
            # Sum up stats across all gameweeks played
            total_minutes = sum(gw['minutes'] for gw in history)
            total_cbi = sum(gw.get('clearances_blocks_interceptions', 0) for gw in history)
            total_tackles = sum(gw.get('tackles', 0) for gw in history)
            
            if total_minutes > 0:
                cbi_per_90 = round((total_cbi / total_minutes) * 90, 2)
                tackles_per_90 = round((total_tackles / total_minutes) * 90, 2)
            else:
                cbi_per_90 = 0.0
                tackles_per_90 = 0.0
            
            # Get player name
            player_info = players[players['id'] == pid].iloc[0]
            full_name = f"{player_info['first_name']} {player_info['second_name']}"
            
            defensive_stats.append({
                'full_name': full_name,
                'clearances_blocks_interceptions_per_90': cbi_per_90,
                'tackles_per_90': tackles_per_90,
                'total_minutes': total_minutes
            })
            
        except Exception as e:
            print(f"Error processing player {pid}: {e}")
            continue
    
    return pd.DataFrame(defensive_stats)


def get_understat_player_stats(season='2025', pt_threshold=60):
    """
    grabs all player individual statistics that we want
    """
    with UnderstatClient() as understat:
        data = understat.league(league="EPL").get_player_data(season=season)
    
    df_understat = pd.DataFrame(data)
    
    numeric_cols = ['time', 'games', 'xG', 'xA', 'yellow_cards', 'red_cards']
    for col in numeric_cols:
        df_understat[col] = pd.to_numeric(df_understat[col], errors='coerce')
    
    df_understat['playing_time_min_percentage'] = ((df_understat['time'] / (df_understat['games'] * 90)) * 100).round(2)
    df_understat['xg_per_90'] = ((df_understat['xG'] / df_understat['time']) * 90).round(2)
    df_understat['xag_per_90'] = ((df_understat['xA'] / df_understat['time']) * 90).round(2)
    df_understat['yellows_per_90'] = ((df_understat['yellow_cards'] / df_understat['time']) * 90).round(2)
    df_understat['reds_per_90'] = ((df_understat['red_cards'] / df_understat['time']) * 90).round(2)

    df_understat = df_understat[df_understat['playing_time_min_percentage'] >= pt_threshold]
    
    name_mapping = {
        'Alejandro Garnacho': 'Alejandro Garnacho Ferreyra',
        'Alejandro Jiménez': 'Alex Jiminez Sanchez',
        'Alisson': 'Alisson Becker',
        'Altay Bayindir': 'Altay Bayındır',
        'Amad Diallo Traore': 'Amad Diallo',
        'André': 'André Trindade da Costa Neto',
        'Ben White': 'Benjamin White',
        'Bernardo Silva': 'Bernardo Mota Veiga de Carvalho e Silva',
        'Bruno Guimarães': 'Bruno Guimarães Rodriguez Moura',
        'Casemiro': 'Carlos Henrique Casimiro',
        'Chimuanya Ugochukwu': 'Lesley Ugochukwu',
        'Dan Ballard': 'Daniel Ballard',
        'David Raya': 'David Raya Martín',
        'Diego Gómez': 'Diego Gómez Amarilla',
        'Diogo Dalot': 'Diogo Dalot Teixeira',
        'Djordje Petrovic': 'Đorđe Petrović',
        'Emiliano Martinez': 'Emiliano Martínez Romero',
        'Evanilson': 'Francisco Evanilson de Lima Barbosa',
        'Ferdi Kadioglu': 'Ferdi Kadıoğlu',
        'Florentino Luís': 'Florentino Ibrain Morris Luís',
        'Gabriel': 'Gabriel dos Santos Magalhães',
        'Hugo Bueno': 'Hugo Bueno López',
        'Igor Jesus': 'Igor Jesus Maciel da Cruz',
        'Iyenoma Destiny Udogie': 'Destiny Udogie',
        'Jair': 'Jair Paula da Cunha Filho',
        'Joelinton': 'Joelinton Cássio Apolinário de Lira',
        'John Victor': 'John Victor Maciel Furtado',
        'Jorge Cuenca': 'Jorge Cuenca Barreno',
        'José Sá': 'José Malheiro de Sá',
        'João Gomes': 'Gustavo Nunes Fernandes Gomes',
        'João Palhinha': 'João Maria Lobo Alves Palhares Costa Palhinha Gonçalves',
        'João Pedro': 'João Pedro Junqueira de Jesus',
        'Lucas Paquetá': 'Lucas Tolentino Coelho de Lima',
        'Lucas Perri': 'Lucas Estella Perri',
        'Lucas Pires': 'Lucas Pires Silva',
        'Marc Cucurella': 'Marc Cucurella Saseta',
        'Mateus Fernandes': 'Mateus Gonçalo Espanha Fernandes',
        'Matheus Cunha': 'Matheus Santos Carneiro da Cunha',
        'Matthew Cash': 'Matty Cash',
        'Max Kilman': 'Maximilian Kilman',
        'Moisés Caicedo': 'Moisés Caicedo Corozo',
        'Morato': 'Felipe Rodrigues Da Silva',
        'Murillo': 'Murillo Costa dos Santos',
        'Naif Aguerd': 'Nayef Aguerd',
        'Nico González': 'Nico González Iglesias',
        'Oliver Scarles': 'Ollie Scarles',
        'Pablo': 'Pablo Felipe Pereira de Jesus',
        'Pedro Neto': 'Pedro Lomba Neto',
        'Pedro Porro': 'Pedro Porro Sauceda',
        'Raúl Jiménez': 'Raúl Jiménez Rodríguez',
        'Reinildo': 'Reinildo Mandava',
        'Richarlison': 'Richarlison de Andrade',
        'Rodri': "Rodrigo 'Rodri'Hernandez Cascante",
        'Rúben Dias': 'Rúben dos Santos Gato Alves Dias',
        'Santiago Bueno': 'Santiago Ignacio Bueno',
        'Sasa Lukic': 'Saša Lukić',
        'Thiago': 'Igor Thiago Nascimento Rodrigues',
        'Toti': 'Toti Gomes',
        'Valentino Livramento': 'Tino Livramento',
        'Yeremi Pino': 'Yéremy Pino Santos',
        'Yerson Mosquera': 'Yerson Mosquera Valdelamar'
    }
    
    df_understat['player_name'] = df_understat['player_name'].replace(name_mapping)
    
    return df_understat[['player_name','playing_time_min_percentage','xg_per_90','xag_per_90','yellows_per_90','reds_per_90']] 

def get_understat_teams(season="2026"):
    """
    Returns xG statistics for all Premier League teams for games up to today.
    """
    
    teams_data = []
    team_names = [
        'Manchester City', 'Arsenal', 'Liverpool', 'Aston Villa', 'Tottenham',
        'Chelsea', 'Newcastle United', 'Manchester United', 'West Ham',
        'Crystal Palace', 'Brighton', 'Bournemouth', 'Fulham', 'Wolverhampton Wanderers',
        'Everton', 'Brentford', 'Nottingham Forest', 'Sunderland', 'Burnley', 'Leeds'
    ]
    
    today = datetime.now()
    
    for team_name in team_names:
        try:
            with UnderstatClient() as understat:
                team_data = understat.team(team=team_name).get_match_data(season=season)
            
            df_team = pd.DataFrame(team_data)
            
            df_team['datetime'] = pd.to_datetime(df_team['datetime'])
            
            df_team = df_team[df_team['datetime'] < today]
            df_team = df_team[df_team['isResult'] == True]  
            
            if len(df_team) == 0:
                continue
            
            df_team['team_xg'] = df_team.apply(
                lambda row: float(row['xG']['h']) if row['side'] == 'h' else float(row['xG']['a']), 
                axis=1
            )
            df_team['team_xg_against'] = df_team.apply(
                lambda row: float(row['xG']['a']) if row['side'] == 'h' else float(row['xG']['h']), 
                axis=1
            )
            
            total_minutes = len(df_team) * 90
            
            team_xg_per_90 = (df_team['team_xg'].sum() / total_minutes * 90).round(2)
            team_xg_against_per_90 = (df_team['team_xg_against'].sum() / total_minutes * 90).round(2)
            
            teams_data.append({
                'team_name': team_name,
                'team_xg_per_90': team_xg_per_90,
                'team_xg_against_per_90': team_xg_against_per_90,
                'matches_played': len(df_team)
            })
            
        except Exception as e:
            print(f"Error processing {team_name}: {e}")
            continue
    
    return pd.DataFrame(teams_data)

def get_fpl_players():
    """
    Grabs a list of all FPL players
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
    
    return players_df

def fuzzy_match(fpl_df, understat_df, threshold=92):
    """
    Fuzzy matches FPL players to Understat player stats by name
    """
    
    understat_names = understat_df['player_name'].tolist()
    
    fpl_names = fpl_df['full_name'].tolist()

    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, understat_names)
        mapping[name] = match if score >= threshold else None

    fpl_df['matched_understat'] = fpl_df['full_name'].map(mapping)

    merged = fpl_df.merge(understat_df, left_on='matched_understat', right_on='player_name', how='inner')

    merged = merged.drop_duplicates(subset=['player_name'])

    return merged


def get_fpl_table():
    """Pulls current PL standings from the official FPL API."""
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()

    teams = pd.DataFrame(data["teams"])[["name", "short_name", "position"]]

    name_map = {
        "Manchester United": "Man Utd",
        "Manchester City": "Man City",
        "Tottenham Hotspur": "Spurs",
        "Tottenham": "Spurs",
        "Nott'ham Forest": "\"Nottham Forest\"",
        "Nottingham Forest": "\"Nottham Forest\"",
        "Nott'm Forest": "\"Nottham Forest\"",
        "Newcastle United": "Newcastle",
        "Leeds United": "Leeds",
        "Brighton and Hove Albion": "Brighton",
        "Wolverhampton Wanderers": "Wolves",
        "West Ham United": "West Ham",
        "Aston Villa": "Aston Villa",
        "Sheffield United": "Sheffield Utd",
    }

    teams["team_name"] = teams["name"].replace(name_map)

    return teams[["team_name", "position"]]

def get_fixtures_and_league_spots(gameweek=curr_gameweek):
    fixtures = get_fixtures(gameweek)
    
    name_map = {
        "Manchester United": "Man Utd",
        "Manchester City": "Man City",
        "Tottenham Hotspur": "Spurs",
        "Tottenham": "Spurs",
        "Nott'ham Forest": "\"Nottham Forest\"",
        "Nottingham Forest": "\"Nottham Forest\"",
        "Nott'm Forest": "\"Nottham Forest\"",
        "Newcastle United": "Newcastle",
        "Leeds United": "Leeds",
        "Brighton and Hove Albion": "Brighton",
        "Wolverhampton Wanderers": "Wolves",
        "West Ham United": "West Ham",
        "Aston Villa": "Aston Villa",
        "Sheffield United": "Sheffield Utd",
    }

    fixtures['home_team'] = fixtures['home_team'].replace(name_map)
    fixtures['away_team'] = fixtures['away_team'].replace(name_map)

    home_df = fixtures[["home_team", "week"]].rename(columns={"home_team": "team"})
    home_df["home"] = 1

    away_df = fixtures[["away_team", "week"]].rename(columns={"away_team": "team"})
    away_df["home"] = 0

    combined = pd.concat([home_df, away_df], ignore_index=True)

    standings = get_fpl_table()

    final = combined.merge(standings, left_on="team", right_on="team_name", how="left")
    final = final.drop(columns=["team_name"])

    return final.sort_values("position")

def join_it_all_together():
    TEAM_TEST_MAP = {
        "Manchester United": "Man Utd",
        "Manchester City": "Man City",
        "Tottenham Hotspur": "Spurs",
        "Tottenham": "Spurs",
        "Nott'ham Forest": "\"Nottham Forest\"",
        "Nottingham Forest": "\"Nottham Forest\"",
        "Nott'm Forest": "\"Nottham Forest\"",
        "Newcastle United": "Newcastle",
        "Leeds United": "Leeds",
        "Brighton and Hove Albion": "Brighton",
        "Wolverhampton Wanderers": "Wolves",
        "West Ham United": "West Ham",
        "Aston Villa": "Aston Villa",
        "Sheffield United": "Sheffield Utd",
    }

    df_fpl = get_fpl_players()
    df_understat = get_understat_player_stats()
    df_teams = get_understat_teams()
    df_defensive = get_fpl_defensive_stats()
    df_fuz = fuzzy_match(df_fpl, df_understat)

    df_fuz = df_fuz.merge(df_defensive, on='full_name', how='left')

    df_fuz["team_name"] = df_fuz["team_name"].replace(TEAM_TEST_MAP)
    df_teams["team_name"] = df_teams["team_name"].replace(TEAM_TEST_MAP)
    
    df = df_fuz.merge(df_teams, left_on="team_name", right_on="team_name", how="left")

    df_fix = get_fixtures_and_league_spots() 

    df = df.merge(df_fix, left_on='team_name', right_on='team', how='left')
    
    fixtures = get_fixtures(curr_gameweek)
    fixtures['home_team'] = fixtures['home_team'].replace(TEAM_TEST_MAP)
    fixtures['away_team'] = fixtures['away_team'].replace(TEAM_TEST_MAP)
    
    opponent_map = {}
    for _, row in fixtures.iterrows():
        opponent_map[row['home_team']] = row['away_team']
        opponent_map[row['away_team']] = row['home_team']
    
    df['opponent_team'] = df['team_name'].map(opponent_map)
    
    df = df.merge(
        df_teams[['team_name', 'team_xg_per_90', 'team_xg_against_per_90']],
        left_on='opponent_team',
        right_on='team_name',
        how='left',
        suffixes=('', '_opp')
    )
    
    standings = get_fpl_table()
    df = df.merge(
        standings[['team_name', 'position']],
        left_on='opponent_team',
        right_on='team_name',
        how='left',
        suffixes=('', '_opp_pos')
    )

    df = df.rename(columns={
        'team_name': 'team_name',
        'position_x': 'player_position',
        'now_cost': 'current_fpl_cost',
        'week': 'gameweek',
        'home': 'is_at_home',
        'position_y': 'team_league_position',
        'team_xg_per_90_opp': 'opponent_xg_per_90',
        'team_xg_against_per_90_opp': 'opponent_xg_against_per_90',
        'position': 'opponent_league_position'
    })
    
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df[['full_name', 'team_name', 'player_position', 'current_fpl_cost', 
               'playing_time_min_percentage', 'xg_per_90', 'xag_per_90',
               'yellows_per_90', 'reds_per_90', 
               'clearances_blocks_interceptions_per_90', 'tackles_per_90', 
               'team_xg_per_90', 'team_xg_against_per_90', 
               'opponent_xg_per_90', 'opponent_xg_against_per_90', 'opponent_league_position',
               'gameweek', 'is_at_home', 'team_league_position']]


def get_players_with_points(gameweek=curr_gameweek-1):
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

print("The current gameweek is: ",curr_gameweek)

gameweeks_seen = get_gameweeks_seen("/home/tars/Projects/fpl-oracle/data")

if curr_gameweek in gameweeks_seen:
    print("The gameweek has already been grabbed.")
else:
    # get the before gameweek data..
    df = join_it_all_together()
    df.to_csv(f'/home/tars/Projects/fpl-oracle/data/X_{curr_gameweek}.csv', index=False)

    # get the after last gameweek data... need to specifically put this in for now because we missed a GW
    if curr_gameweek != 24:
        df_ = get_players_with_points()
        X = pd.read_csv(f"/home/tars/Projects/fpl-oracle/data/X_{curr_gameweek-1}.csv")
        filtered = df_[df_['full_name'].isin(X['full_name'])]
        filtered.to_csv(f'/home/tars/Projects/fpl-oracle/data/y_{curr_gameweek-1}.csv', index=False)