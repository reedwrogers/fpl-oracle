import soccerdata as sd
import pandas as pd
from datetime import datetime
import requests
from thefuzz import process
import warnings

curr_gameweek = 12

warnings.simplefilter(action='ignore', category=FutureWarning)

def get_fixtures(week_wanted):
    """
    grabs the list of games for the week, extracts only the cleaned team names of home and away team, as well as match_week, 
    """
    fbref = sd.FBref(leagues='ENG-Premier League', seasons='2025-2026')
    schedule = fbref.read_schedule()
    schedule['date'] = pd.to_datetime(schedule['date'], errors='coerce')
    schedule = schedule[schedule['week'] == week_wanted]

    return schedule[['home_team','away_team','week']]


def get_fbref_player_stats(season='2025-2026',pt_threshold=40):
    """
    grabs all player individual statistics that we want
    """
    fbref = sd.FBref('ENG-Premier League', season)

    standard = fbref.read_player_season_stats(stat_type="standard")
    shooting = fbref.read_player_season_stats(stat_type="shooting")
    passing = fbref.read_player_season_stats(stat_type="passing")
    defense = fbref.read_player_season_stats(stat_type="defense")
    playing_time = fbref.read_player_season_stats(stat_type="playing_time")

    def flatten_cols(df):
        df = df.copy()
        df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df.columns.values]
        return df

    standard = flatten_cols(standard)
    shooting = flatten_cols(shooting)
    passing = flatten_cols(passing)
    defense = flatten_cols(defense)
    playing_time = flatten_cols(playing_time)

    for df in [standard, shooting, passing, defense, playing_time]:
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'player'}, inplace=True)

    metadata_cols = ['season', 'league', 'team', 'nation_', 'pos_', 'age_', 'born_']
    for df in [standard, shooting, passing, defense]:
        df.drop(columns=[c for c in metadata_cols if c in df.columns], inplace=True)

    fbref_stats = standard
    for df in [shooting, passing, defense, playing_time]:
        fbref_stats = fbref_stats.merge(df, on='player', how='outer')

    
    fbref_stats['Tackles_Tkl_per90'] = fbref_stats['Tackles_Tkl'] / fbref_stats['Playing Time_90s_y']
    fbref_stats['Blocks_Blocks_per90'] = fbref_stats['Blocks_Blocks'] / fbref_stats['Playing Time_90s_y']
    fbref_stats['yellow_per90'] = fbref_stats['Performance_CrdY'] / fbref_stats['Playing Time_90s_y']
    fbref_stats['red_per90'] = fbref_stats['Performance_CrdR'] / fbref_stats['Playing Time_90s_y']

    fbref_stats = fbref_stats[fbref_stats['Playing Time_Min%'] >= pt_threshold]

    name_map = {
        "Alisson": "Alisson Becker",
        "André": "André Trindade da Costa Neto",
        "Benjamin Šeško": "Benjamin Sesko",
        "Bernardo Silva": "Bernardo Mota Veiga de Carvalho e Silva",
        "Beto": "Norberto Bercique Gomes Betuncal",
        "Bruno Guimarães": "Bruno Guimarães Rodriguez Moura",
        "Casemiro": "Carlos Henrique Casimiro",
        "David Raya": "David Raya Martín",
        "Diego Gómez": "Diego Gómez Amarilla",
        "Diogo Dalot": "Diogo Dalot Teixeira",
        "Emi Buendía": "Emiliano Buendía Stati",
        "Evanilson": "Francisco Evanilson de Lima Barbosa",
        "Ezri Konsa": "Ezri Konsa Ngoyo",
        "Ferdi Kadioglu": "Ferdi Kadıoğlu",
        "Florentino Luís": "Florentino Ibrain Morris Luís",
        "Gabriel Magalhães": "Gabriel dos Santos Magalhães",
        "Hugo Bueno": "Hugo Bueno López",
        "Jeremy Doku": "Jérémy Doku",
        "Joelinton": "Joelinton Cássio Apolinário de Lira",
        "Joshua King": "Josh King",
        "João Gomes": "Gustavo Nunes Fernandes Gomes",
        "João Palhinha": "João Maria Lobo Alves Palhares Costa Palhinha Gonçalves",
        "João Pedro": "João Pedro Junqueira de Jesus",
        "Lucas Paquetá": "Lucas Tolentino Coelho de Lima", 
        "Lucas Perri": "Lucas Estella Perri",
        "Marc Cucurella": "Marc Cucurella Saseta",
        "Mateus Fernandes": "Mateus Gonçalo Espanha Fernandes",
        "Matheus Cunha": "Matheus Santos Carneiro da Cunha",
        "Max Kilman": "Maximilian Kilman",
        "Moisés Caicedo": "Moisés Caicedo Corozo",
        "Morato": "Felipe Rodrigues Da Silva",
        "Murillo": "Murillo Costa dos Santos",
        "Nicolás González": "Nico González Iglesias",
        "Pedro Neto": "Pedro Lomba Neto",
        "Pedro Porro": "Pedro Porro Sauceda",
        "Raúl Jiménez": "Raúl Jiménez Rodríguez",
        "Richarlison": "Richarlison de Andrade",
        "Rúben Dias": "Rúben dos Santos Gato Alves Dias",
        "Santiago Bueno": "Santiago Ignacio Bueno",
        "Thiago": "Igor Thiago Nascimento Rodrigues",
        "Valentino Livramento": "Tino Livramento",
        "Yeremi Pino": "Yéremy Pino Santos",
        "Álex Jiménez": "Álex Jiménez Sánchez"
    }

    fbref_stats['player'] = fbref_stats['player'].apply(lambda x: name_map.get(x, x))

    return fbref_stats[['player','Playing Time_Min%','Per 90 Minutes_xG','Per 90 Minutes_xAG','Tackles_Tkl_per90','Blocks_Blocks_per90','yellow_per90','red_per90']]
    
def get_teams(season="2025-2026"):
    fbref = sd.FBref("ENG-Premier League", season)
    
    teams = fbref.read_team_season_stats(stat_type="shooting").index.tolist()
    
    data = []
    today = pd.Timestamp(datetime.today().date())
    
    for team in teams:
        matches = fbref.read_team_match_stats(stat_type="schedule", team=team)
        
        matches = matches[pd.to_datetime(matches["date"]) <= today]
        
        if matches.empty:
            continue
        
        xG_for_per90 = matches["xG"].sum() / len(matches)
        xG_against_per90 = matches["xGA"].sum() / len(matches)
        
        data.append({
            "team": team,
            "xG_for_per90": xG_for_per90,
            "xG_against_per90": xG_against_per90
        })

    df = pd.DataFrame(data)
    
    df['team'] = df['team'].astype(str).str.replace(r'^.*,\s*(.*?)\)$', r'\1', regex=True)

    df['team'] = df['team'].str.replace("'", "").str.strip()

    name_map = {
    "Manchester Utd": "Man Utd",
    "Manchester City": "Man City",
    "Tottenham": "Spurs",
    "Nott'ham Forest": "Nott'm Forest",
    "Newcastle Utd": "Newcastle",
    "Leeds United" : "Leeds"}
    
    df['team_name'] = df['team'].replace(name_map)

    return df


def get_players():
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


def fuzzy_match(fpl_df, fbref_df, threshold=92):
    """
    Fuzzy matches FPL players (already mapped) to FBref player stats by name
    """
    
    fbref_names = fbref_df['player'].tolist()
    
    fpl_names = fpl_df['full_name'].tolist()

    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, fbref_names)
        mapping[name] = match if score >= threshold else None

    fpl_df['matched_fbref'] = fpl_df['full_name'].map(mapping)

    merged = fpl_df.merge(fbref_df, left_on='matched_fbref', right_on='player', how='inner')

    merged = merged.drop_duplicates(subset=['player'])

    return merged

import pandas as pd
import requests

def get_fpl_table():
    """Pulls current PL standings from the official FPL API."""
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()

    teams = pd.DataFrame(data["teams"])[["name", "short_name", "position"]]

    # Normalize names to match your naming scheme
    name_map = {
        "Manchester United": "Man Utd",
        "Manchester City": "Man City",
        "Tottenham Hotspur": "Spurs",
        "Nottingham Forest": "Nott'm Forest",
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
        "Manchester Utd": "Man Utd",
        "Manchester City": "Man City",
        "Tottenham": "Spurs",
        "Nott'ham Forest": "Nott'm Forest",
        "Newcastle Utd": "Newcastle",
        "Leeds United": "Leeds"
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
    df_fpl = get_players()
    df_fbref = get_fbref_player_stats()
    df_teams = get_teams()
    df_fuz = fuzzy_match(df_fpl,df_fbref)
    df = df_fuz.merge(df_teams,left_on="team_name",right_on="team",how="left")

    df_fix = get_fixtures_and_league_spots() 

    df = df.merge(df_fix,on='team',how='left')

    df = df.rename(columns={'team':'team_name','position_x':'player_position','now_cost':'current_fpl_cost',
                        'Playing Time_Min%':'playing_time_min_percentage','Per 90 Minutes_xG':'xg_per_90',
                       'Per 90 Minutes_xAG':'xag_per_90','Tackles_Tkl_per90':'tackles_per_90','Blocks_Blocks_per90':'blocks_per_90',
                       'yellow_per90':'yellows_per_90','red_per90':'reds_per_90','xG_for_per90':'team_xg_per_90',
                       'xG_against_per90':'team_xg_against_per_90','week':'gameweek','home':'is_at_home',
                       'position_y':'team_league_position'})
    
    return df[['full_name','team_name','player_position','current_fpl_cost','playing_time_min_percentage','xg_per_90','xag_per_90','blocks_per_90',
          'yellows_per_90','reds_per_90','team_xg_per_90','team_xg_against_per_90','gameweek','is_at_home','team_league_position']]


df = join_it_all_together()

df.to_csv(f'/home/tars/Projects/fpl-oracle/data/X_{curr_gameweek}.csv', index=False)