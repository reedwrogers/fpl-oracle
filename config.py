"""Single source of truth for configuration and shared constants."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = Path(os.environ.get("FPL_ORACLE_DATA_DIR", BASE_DIR / "data"))
# Where the web assets (predictions.csv, metrics.json, squad.json) are written.
# Served by the project's own nginx container at fpl.reedrogers.xyz.
PUBLISH_DIR = Path(os.environ.get("FPL_ORACLE_PUBLISH_DIR", BASE_DIR / "site" / "data"))

FPL_API = "https://fantasy.premierleague.com/api"

# Understat labels a season by its start year.
SEASON = "2026"        # current / upcoming season
PRIOR_SEASON = "2025"  # last completed season (used pre-season / backfill)

# --- Name normalisation -----------------------------------------------------

# FPL team name -> Understat team name.
TEAM_MAP = {
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
    "Ipswich Town": "Ipswich",
    "Coventry City": "Coventry",
    "Hull City": "Hull",
}

# FPL player name -> Understat player name.
NAME_MAP = {
    "Alejandro Garnacho": "Alejandro Garnacho Ferreyra",
    "Alejandro Jiménez": "Alex Jiminez Sanchez",
    "Alisson": "Alisson Becker",
    "Altay Bayindir": "Altay Bayındır",
    "Amad Diallo Traore": "Amad Diallo",
    "André": "André Trindade da Costa Neto",
    "Ben White": "Benjamin White",
    "Bernardo Silva": "Bernardo Mota Veiga de Carvalho e Silva",
    "Bruno Guimarães": "Bruno Guimarães Rodriguez Moura",
    "Casemiro": "Carlos Henrique Casimiro",
    "Chimuanya Ugochukwu": "Lesley Ugochukwu",
    "Dan Ballard": "Daniel Ballard",
    "David Raya": "David Raya Martín",
    "Diego Gómez": "Diego Gómez Amarilla",
    "Diogo Dalot": "Diogo Dalot Teixeira",
    "Djordje Petrovic": "Đorđe Petrović",
    "Emiliano Martinez": "Emiliano Martínez Romero",
    "Evanilson": "Francisco Evanilson de Lima Barbosa",
    "Ferdi Kadioglu": "Ferdi Kadıoğlu",
    "Florentino Luís": "Florentino Ibrain Morris Luís",
    "Gabriel": "Gabriel dos Santos Magalhães",
    "Hugo Bueno": "Hugo Bueno López",
    "Igor Jesus": "Igor Jesus Maciel da Cruz",
    "Iyenoma Destiny Udogie": "Destiny Udogie",
    "Jair": "Jair Paula da Cunha Filho",
    "Joelinton": "Joelinton Cássio Apolinário de Lira",
    "John Victor": "John Victor Maciel Furtado",
    "Jorge Cuenca": "Jorge Cuenca Barreno",
    "José Sá": "José Malheiro de Sá",
    "João Gomes": "Gustavo Nunes Fernandes Gomes",
    "João Palhinha": "João Maria Lobo Alves Palhares Costa Palhinha Gonçalves",
    "João Pedro": "João Pedro Junqueira de Jesus",
    "Lucas Paquetá": "Lucas Tolentino Coelho de Lima",
    "Lucas Perri": "Lucas Estella Perri",
    "Lucas Pires": "Lucas Pires Silva",
    "Marc Cucurella": "Marc Cucurella Saseta",
    "Mateus Fernandes": "Mateus Gonçalo Espanha Fernandes",
    "Matheus Cunha": "Matheus Santos Carneiro da Cunha",
    "Matthew Cash": "Matty Cash",
    "Max Kilman": "Maximilian Kilman",
    "Moisés Caicedo": "Moisés Caicedo Corozo",
    "Morato": "Felipe Rodrigues Da Silva",
    "Murillo": "Murillo Costa dos Santos",
    "Naif Aguerd": "Nayef Aguerd",
    "Nico González": "Nico González Iglesias",
    "Oliver Scarles": "Ollie Scarles",
    "Pablo": "Pablo Felipe Pereira de Jesus",
    "Pedro Neto": "Pedro Lomba Neto",
    "Pedro Porro": "Pedro Porro Sauceda",
    "Raúl Jiménez": "Raúl Jiménez Rodríguez",
    "Reinildo": "Reinildo Mandava",
    "Richarlison": "Richarlison de Andrade",
    "Rodri": "Rodrigo 'Rodri'Hernandez Cascante",
    "Rúben Dias": "Rúben dos Santos Gato Alves Dias",
    "Santiago Bueno": "Santiago Ignacio Bueno",
    "Sasa Lukic": "Saša Lukić",
    "Thiago": "Igor Thiago Nascimento Rodrigues",
    "Toti": "Toti Gomes",
    "Valentino Livramento": "Tino Livramento",
    "Yeremi Pino": "Yéremy Pino Santos",
    "Yerson Mosquera": "Yerson Mosquera Valdelamar",
}

# --- Understat team lists ---------------------------------------------------

UNDERSTAT_TEAMS = [
    "Manchester City", "Arsenal", "Liverpool", "Aston Villa", "Tottenham",
    "Chelsea", "Newcastle United", "Manchester United", "Crystal Palace",
    "Brighton", "Bournemouth", "Fulham", "Everton", "Brentford",
    "Nottingham Forest", "Ipswich", "Coventry", "Hull", "Leeds", "Sunderland",
]

UNDERSTAT_TEAMS_PRIOR = [
    "Manchester City", "Arsenal", "Liverpool", "Aston Villa", "Tottenham",
    "Chelsea", "Newcastle United", "Manchester United", "West Ham",
    "Crystal Palace", "Brighton", "Bournemouth", "Fulham", "Wolverhampton Wanderers",
    "Everton", "Brentford", "Nottingham Forest", "Sunderland", "Burnley", "Leeds",
]

# --- Feature engineering knobs ----------------------------------------------

# Players no longer on any FPL team (left the league/club) and still listed in
# the API. Excluded from feature generation so they never enter predictions,
# training pairs, or the recommended squad.
EXCLUDED_PLAYERS = {"Enes Ünal"}

# Rookies/fringe players need at least one full match of minutes to be a real
# option (a single garbage-time minute still counts as >0 in the API).
MIN_TOTAL_MINUTES = 90

# Minimum % of minutes played to keep a player's Understat stats (drops players
# with too little playing time to trust their per-90 rates).
PLAYING_TIME_MIN_PCT = 60

# Fuzzy-match score threshold for mapping FPL -> Understat player names.
FUZZY_MATCH_THRESHOLD = 92

# FPL bootstrap normalizes ICT to per-90; a player with a few garbage-time
# minutes gets absurd per-90 values, so we winsorize at this quantile.
PER_90_CAP_QUANTILE = 0.99

PER_90_COLS = [
    "influence_per_90",
    "creativity_per_90",
    "threat_per_90",
    "ict_per_90",
]

# Canonical schema for every X_*.csv. Feature builders must produce exactly
# these columns, in this order.
FEATURE_COLUMNS = [
    "full_name", "team_name", "player_position", "current_fpl_cost",
    "playing_time_min_percentage", "xg_per_90", "xag_per_90",
    "yellows_per_90", "reds_per_90",
    "clearances_blocks_interceptions_per_90", "tackles_per_90",
    "team_xg_per_90", "team_xg_against_per_90",
    "opponent_xg_per_90", "opponent_xg_against_per_90", "opponent_league_position",
    "gameweek", "is_at_home", "team_league_position",
    "points_last_3", "xg_last_3", "minutes_last_3", "total_minutes",
    "is_penalty_taker", "opponent_goals_conceded_last_3", "ownership_percent",
    "influence_per_90", "creativity_per_90", "threat_per_90", "ict_per_90",
]
