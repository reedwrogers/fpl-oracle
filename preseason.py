#!/usr/bin/env python3
"""Generate pre-season X_gw.csv for any gameweek using 2025/26 Understat stats.

FPL 2026/27 player data is available (bootstrap-static).
Understat 2026/27 has no data (season hasn't started).
We use 2025/26 Understat end-of-season stats for returning players.
FPL-derived features (points_last_3, etc.) are zeroed out since no games played.

Usage:
  python preseason.py        # generate GWs 1, 2, 3
  python preseason.py 5      # generate GW 5 only
"""

import os
import sys
import pandas as pd
import numpy as np
from thefuzz import process
from understatapi import UnderstatClient
import requests

sys.path.insert(0, os.path.dirname(__file__))
from data_v3 import TEAM_TEST_MAP, get_fixtures, get_opponent_goals_conceded

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

UNDERSTAT_TEAMS_2025 = [
    "Manchester City", "Arsenal", "Liverpool", "Aston Villa", "Tottenham",
    "Chelsea", "Newcastle United", "Manchester United", "West Ham",
    "Crystal Palace", "Brighton", "Bournemouth", "Fulham", "Wolverhampton Wanderers",
    "Everton", "Brentford", "Nottingham Forest", "Sunderland", "Burnley", "Leeds",
]

NAME_MAPPING = {
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
    'Yerson Mosquera': 'Yerson Mosquera Valdelamar',
}


def load_shared_data():
    """Fetch FPL + Understat data once (same across all preseason GWs)."""
    print("Fetching 2026/27 FPL data...")
    bs = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
    fpl = pd.DataFrame(bs["elements"])
    teams = {t["id"]: t["name"] for t in bs["teams"]}
    positions = {p["id"]: p["singular_name"] for p in bs["element_types"]}
    fpl["team_name"] = fpl["team"].map(teams)
    fpl["player_position"] = fpl["element_type"].map(positions)
    fpl["full_name"] = fpl["first_name"] + " " + fpl["second_name"]
    fpl["current_fpl_cost"] = fpl["now_cost"]
    fpl["selected_by_percent"] = fpl["selected_by_percent"].astype(float)
    print(f"  {len(fpl)} players")

    print("Fetching 2025/26 Understat player data...")
    with UnderstatClient() as understat:
        u_data = understat.league(league="EPL").get_player_data(season="2025")
    u_df = pd.DataFrame(u_data)
    for col in ["time", "games", "xG", "xA", "yellow_cards", "red_cards"]:
        u_df[col] = pd.to_numeric(u_df[col], errors="coerce")
    u_df["playing_time_min_percentage"] = (
        (u_df["time"] / (u_df["games"] * 90)) * 100
    ).round(2)
    u_df["xg_per_90"] = ((u_df["xG"] / u_df["time"]) * 90).round(2)
    u_df["xag_per_90"] = ((u_df["xA"] / u_df["time"]) * 90).round(2)
    u_df["yellows_per_90"] = ((u_df["yellow_cards"] / u_df["time"]) * 90).round(2)
    u_df["reds_per_90"] = ((u_df["red_cards"] / u_df["time"]) * 90).round(2)
    u_df = u_df[u_df["playing_time_min_percentage"] >= 60]
    u_df["player_name"] = u_df["player_name"].replace(NAME_MAPPING)
    print(f"  {len(u_df)} players with >=60% minutes")

    print("Fetching 2025/26 Understat team xG data...")
    import data_v3
    _saved = data_v3.UNDERSTAT_TEAMS
    data_v3.UNDERSTAT_TEAMS = UNDERSTAT_TEAMS_2025
    df_teams = data_v3.get_understat_teams(season="2025")
    data_v3.UNDERSTAT_TEAMS = _saved

    df_teams["team_name"] = df_teams["team_name"].replace(TEAM_TEST_MAP)

    avg_xg = df_teams["team_xg_per_90"].mean()
    avg_xga = df_teams["team_xg_against_per_90"].mean()
    for t in ["Ipswich", "Coventry", "Hull"]:
        if t not in df_teams["team_name"].values:
            df_teams = pd.concat([
                df_teams,
                pd.DataFrame([{"team_name": t, "team_xg_per_90": avg_xg,
                               "team_xg_against_per_90": avg_xga}])
            ], ignore_index=True)

    print("Fetching opponent goals conceded (2025/26 season)...")
    df_goals = get_opponent_goals_conceded()
    df_goals["team_name"] = df_goals["team_name"].replace(TEAM_TEST_MAP)

    # Fuzzy match setup (reusable)
    u_names = u_df["player_name"].tolist()
    fpl_names = fpl["full_name"].tolist()
    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, u_names)
        mapping[name] = match if score >= 92 else None

    return {
        "fpl": fpl,
        "u_df": u_df,
        "u_names": u_names,
        "match_map": mapping,
        "teams": df_teams,
        "goals": df_goals,
    }


def build_preseason_x(gameweek, shared):
    """Build X_{gameweek}.csv using cached shared data + GW-specific fixtures."""
    fpl = shared["fpl"]
    u_df = shared["u_df"]
    teams_data = shared["teams"]
    goals_data = shared["goals"]

    df = fpl[["full_name", "team_name", "player_position", "current_fpl_cost",
              "selected_by_percent"]].copy()

    # Merge Understat
    df["_match"] = df["full_name"].map(shared["match_map"])
    df = df.merge(u_df, left_on="_match", right_on="player_name", how="left").drop(
        columns=["_match", "player_name"]
    )

    # Fill missing Understat stats with medians
    for col in ["playing_time_min_percentage", "xg_per_90", "xag_per_90",
                "yellows_per_90", "reds_per_90"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    df["team_name"] = df["team_name"].replace(TEAM_TEST_MAP)
    df = df.merge(teams_data, on="team_name", how="left")

    # GW-specific fixtures
    fixtures = get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(TEAM_TEST_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(TEAM_TEST_MAP)

    opponent_map = {}
    home_map = {}
    for _, row in fixtures.iterrows():
        opponent_map[row["home_team"]] = row["away_team"]
        opponent_map[row["away_team"]] = row["home_team"]
        home_map[row["home_team"]] = 1
        home_map[row["away_team"]] = 0

    df["opponent_team"] = df["team_name"].map(opponent_map)
    df["gameweek"] = gameweek
    df["is_at_home"] = df["team_name"].map(home_map).fillna(0).astype(int)

    # Opponent xG
    df = df.merge(
        teams_data[["team_name", "team_xg_per_90", "team_xg_against_per_90"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp"),
    )

    # Opponent goals conceded
    df = df.merge(
        goals_data[["team_name", "goals_conceded_last_3"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp_gc"),
    )

    df = df.rename(columns={
        "team_xg_per_90_opp": "opponent_xg_per_90",
        "team_xg_against_per_90_opp": "opponent_xg_against_per_90",
        "goals_conceded_last_3": "opponent_goals_conceded_last_3",
    })

    for col in ["opponent_xg_per_90", "opponent_xg_against_per_90",
                "opponent_goals_conceded_last_3"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # Zero out FPL-derived features (no games played yet)
    for col in ["points_last_3", "xg_last_3", "minutes_last_3", "is_penalty_taker",
                "influence", "creativity", "threat", "ict_index",
                "clearances_blocks_interceptions_per_90", "tackles_per_90",
                "team_league_position", "opponent_league_position"]:
        df[col] = 0

    df["ownership_percent"] = fpl["selected_by_percent"].astype(float)

    df = df.loc[:, ~df.columns.duplicated()]

    return df[[
        "full_name", "team_name", "player_position", "current_fpl_cost",
        "playing_time_min_percentage", "xg_per_90", "xag_per_90",
        "yellows_per_90", "reds_per_90",
        "clearances_blocks_interceptions_per_90", "tackles_per_90",
        "team_xg_per_90", "team_xg_against_per_90",
        "opponent_xg_per_90", "opponent_xg_against_per_90", "opponent_league_position",
        "gameweek", "is_at_home", "team_league_position",
        "points_last_3", "xg_last_3", "minutes_last_3",
        "is_penalty_taker", "opponent_goals_conceded_last_3", "ownership_percent",
        "influence", "creativity", "threat", "ict_index",
    ]]


if __name__ == "__main__":
    shared = load_shared_data()

    gws = [int(sys.argv[1])] if len(sys.argv) > 1 else [1, 2, 3]
    for gw in gws:
        print(f"\nBuilding pre-season X_{gw}.csv...")
        df = build_preseason_x(gw, shared)
        out_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
        df.to_csv(out_path, index=False)
        print(f"  Wrote {out_path} ({len(df)} players, {len(df.columns)} cols)")
