#!/usr/bin/env python3
"""Re-run the v3 pipeline for existing gameweeks 29-37 using fresh Understat/fixtures data.

FPL-derived features (points_last_3, minutes_last_3, ICT index, etc.) are
preserved from the existing X files since they were computed correctly during
the season.  Understat and fixture-derived features are recomputed from
currently-available APIs.
"""

import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from data_v3 import (
    TEAM_TEST_MAP,
    get_understat_player_stats,
    get_fixtures,
    get_opponent_goals_conceded,
    get_gameweeks_seen,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
# Columns that come from FPL APIs (already correct in existing files, keep as-is)
FPL_BASE_COLS = [
    "full_name", "team_name", "player_position", "current_fpl_cost",
    "points_last_3", "xg_last_3", "minutes_last_3",
    "is_penalty_taker", "ownership_percent",
    "influence", "creativity", "threat", "ict_index",
    "clearances_blocks_interceptions_per_90", "tackles_per_90",
    "gameweek", "is_at_home", "team_league_position", "opponent_league_position",
]

# Teams in the 2025/26 Premier League (Sunderland, Burnley, Leeds replaced by Ipswich, Coventry, Hull for 26/27)
UNDERSTAT_TEAMS_2025 = [
    "Manchester City", "Arsenal", "Liverpool", "Aston Villa", "Tottenham",
    "Chelsea", "Newcastle United", "Manchester United", "West Ham",
    "Crystal Palace", "Brighton", "Bournemouth", "Fulham", "Wolverhampton Wanderers",
    "Everton", "Brentford", "Nottingham Forest", "Sunderland", "Burnley", "Leeds",
]


def rebuild_x(gameweek: int) -> pd.DataFrame:
    """Rebuild X_{gw}.csv: keep FPL-derived cols from existing, refresh Understat + opponent stats."""
    prev_path = os.path.join(DATA_DIR, f"X_{gameweek}.csv")
    existing = pd.read_csv(prev_path)
    print(f"  Read {prev_path} ({len(existing)} rows)")

    df = existing[FPL_BASE_COLS].copy()

    df_understat = get_understat_player_stats(season="2025")

    # Temporarily use 2025/26 team list for Understat queries
    import data_v3
    _saved_teams = data_v3.UNDERSTAT_TEAMS
    data_v3.UNDERSTAT_TEAMS = UNDERSTAT_TEAMS_2025
    df_teams = data_v3.get_understat_teams(season="2025")
    data_v3.UNDERSTAT_TEAMS = _saved_teams

    df_goals_conceded = get_opponent_goals_conceded()

    # Fuzzy match Understat names to FPL names
    from thefuzz import process

    understat_names = df_understat["player_name"].tolist()
    fpl_names = df["full_name"].tolist()
    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, understat_names)
        mapping[name] = match if score >= 92 else None
    df["_match"] = df["full_name"].map(mapping)
    df = df.merge(
        df_understat,
        left_on="_match",
        right_on="player_name",
        how="inner",
    ).drop(columns=["_match", "player_name"])

    # Normalize team names
    df["team_name"] = df["team_name"].replace(TEAM_TEST_MAP)
    df_teams["team_name"] = df_teams["team_name"].replace(TEAM_TEST_MAP)
    df_goals_conceded["team_name"] = df_goals_conceded["team_name"].replace(TEAM_TEST_MAP)

    # Merge team xG
    df = df.merge(df_teams, on="team_name", how="left")

    # Compute opponent from fixtures
    fixtures = get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(TEAM_TEST_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(TEAM_TEST_MAP)
    opponent_map = {}
    for _, row in fixtures.iterrows():
        opponent_map[row["home_team"]] = row["away_team"]
        opponent_map[row["away_team"]] = row["home_team"]
    df["opponent_team"] = df["team_name"].map(opponent_map)

    # Merge opponent xG/xGA
    df = df.merge(
        df_teams[["team_name", "team_xg_per_90", "team_xg_against_per_90"]],
        left_on="opponent_team",
        right_on="team_name",
        how="left",
        suffixes=("", "_opp"),
    )

    # Merge opponent goals conceded
    df = df.merge(
        df_goals_conceded[["team_name", "goals_conceded_last_3"]],
        left_on="opponent_team",
        right_on="team_name",
        how="left",
        suffixes=("", "_opp_gc"),
    )

    # Rename opponent columns
    df = df.rename(columns={
        "team_xg_per_90_opp": "opponent_xg_per_90",
        "team_xg_against_per_90_opp": "opponent_xg_against_per_90",
        "goals_conceded_last_3": "opponent_goals_conceded_last_3",
    })

    df = df.loc[:, ~df.columns.duplicated()]

    result = df[[
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
    return result


def main():
    gameweeks = get_gameweeks_seen(DATA_DIR)
    print(f"Existing gameweeks in data/: {gameweeks}")

    # Pre-fetch shared data that doesn't change per gameweek
    print("Fetching Understat data (one-time)...")
    import data_v3
    _saved_teams = data_v3.UNDERSTAT_TEAMS
    data_v3.UNDERSTAT_TEAMS = UNDERSTAT_TEAMS_2025
    df_understat = get_understat_player_stats(season="2025")
    df_teams = data_v3.get_understat_teams(season="2025")
    data_v3.UNDERSTAT_TEAMS = _saved_teams

    df_teams["team_name"] = df_teams["team_name"].replace(TEAM_TEST_MAP)

    print("Fetching opponent goals conceded (one-time)...")
    df_goals_conceded = get_opponent_goals_conceded()
    df_goals_conceded["team_name"] = df_goals_conceded["team_name"].replace(TEAM_TEST_MAP)

    for gw in gameweeks:
        print(f"\nRebuilding GW {gw}...")
        x_df = rebuild_x_single(gw, df_understat, df_teams, df_goals_conceded)
        out_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
        x_df.to_csv(out_path, index=False)
        print(f"  Wrote {out_path} ({len(x_df)} players, {len(x_df.columns)} cols)")


def rebuild_x_single(gameweek, df_understat, df_teams, df_goals_conceded):
    """Rebuild a single X file using pre-fetched shared data."""
    prev_path = os.path.join(DATA_DIR, f"X_{gameweek}.csv")
    existing = pd.read_csv(prev_path)
    print(f"  Read {prev_path} ({len(existing)} rows)")

    df = existing[FPL_BASE_COLS].copy()

    from thefuzz import process

    understat_names = df_understat["player_name"].tolist()
    fpl_names = df["full_name"].tolist()
    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, understat_names)
        mapping[name] = match if score >= 92 else None
    df["_match"] = df["full_name"].map(mapping)
    df = df.merge(
        df_understat,
        left_on="_match",
        right_on="player_name",
        how="inner",
    ).drop(columns=["_match", "player_name"])

    df["team_name"] = df["team_name"].replace(TEAM_TEST_MAP)

    # Merge team xG
    df = df.merge(df_teams, on="team_name", how="left")

    # Compute opponent from per-GW fixtures
    fixtures = get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(TEAM_TEST_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(TEAM_TEST_MAP)
    opponent_map = {}
    for _, row in fixtures.iterrows():
        opponent_map[row["home_team"]] = row["away_team"]
        opponent_map[row["away_team"]] = row["home_team"]
    df["opponent_team"] = df["team_name"].map(opponent_map)

    # Merge opponent xG/xGA
    df = df.merge(
        df_teams[["team_name", "team_xg_per_90", "team_xg_against_per_90"]],
        left_on="opponent_team",
        right_on="team_name",
        how="left",
        suffixes=("", "_opp"),
    )

    # Merge opponent goals conceded
    df = df.merge(
        df_goals_conceded[["team_name", "goals_conceded_last_3"]],
        left_on="opponent_team",
        right_on="team_name",
        how="left",
        suffixes=("", "_opp_gc"),
    )

    df = df.rename(columns={
        "team_xg_per_90_opp": "opponent_xg_per_90",
        "team_xg_against_per_90_opp": "opponent_xg_against_per_90",
        "goals_conceded_last_3": "opponent_goals_conceded_last_3",
    })

    df = df.loc[:, ~df.columns.duplicated()]

    result = df[[
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
    return result


if __name__ == "__main__":
    main()
