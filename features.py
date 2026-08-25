"""Feature assembly — combine FPL + Understat sources into the X schema."""

import numpy as np
import pandas as pd
from thefuzz import process

import config
import fpl
import understat


def cap_per_90_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Winsorize each per-90 ICT column at its live quantile to tame outliers
    from players with very few minutes. Mutates and returns df."""
    for col in config.PER_90_COLS:
        if col in df.columns:
            values = df[col].dropna()
            if len(values) == 0:
                continue
            cap = values.quantile(config.PER_90_CAP_QUANTILE)
            df[col] = df[col].clip(upper=cap)
    return df


_ICT_COLS = ["influence_per_90", "creativity_per_90", "threat_per_90", "ict_per_90"]


def _last_season_ict_lookup() -> dict:
    """Per-player ICT from the most recent prior-season X file (keyed by
    full_name). Used to bridge the gap until the current season's ICT is
    populated by the FPL API."""
    files = sorted(
        (p for p in config.DATA_DIR.glob("X_*.csv")),
        key=lambda p: int(p.stem.split("_")[1]),
    )
    if not files:
        return {}
    df = pd.read_csv(files[-1])
    cols = [c for c in _ICT_COLS if c in df.columns]
    if "full_name" not in df.columns or not cols:
        return {}
    return df.set_index("full_name")[cols].to_dict("index")


def _fill_ict_from_prior(df: pd.DataFrame) -> pd.DataFrame:
    """Replace zero (not-yet-populated) current-season ICT with the most recent
    prior-season values. Non-zero (real) current values are left untouched."""
    lookup = _last_season_ict_lookup()
    if not lookup:
        return df
    for col in _ICT_COLS:
        if col not in df.columns:
            continue
        prior = df["full_name"].map(lambda n: lookup.get(n, {}).get(col, np.nan))
        df[col] = df[col].where(df[col] != 0, prior)
    return df


def _match_map(fpl_names, understat_names) -> dict:
    """Fuzzy-match each FPL name to its closest Understat name (or None)."""
    mapping = {}
    for name in fpl_names:
        if pd.isna(name):
            mapping[name] = None
            continue
        match, score = process.extractOne(name, understat_names)
        mapping[name] = match if score >= config.FUZZY_MATCH_THRESHOLD else None
    return mapping


def _fuzzy_join(df_fpl: pd.DataFrame, df_understat: pd.DataFrame, how: str) -> pd.DataFrame:
    df = df_fpl.copy()
    df["_match"] = df["full_name"].map(
        _match_map(df["full_name"].tolist(), df_understat["player_name"].tolist())
    )
    merged = df.merge(df_understat, left_on="_match", right_on="player_name", how=how)
    merged = merged.drop(columns=["_match"])
    if how == "inner":
        merged = merged.drop_duplicates(subset=["player_name"])
    return merged


def _add_opponent_features(df, df_teams, df_goals_conceded, gameweek) -> pd.DataFrame:
    fixtures = fpl.get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(config.TEAM_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(config.TEAM_MAP)

    opponent_map = {}
    for _, row in fixtures.iterrows():
        opponent_map[row["home_team"]] = row["away_team"]
        opponent_map[row["away_team"]] = row["home_team"]

    df["opponent_team"] = df["team_name"].map(opponent_map)

    df = df.merge(
        df_teams[["team_name", "team_xg_per_90", "team_xg_against_per_90"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp"),
    )
    df = df.merge(
        df_goals_conceded[["team_name", "goals_conceded_last_3"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp_gc"),
    )
    return df


def build_features(gameweek: int, season: str = config.SEASON) -> pd.DataFrame:
    """Assemble the full feature matrix for a gameweek (in-season)."""
    df_fpl = fpl.get_players()
    df_understat = understat.get_player_stats(season)
    if df_understat.empty:
        raise ValueError(f"Understat has no player data for season {season}")

    df_teams = understat.get_team_stats(season)
    df_defensive = fpl.get_defensive_stats()
    df_recent = fpl.get_recent_stats()
    df_goals_conceded = fpl.get_opponent_goals_conceded()

    df = _fuzzy_join(df_fpl, df_understat, how="inner")
    df = df.merge(df_defensive, on="full_name", how="left")
    df = df.merge(df_recent, on="full_name", how="left")

    df["team_name"] = df["team_name"].replace(config.TEAM_MAP)
    df_teams["team_name"] = df_teams["team_name"].replace(config.TEAM_MAP)
    df_goals_conceded["team_name"] = df_goals_conceded["team_name"].replace(config.TEAM_MAP)

    df = df.merge(df_teams, on="team_name", how="left")

    df_fix = fpl.get_fixtures_and_league_spots(gameweek)
    df = df.merge(df_fix, left_on="team_name", right_on="team", how="left")

    df = _add_opponent_features(df, df_teams, df_goals_conceded, gameweek)

    standings = fpl.get_standings()
    df = df.merge(
        standings[["team_name", "position"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp_pos"),
    )

    df = df.rename(columns={
        "position_x": "player_position",
        "now_cost": "current_fpl_cost",
        "week": "gameweek",
        "home": "is_at_home",
        "position_y": "team_league_position",
        "team_xg_per_90_opp": "opponent_xg_per_90",
        "team_xg_against_per_90_opp": "opponent_xg_against_per_90",
        "position": "opponent_league_position",
        "goals_conceded_last_3": "opponent_goals_conceded_last_3",
    })

    df = df.loc[:, ~df.columns.duplicated()]
    df = df[~df["full_name"].isin(config.EXCLUDED_PLAYERS)].copy()
    if "total_minutes" in df.columns:
        df = df[df["total_minutes"] >= config.MIN_TOTAL_MINUTES]
    df = _fill_ict_from_prior(df)
    df = cap_per_90_outliers(df)

    return df[config.FEATURE_COLUMNS]


def _load_preseason_shared() -> dict:
    """Fetch and pre-process shared pre-season data (FPL + prior-season Understat)."""
    print("Fetching FPL data...")
    bs = fpl.bootstrap()
    fpl_df = pd.DataFrame(bs["elements"])
    teams = {t["id"]: t["name"] for t in bs["teams"]}
    positions = {p["id"]: p["singular_name"] for p in bs["element_types"]}

    fpl_df["team_name"] = fpl_df["team"].map(teams)
    fpl_df["player_position"] = fpl_df["element_type"].map(positions)
    fpl_df["full_name"] = fpl_df["first_name"] + " " + fpl_df["second_name"]
    fpl_df["current_fpl_cost"] = fpl_df["now_cost"]
    fpl_df["selected_by_percent"] = fpl_df["selected_by_percent"].astype(float)

    minutes = fpl_df["minutes"].astype(float)
    fpl_df["total_minutes"] = minutes
    per_90 = np.where(minutes > 0, 90.0 / minutes, np.nan)
    fpl_df["influence_per_90"] = fpl_df["influence"].astype(float) * per_90
    fpl_df["creativity_per_90"] = fpl_df["creativity"].astype(float) * per_90
    fpl_df["threat_per_90"] = fpl_df["threat"].astype(float) * per_90
    fpl_df["ict_per_90"] = fpl_df["ict_index"].astype(float) * per_90
    fpl_df["is_penalty_taker"] = fpl_df["penalties_order"].isin([1.0, 2.0]).astype(int)

    print("Fetching prior-season Understat player data...")
    u_df = understat.get_player_stats(season=config.PRIOR_SEASON)

    print("Fetching prior-season Understat team xG data...")
    df_teams = understat.get_team_stats(
        season=config.PRIOR_SEASON, teams=config.UNDERSTAT_TEAMS_PRIOR
    )
    df_teams["team_name"] = df_teams["team_name"].replace(config.TEAM_MAP)

    # Promoted teams have no prior-season Understat data; use league average.
    avg_xg = df_teams["team_xg_per_90"].mean()
    avg_xga = df_teams["team_xg_against_per_90"].mean()
    for t in ["Ipswich", "Coventry", "Hull"]:
        if t not in df_teams["team_name"].values:
            df_teams = pd.concat([
                df_teams,
                pd.DataFrame([{"team_name": t, "team_xg_per_90": avg_xg,
                               "team_xg_against_per_90": avg_xga}]),
            ], ignore_index=True)

    print("Fetching opponent goals conceded...")
    df_goals = fpl.get_opponent_goals_conceded()
    df_goals["team_name"] = df_goals["team_name"].replace(config.TEAM_MAP)

    match_map = _match_map(fpl_df["full_name"].tolist(), u_df["player_name"].tolist())

    return {"fpl": fpl_df, "u_df": u_df, "teams": df_teams,
            "goals": df_goals, "match_map": match_map}


def build_preseason_features(gameweek: int, shared: dict | None = None) -> pd.DataFrame:
    """Build X for a gameweek before the season starts, using prior-season data."""
    shared = shared or _load_preseason_shared()
    fpl_df = shared["fpl"]
    u_df = shared["u_df"]
    teams_data = shared["teams"]
    goals_data = shared["goals"]

    df = fpl_df[[
        "full_name", "team_name", "player_position", "current_fpl_cost",
        "selected_by_percent", "total_minutes", "influence_per_90",
        "creativity_per_90", "threat_per_90", "ict_per_90", "is_penalty_taker",
    ]].copy()

    df["_match"] = df["full_name"].map(shared["match_map"])
    df = df.merge(u_df, left_on="_match", right_on="player_name", how="left").drop(
        columns=["_match", "player_name"]
    )

    for col in ["playing_time_min_percentage", "xg_per_90", "xag_per_90",
                "yellows_per_90", "reds_per_90"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    df["team_name"] = df["team_name"].replace(config.TEAM_MAP)
    df = df.merge(teams_data, on="team_name", how="left")

    fixtures = fpl.get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(config.TEAM_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(config.TEAM_MAP)

    opponent_map, home_map = {}, {}
    for _, row in fixtures.iterrows():
        opponent_map[row["home_team"]] = row["away_team"]
        opponent_map[row["away_team"]] = row["home_team"]
        home_map[row["home_team"]] = 1
        home_map[row["away_team"]] = 0

    df["opponent_team"] = df["team_name"].map(opponent_map)
    df["gameweek"] = gameweek
    df["is_at_home"] = df["team_name"].map(home_map).fillna(0).astype(int)

    df = df.merge(
        teams_data[["team_name", "team_xg_per_90", "team_xg_against_per_90"]],
        left_on="opponent_team", right_on="team_name", how="left",
        suffixes=("", "_opp"),
    )
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

    # No games played yet: zero out per-gameweek-history features.
    for col in ["points_last_3", "xg_last_3", "minutes_last_3",
                "clearances_blocks_interceptions_per_90", "tackles_per_90",
                "team_league_position", "opponent_league_position"]:
        df[col] = 0

    df["ownership_percent"] = fpl_df["selected_by_percent"].astype(float)

    df = df[~df["full_name"].isin(config.EXCLUDED_PLAYERS)].copy()
    if "total_minutes" in df.columns:
        df = df[df["total_minutes"] >= config.MIN_TOTAL_MINUTES]
    df = _fill_ict_from_prior(df)
    df = cap_per_90_outliers(df)

    df = df.loc[:, ~df.columns.duplicated()]
    return df[config.FEATURE_COLUMNS]
