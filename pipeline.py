#!/usr/bin/env python3
"""Orchestrator for the FPL Oracle data + publish workflow.

Commands:
  run          Idempotent daily step: write y (actuals), X (features), publish.
  preseason    Build pre-season X files using prior-season Understat data.
  backfill     Rebuild existing X files with prior-season Understat data.
  publish      Publish predictions + squad for the latest gameweek.

Usage:
  python pipeline.py run
  python pipeline.py preseason          # GWs 1, 2, 3
  python pipeline.py preseason 5        # GW 5 only
  python pipeline.py backfill
"""

import json
import sys

import pandas as pd

import config
import fpl
import features


def write_y(gameweek: int) -> None:
    y_path = config.DATA_DIR / f"y_{gameweek}.csv"
    df_points = fpl.get_players_with_points(gameweek)

    x_path = config.DATA_DIR / f"X_{gameweek}.csv"
    if x_path.exists():
        X = pd.read_csv(x_path)
        df_points = df_points[df_points["full_name"].isin(X["full_name"])]

    df_points.to_csv(y_path, index=False)
    print(f"Wrote {y_path}")


def write_x(gameweek: int, season: str = config.SEASON) -> None:
    x_path = config.DATA_DIR / f"X_{gameweek}.csv"
    df = features.build_features(gameweek, season=season)
    df.to_csv(x_path, index=False)
    print(f"Wrote {x_path} ({len(df)} players)")


def publish(gameweek: int | None = None) -> None:
    import model
    if gameweek is None:
        gameweek = model.find_latest_gameweek()
    model.publish(gameweek)


def write_my_team() -> None:
    """Fetch the manager's FPL squad and write it for the site (idempotent)."""
    config.PUBLISH_DIR.mkdir(parents=True, exist_ok=True)
    out = config.PUBLISH_DIR / "myteam.json"
    try:
        team = fpl.get_my_team(config.MY_TEAM_ID)
    except Exception as e:
        print(f"Skipping my-team fetch: {e}")
        return
    with open(out, "w") as f:
        json.dump(team, f, indent=2)
    print(f"Wrote {out} ({team['team_name']}, GW {team['gameweek']})")


def run() -> None:
    last_finished = fpl.get_latest_finished_gameweek()
    next_gw = fpl.get_next_gameweek()
    live_gw = fpl.get_current_gameweek()

    # Publish target: the gameweek in progress while it is still live,
    # otherwise the upcoming gameweek. A future gameweek is never published
    # while its predecessor is live. `last_finished` (derived from fixture
    # scores) takes precedence so a just-finished gameweek rolls forward even
    # if the API `is_current` flag lags behind the last whistle.
    if live_gw is not None and live_gw > last_finished:
        publish_gw = live_gw
    else:
        publish_gw = next_gw

    publish_ready = False

    # 1) Record actual points for the most recently finished gameweek, as soon
    #    as its last fixture has a score.
    if last_finished >= 1:
        y_path = config.DATA_DIR / f"y_{last_finished}.csv"
        if not y_path.exists():
            print(f"Recording actuals for finished GW {last_finished}")
            write_y(last_finished)
            publish_ready = True

    # 2) Prep features for the upcoming gameweek early (waiting state) and
    #    refresh them once new actuals land. Building a future gameweek alone
    #    never triggers a publish.
    if next_gw >= 1:
        x_path = config.DATA_DIR / f"X_{next_gw}.csv"
        if not x_path.exists() or publish_ready:
            action = "Building" if not x_path.exists() else "Refreshing"
            print(f"{action} features for GW {next_gw}")
            write_x(next_gw)
            if next_gw == publish_gw:
                publish_ready = True
            else:
                print(f"GW {next_gw} features ready, waiting for GW {publish_gw} to finish")

    # 3) Ensure the publish target's features exist (e.g. fresh checkout).
    if publish_gw >= 1 and publish_gw != next_gw:
        target_x = config.DATA_DIR / f"X_{publish_gw}.csv"
        if not target_x.exists():
            print(f"Building features for GW {publish_gw}")
            write_x(publish_gw)
            publish_ready = True

    # 4) Refresh the manager's own squad for the site (cheap, always run).
    write_my_team()

    # 5) Publish only the publish target, and only when it has new data.
    if publish_ready:
        publish(publish_gw)
    else:
        print(f"Up to date (live GW {live_gw}, next GW {next_gw}, finished {last_finished})")


def preseason() -> None:
    gws = [int(sys.argv[2])] if len(sys.argv) > 2 else [1, 2, 3]
    shared = features._load_preseason_shared()
    for gw in gws:
        print(f"\nBuilding pre-season X_{gw}.csv...")
        df = features.build_preseason_features(gw, shared)
        out_path = config.DATA_DIR / f"X_{gw}.csv"
        df.to_csv(out_path, index=False)
        print(f"  Wrote {out_path} ({len(df)} players, {len(df.columns)} cols)")


def backfill() -> None:
    """Rebuild existing X files using prior-season Understat data, preserving
    FPL-derived columns already stored in the files."""
    from thefuzz import process

    gameweeks = sorted(
        int(p.stem.split("_")[1])
        for p in config.DATA_DIR.glob("X_*.csv")
    )
    print(f"Existing gameweeks in data/: {gameweeks}")

    fpl_base_cols = [
        "full_name", "team_name", "player_position", "current_fpl_cost",
        "points_last_3", "xg_last_3", "minutes_last_3", "total_minutes",
        "is_penalty_taker", "ownership_percent",
        "influence_per_90", "creativity_per_90", "threat_per_90", "ict_per_90",
        "clearances_blocks_interceptions_per_90", "tackles_per_90",
        "gameweek", "is_at_home", "team_league_position", "opponent_league_position",
    ]

    import understat

    print("Fetching prior-season Understat data (one-time)...")
    df_understat = understat.get_player_stats(season=config.PRIOR_SEASON)
    df_teams = understat.get_team_stats(
        season=config.PRIOR_SEASON, teams=config.UNDERSTAT_TEAMS_PRIOR
    )
    df_teams["team_name"] = df_teams["team_name"].replace(config.TEAM_MAP)

    df_goals_conceded = fpl.get_opponent_goals_conceded()
    df_goals_conceded["team_name"] = df_goals_conceded["team_name"].replace(config.TEAM_MAP)

    for gw in gameweeks:
        prev_path = config.DATA_DIR / f"X_{gw}.csv"
        existing = pd.read_csv(prev_path)
        print(f"\nRebuilding GW {gw} ({len(existing)} rows)...")

        df = existing[fpl_base_cols].copy()

        match_map = features._match_map(
            df["full_name"].tolist(), df_understat["player_name"].tolist()
        )
        df["_match"] = df["full_name"].map(match_map)
        df = df.merge(
            df_understat, left_on="_match", right_on="player_name", how="inner"
        ).drop(columns=["_match", "player_name"])

        df["team_name"] = df["team_name"].replace(config.TEAM_MAP)
        df = df.merge(df_teams, on="team_name", how="left")

        fixtures = fpl.get_fixtures(gw)
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

        df = df.rename(columns={
            "team_xg_per_90_opp": "opponent_xg_per_90",
            "team_xg_against_per_90_opp": "opponent_xg_against_per_90",
            "goals_conceded_last_3": "opponent_goals_conceded_last_3",
        })

        df = df.loc[:, ~df.columns.duplicated()]

        out = df[config.FEATURE_COLUMNS]
        out.to_csv(prev_path, index=False)
        print(f"  Wrote {prev_path} ({len(out)} players, {len(out.columns)} cols)")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    {
        "run": run,
        "preseason": preseason,
        "backfill": backfill,
        "publish": lambda: publish(),
        "myteam": write_my_team,
    }[command]()
