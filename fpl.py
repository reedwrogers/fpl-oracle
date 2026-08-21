"""FPL API client — all data fetching from fantasy.premierleague.com."""

from functools import lru_cache

import pandas as pd
import requests

import config


@lru_cache(maxsize=None)
def _get_json(url: str) -> dict:
    return requests.get(url).json()


def bootstrap() -> dict:
    """Cached bootstrap-static payload (players, teams, events, element_types)."""
    return _get_json(f"{config.FPL_API}/bootstrap-static/")


def _element_summary(pid: int) -> dict:
    return _get_json(f"{config.FPL_API}/element-summary/{pid}/")


# --- Gameweek helpers -------------------------------------------------------


def get_next_gameweek() -> int:
    events = pd.DataFrame(bootstrap()["events"])
    next_gw = events.loc[events["is_next"], "id"]
    if next_gw.empty:
        raise ValueError("No upcoming gameweek found")
    return int(next_gw.iloc[0])


def get_latest_finished_gameweek() -> int:
    events = pd.DataFrame(bootstrap()["events"])
    finished = events.loc[events["finished"] == True, "id"]
    if finished.empty:
        return 0
    return int(finished.max())


# --- Data sources -----------------------------------------------------------


def get_players() -> pd.DataFrame:
    """Basic FPL player list with team and position resolved to names."""
    data = bootstrap()
    players = pd.DataFrame(data["elements"])
    teams = {t["id"]: t["name"] for t in data["teams"]}
    positions = {p["id"]: p["singular_name"] for p in data["element_types"]}

    players["team_name"] = players["team"].map(teams)
    players["position"] = players["element_type"].map(positions)
    players["full_name"] = players["first_name"] + " " + players["second_name"]

    return players[
        ["id", "first_name", "second_name", "team_name", "position", "now_cost", "full_name"]
    ]


def get_fixtures(week: int) -> pd.DataFrame:
    """Games for a given gameweek, with team names resolved."""
    data = bootstrap()
    team_map = {t["id"]: t["name"] for t in data["teams"]}

    fixtures = pd.DataFrame(_get_json(f"{config.FPL_API}/fixtures/"))
    fixtures = fixtures[fixtures["event"] == week]

    fixtures["home_team"] = fixtures["team_h"].map(team_map)
    fixtures["away_team"] = fixtures["team_a"].map(team_map)
    fixtures["week"] = fixtures["event"]

    return fixtures[["home_team", "away_team", "week"]]


def get_standings() -> pd.DataFrame:
    data = bootstrap()
    teams = pd.DataFrame(data["teams"])[["name", "position"]]
    teams["team_name"] = teams["name"].replace(config.TEAM_MAP)
    return teams[["team_name", "position"]]


def get_fixtures_and_league_spots(gameweek: int) -> pd.DataFrame:
    """Per-team rows for a gameweek: home/away flag and league position."""
    fixtures = get_fixtures(gameweek)
    fixtures["home_team"] = fixtures["home_team"].replace(config.TEAM_MAP)
    fixtures["away_team"] = fixtures["away_team"].replace(config.TEAM_MAP)

    home = fixtures[["home_team", "week"]].rename(columns={"home_team": "team"})
    home["home"] = 1
    away = fixtures[["away_team", "week"]].rename(columns={"away_team": "team"})
    away["home"] = 0

    combined = pd.concat([home, away], ignore_index=True)
    standings = get_standings()

    final = combined.merge(standings, left_on="team", right_on="team_name", how="left")
    return final.drop(columns=["team_name"]).sort_values("position")


def get_defensive_stats() -> pd.DataFrame:
    """Season-long CBI and tackles per 90, from each player's history."""
    players = pd.DataFrame(bootstrap()["elements"])
    rows = []

    for pid in players["id"]:
        try:
            history = _element_summary(pid).get("history", [])
            if not history:
                continue

            total_minutes = sum(gw["minutes"] for gw in history)
            total_cbi = sum(gw.get("clearances_blocks_interceptions", 0) for gw in history)
            total_tackles = sum(gw.get("tackles", 0) for gw in history)

            if total_minutes > 0:
                cbi_per_90 = round((total_cbi / total_minutes) * 90, 2)
                tackles_per_90 = round((total_tackles / total_minutes) * 90, 2)
            else:
                cbi_per_90 = 0.0
                tackles_per_90 = 0.0

            info = players[players["id"] == pid].iloc[0]
            rows.append({
                "full_name": f"{info['first_name']} {info['second_name']}",
                "clearances_blocks_interceptions_per_90": cbi_per_90,
                "tackles_per_90": tackles_per_90,
            })
        except Exception as e:
            print(f"Error processing player {pid}: {e}")

    return pd.DataFrame(rows)


def get_recent_stats() -> pd.DataFrame:
    """Recent form (last 3 games) plus season-total minutes and per-90 ICT."""
    players = pd.DataFrame(bootstrap()["elements"])
    rows = []

    for pid in players["id"]:
        try:
            history = _element_summary(pid).get("history", [])
            info = players[players["id"] == pid].iloc[0]
            full_name = f"{info['first_name']} {info['second_name']}"

            last_3 = history[-3:] if len(history) >= 3 else history
            points_last_3 = sum(gw.get("total_points", 0) for gw in last_3)
            xg_last_3 = round(sum(float(gw.get("expected_goals", 0)) for gw in last_3), 2)
            minutes_last_3 = sum(gw.get("minutes", 0) for gw in last_3)

            minutes = float(info.get("minutes", 0))
            per_90 = (90.0 / minutes) if minutes > 0 else float("nan")
            is_penalty_taker = 1 if info.get("penalties_order", 0) in [1, 2] else 0

            rows.append({
                "full_name": full_name,
                "points_last_3": points_last_3,
                "xg_last_3": xg_last_3,
                "minutes_last_3": minutes_last_3,
                "total_minutes": minutes,
                "is_penalty_taker": is_penalty_taker,
                "ownership_percent": float(info.get("selected_by_percent", 0)),
                "influence_per_90": float(info.get("influence", 0)) * per_90,
                "creativity_per_90": float(info.get("creativity", 0)) * per_90,
                "threat_per_90": float(info.get("threat", 0)) * per_90,
                "ict_per_90": float(info.get("ict_index", 0)) * per_90,
            })
        except Exception as e:
            print(f"Error processing player {pid}: {e}")

    return pd.DataFrame(rows)


def get_opponent_goals_conceded() -> pd.DataFrame:
    """Goals conceded in the last 3 finished games for each team."""
    teams = pd.DataFrame(bootstrap()["teams"])
    fixtures = pd.DataFrame(_get_json(f"{config.FPL_API}/fixtures/"))
    fixtures = fixtures[fixtures["finished"] == True]

    rows = []
    for team_id in teams["id"]:
        team_fixtures = fixtures[
            (fixtures["team_h"] == team_id) | (fixtures["team_a"] == team_id)
        ].sort_values("event", ascending=False).head(3)

        goals_conceded = 0
        for _, fx in team_fixtures.iterrows():
            if fx["team_h"] == team_id:
                goals_conceded += fx["team_a_score"]
            else:
                goals_conceded += fx["team_h_score"]

        team_name = teams[teams["id"] == team_id].iloc[0]["name"]
        rows.append({"team_name": team_name, "goals_conceded_last_3": goals_conceded})

    return pd.DataFrame(rows)


def get_players_with_points(gameweek: int) -> pd.DataFrame:
    """FPL players with their points and minutes for a specific gameweek."""
    players = get_players()

    points, minutes = [], []
    for pid in players["id"]:
        history = _element_summary(pid).get("history", [])
        gw = next((g for g in history if g["round"] == gameweek), None)
        points.append(gw["total_points"] if gw else 0)
        minutes.append(gw["minutes"] if gw else 0)

    players["gw_points"] = points
    players["gw_minutes"] = minutes
    return players[["full_name", "gw_points", "gw_minutes"]]
