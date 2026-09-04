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
    """Most recent completed gameweek.

    The event-level ``finished`` flag can lag behind the actual results (it
    sometimes doesn't flip until the next gameweek begins), so we also derive
    completion from fixture data (all fixtures kicked off + recorded a score)
    and take the maximum of the two."""
    events = pd.DataFrame(bootstrap()["events"])
    finished = events.loc[events["finished"] == True, "id"]
    event_max = int(finished.max()) if not finished.empty else 0

    fixtures = pd.DataFrame(_get_json(f"{config.FPL_API}/fixtures/"))
    if fixtures.empty:
        return event_max

    done = (
        (fixtures["started"] == True)
        & fixtures["team_h_score"].notna()
        & fixtures["team_a_score"].notna()
    )
    fixtures = fixtures.assign(done=done)

    completed = [
        int(gw)
        for gw, grp in fixtures.groupby("event")
        if len(grp) > 0 and grp["done"].all()
    ]
    fixture_max = max(completed) if completed else 0

    return max(event_max, fixture_max)


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


def _played_fixtures() -> pd.DataFrame:
    """Fixtures that have actually been played (started + a recorded score).

    The ``finished`` flag lags behind real results, so derive "played" from the
    presence of a scoreline."""
    fixtures = pd.DataFrame(_get_json(f"{config.FPL_API}/fixtures/"))
    if fixtures.empty:
        return fixtures
    played = (
        (fixtures["started"] == True)
        & fixtures["team_h_score"].notna()
        & fixtures["team_a_score"].notna()
    )
    return fixtures[played]


def get_standings() -> pd.DataFrame:
    """Current league table computed from played fixtures.

    The bootstrap ``teams[].position`` field is not populated reliably (it's 0
    here), so we rank teams ourselves: points, then goal difference, then goals
    scored."""
    teams = pd.DataFrame(bootstrap()["teams"])[["id", "name"]]
    fixtures = _played_fixtures()

    rows = []
    for _, t in teams.iterrows():
        tid = t["id"]
        home = fixtures[fixtures["team_h"] == tid]
        away = fixtures[fixtures["team_a"] == tid]

        gf = int(home["team_h_score"].sum() + away["team_a_score"].sum())
        ga = int(home["team_a_score"].sum() + away["team_h_score"].sum())
        wins = int(
            (home["team_h_score"] > home["team_a_score"]).sum()
            + (away["team_a_score"] > away["team_h_score"]).sum()
        )
        draws = int(
            (home["team_h_score"] == home["team_a_score"]).sum()
            + (away["team_a_score"] == away["team_h_score"]).sum()
        )

        rows.append({
            "name": t["name"],
            "points": wins * 3 + draws,
            "goal_difference": gf - ga,
            "goals_for": gf,
        })

    standings = pd.DataFrame(rows).sort_values(
        ["points", "goal_difference", "goals_for"], ascending=False
    ).reset_index(drop=True)
    standings["position"] = standings.index + 1
    standings["team_name"] = standings["name"].replace(config.TEAM_MAP)
    return standings[["team_name", "position"]]


def get_upcoming_fixtures(gameweek: int, num_weeks: int) -> dict:
    """Upcoming fixtures keyed by raw FPL team name.

    Returns ``{team_name: [{gameweek, opponent, is_home}]}`` for the next
    ``num_weeks`` gameweeks, where ``opponent`` is the raw FPL team name."""
    data = bootstrap()
    team_names = {t["id"]: t["name"] for t in data["teams"]}
    fixtures = pd.DataFrame(_get_json(f"{config.FPL_API}/fixtures/"))
    fixtures = fixtures[
        (fixtures["event"] >= gameweek) & (fixtures["event"] < gameweek + num_weeks)
    ]

    out: dict[str, list[dict]] = {}
    for _, fx in fixtures.iterrows():
        h = team_names[fx["team_h"]]
        a = team_names[fx["team_a"]]
        gw = int(fx["event"])
        out.setdefault(h, []).append({"gameweek": gw, "opponent": a, "is_home": True})
        out.setdefault(a, []).append({"gameweek": gw, "opponent": h, "is_home": False})
    return out


def mapped_to_raw_team_map() -> dict:
    """Map each Understat/mapped team name back to the raw FPL team name."""
    data = bootstrap()
    return {
        config.TEAM_MAP.get(t["name"], t["name"]): t["name"]
        for t in data["teams"]
    }


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
    """Goals conceded in the last 3 played games for each team."""
    teams = pd.DataFrame(bootstrap()["teams"])
    fixtures = _played_fixtures()

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


def get_my_team(team_id: int, gameweek: int | None = None) -> dict:
    """A manager's submitted squad for a gameweek, resolved to names/teams/
    positions/cost with live points where available. No auth required — the
    entry and picks endpoints are public."""
    data = bootstrap()
    players = pd.DataFrame(data["elements"])
    teams = {t["id"]: t["name"] for t in data["teams"]}
    positions = {p["id"]: p["singular_name"] for p in data["element_types"]}

    entry = _get_json(f"{config.FPL_API}/entry/{team_id}/")
    if gameweek is None:
        gameweek = int(entry.get("current_event") or get_next_gameweek())

    picks = _get_json(f"{config.FPL_API}/entry/{team_id}/event/{gameweek}/picks/")
    history = picks.get("entry_history", {})

    live_points: dict[int, int] = {}
    try:
        live = _get_json(f"{config.FPL_API}/event/{gameweek}/live/")
        for el in live.get("elements", []):
            live_points[int(el["id"])] = el["stats"].get("total_points", 0)
    except Exception:
        pass

    info = players.set_index("id")

    def player_row(pid: int) -> dict:
        p = info.loc[int(pid)]
        return {
            "id": int(pid),
            "name": f"{p['first_name']} {p['second_name']}",
            "team": teams.get(int(p["team"])),
            "position": positions.get(int(p["element_type"])),
            "cost": float(p["now_cost"]) / 10.0,
            "points": int(live_points.get(int(pid), 0)),
        }

    starters, bench = [], []
    captain = vice_captain = None
    for pk in picks.get("picks", []):
        row = player_row(pk["element"])
        row["is_captain"] = bool(pk["is_captain"])
        row["is_vice_captain"] = bool(pk["is_vice_captain"])
        (starters if pk["multiplier"] > 0 else bench).append(row)
        if row["is_captain"]:
            captain = row["name"]
        if row["is_vice_captain"]:
            vice_captain = row["name"]

    position_order = {"Goalkeeper": 0, "Defender": 1, "Midfielder": 2, "Forward": 3}
    starters.sort(key=lambda r: (position_order.get(r["position"], 9), -r["points"]))
    bench.sort(key=lambda r: (position_order.get(r["position"], 9), -r["points"]))

    def_count = sum(1 for r in starters if r["position"] == "Defender")
    mid_count = sum(1 for r in starters if r["position"] == "Midfielder")
    fwd_count = sum(1 for r in starters if r["position"] == "Forward")

    return {
        "team_id": team_id,
        "team_name": entry.get("name"),
        "manager": f"{entry.get('player_first_name', '')} {entry.get('player_last_name', '')}".strip(),
        "gameweek": gameweek,
        "points": history.get("points", 0),
        "total_points": history.get("total_points", 0),
        "overall_rank": history.get("overall_rank", entry.get("summary_overall_rank")),
        "team_value": (history.get("value") or entry.get("last_deadline_value", 0)) / 10.0,
        "bank": (history.get("bank") or entry.get("last_deadline_bank", 0)) / 10.0,
        "transfers": history.get("event_transfers", 0),
        "transfers_cost": history.get("event_transfers_cost", 0),
        "points_on_bench": history.get("points_on_bench", 0),
        "captain": captain,
        "vice_captain": vice_captain,
        "formation": f"{def_count}-{mid_count}-{fwd_count}",
        "starters": starters,
        "bench": bench,
    }
