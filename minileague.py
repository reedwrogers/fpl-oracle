#!/usr/bin/env python3
"""Mini-league view — standings, ownership overlap, and league-aware transfer lens.

Unlike optimize.publish_transfers (maximizes raw expected points = the
overall-rank objective), this module answers "what wins MY league":
  - where I stand vs each rival (points gap),
  - which players the league owns that I don't (block targets when leading),
  - which high-upside picks the league doesn't own (differentials when chasing),
  - what every rival is captaining.

Usage:
  python minileague.py            # fetch league + publish site/data/minileague.json
"""

import json
import os
import sys

import pandas as pd

import config
import fpl

PUBLISH_DIR = config.PUBLISH_DIR


def fetch_league(team_ids=None):
    """Fetch every manager's squad. Returns list of get_my_team dicts."""
    team_ids = team_ids or config.MINI_LEAGUE_IDS
    teams = []
    for tid in team_ids:
        try:
            teams.append(fpl.get_my_team(tid))
        except Exception as e:
            print(f"  Skipping team {tid}: {e}")
    return teams


def load_predictions():
    """Map full_name -> predicted points from the published predictions.csv."""
    path = os.path.join(PUBLISH_DIR, "predictions.csv")
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path, usecols=["full_name", "predicted_points"])
        return dict(zip(df["full_name"], df["predicted_points"]))
    except Exception as e:
        print(f"  No predictions available: {e}")
        return {}


def build_minileague(team_ids=None):
    teams = fetch_league(team_ids)
    if not teams:
        raise ValueError("No league teams fetched.")

    me_id = config.MY_TEAM_ID
    me = next((t for t in teams if t["team_id"] == me_id), teams[0])
    my_names = {p["name"] for p in me["starters"] + me["bench"]}

    # --- Standings (by total points) ---
    standings = sorted(teams, key=lambda t: -(t.get("total_points") or 0))
    my_total = me.get("total_points") or 0
    leader_total = standings[0].get("total_points") or 0
    my_rank = next(i for i, t in enumerate(standings, 1) if t["team_id"] == me["team_id"])

    # --- Ownership: player -> list of owning team names ---
    owners: dict[str, list[str]] = {}
    player_meta: dict[str, dict] = {}
    for t in teams:
        for p in t["starters"] + t["bench"]:
            owners.setdefault(p["name"], []).append(t["team_name"])
            player_meta.setdefault(p["name"], {
                "team": p.get("team"),
                "position": p.get("position"),
                "cost": p.get("cost"),
            })

    preds = load_predictions()
    n = len(teams)

    ownership = [
        {
            "name": name,
            "owners": len(names),
            "ownership_pct": round(100 * len(names) / n, 1),
            "owned_by": sorted(names),
            "i_own": name in my_names,
            "predicted_points": (
                float(preds[name]) if name in preds else None
            ),
            **player_meta[name],
        }
        for name, names in owners.items()
    ]
    ownership.sort(key=lambda r: (-r["owners"], -(r["predicted_points"] or -1)))

    # --- Transfer lens ---
    # Block targets: widely owned in the league but not by me (risk if I go without).
    block_targets = [
        r for r in ownership
        if not r["i_own"] and r["owners"] >= max(2, (n + 1) // 2)
    ]
    block_targets.sort(key=lambda r: (-r["owners"], -(r["predicted_points"] or -1)))

    # Differential targets: predicted well but owned by <=1 league rival and not me.
    differential_targets = [
        r for r in ownership
        if not r["i_own"] and r["owners"] <= 1 and (r["predicted_points"] or 0) > 0
    ]
    differential_targets.sort(key=lambda r: -(r["predicted_points"] or -1))

    # My exposed differentials: players only I own (my edge if they haul).
    my_differentials = [
        r for r in ownership if r["i_own"] and r["owners"] <= 1
    ]
    my_differentials.sort(key=lambda r: -(r["predicted_points"] or -1))

    # --- Per-rival overlap with me ---
    rivals = []
    for t in standings:
        if t["team_id"] == me["team_id"]:
            continue
        t_names = {p["name"] for p in t["starters"] + t["bench"]}
        shared = sorted(my_names & t_names)
        rivals.append({
            "team_id": t["team_id"],
            "team_name": t.get("team_name"),
            "manager": t.get("manager"),
            "total_points": t.get("total_points"),
            "gap": round((my_total or 0) - (t.get("total_points") or 0), 1),
            "captain": t.get("captain"),
            "shared_players": len(shared),
            "shared": shared,
            "only_they_own": sorted(t_names - my_names),
            "only_i_own": sorted(my_names - t_names),
        })

    gameweek = me.get("gameweek")

    return {
        "gameweek": gameweek,
        "my_team_id": me["team_id"],
        "my_team_name": me.get("team_name"),
        "my_rank": my_rank,
        "my_total": my_total,
        "leader_total": leader_total,
        "points_behind_leader": round((leader_total or 0) - (my_total or 0), 1),
        "mode": "chase" if (my_rank or 1) > 1 else "defend",
        "standings": [
            {
                "team_id": t["team_id"],
                "team_name": t.get("team_name"),
                "manager": t.get("manager"),
                "total_points": t.get("total_points"),
                "is_me": t["team_id"] == me["team_id"],
            }
            for t in standings
        ],
        "captains": [
            {"team_name": t.get("team_name"), "captain": t.get("captain"),
             "is_me": t["team_id"] == me["team_id"]}
            for t in teams
        ],
        "ownership": ownership,
        "block_targets": block_targets[:15],
        "differential_targets": differential_targets[:15],
        "my_differentials": my_differentials[:15],
        "rivals": rivals,
    }


def publish(team_ids=None):
    print("Building mini-league view...")
    output = build_minileague(team_ids)
    os.makedirs(PUBLISH_DIR, exist_ok=True)
    path = os.path.join(PUBLISH_DIR, "minileague.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Published {path} "
          f"(rank {output['my_rank']}/{len(output['standings'])}, "
          f"{output['points_behind_leader']} behind leader, mode={output['mode']})")
    return output


if __name__ == "__main__":
    ids = [int(a) for a in sys.argv[1:]] or None
    publish(ids)
