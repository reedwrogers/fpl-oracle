#!/usr/bin/env python3
"""FPL Team Optimizer — integer programming via pulp.

Selects a 15-man squad that maximizes the expected points of each gameweek's
BEST STARTING XI (formation + captain), summed across the horizon. Bench players
only matter for selection, not for the objective — only the best 11 count each
week.

Usage:
  python optimize.py 1 5            # optimize GW1 squad across next 5 GWs
"""

import json
import os
import sys

import numpy as np
import pandas as pd
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, PULP_CBC_CMD

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PUBLISH_DIR = "/var/www/reedrogers/data"

POSITIONS = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
SQUAD_COUNTS = {"Goalkeeper": 2, "Defender": 5, "Midfielder": 5, "Forward": 3}
FORMATION_BOUNDS = {
    "Goalkeeper": (1, 1),
    "Defender": (3, 5),
    "Midfielder": (3, 5),
    "Forward": (1, 3),
}


def load_long(gameweek, num_weeks):
    """Return a long DataFrame: one row per (player, gameweek) with predicted
    points plus static player info."""
    from model import predict as model_predict

    rows = []
    for gw in range(gameweek, gameweek + num_weeks):
        x_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
        if not os.path.exists(x_path):
            print(f"  X_{gw}.csv missing, skipping")
            continue
        X = pd.read_csv(x_path)
        pred_df, _ = model_predict(gw, verbose=False)
        merged = pred_df.merge(
            X[["full_name", "current_fpl_cost"]], on="full_name", how="left"
        )
        merged["gameweek"] = gw
        rows.append(
            merged[
                [
                    "full_name", "position", "team_name",
                    "current_fpl_cost", "gameweek", "predicted_points",
                ]
            ]
        )
        print(f"  GW {gw}: {len(merged)} players predicted")

    df = pd.concat(rows, ignore_index=True)
    df = df.dropna(
        subset=["predicted_points", "current_fpl_cost", "position", "team_name"]
    )
    return df


def optimize_squad(df, budget=1000, time_limit=120):
    """Solve the combined squad + per-week starting-XI problem.

    Returns (squad, weeks) where:
      - squad is a list of 15 player dicts
      - weeks is a list of per-gameweek dicts (formation, expected_points,
        captain, starters, bench)
    """
    names = df["full_name"].unique().tolist()
    idx = {name: i for i, name in enumerate(names)}
    n = len(names)
    weeks = sorted(int(w) for w in df["gameweek"].unique())

    pos = df.groupby("full_name")["position"].first().to_dict()
    team = df.groupby("full_name")["team_name"].first().to_dict()
    cost = df.groupby("full_name")["current_fpl_cost"].first().to_dict()
    avg_pts = df.groupby("full_name")["predicted_points"].mean().to_dict()
    pts = {
        (r.full_name, r.gameweek): r.predicted_points for r in df.itertuples()
    }

    prob = LpProblem("Squad", LpMaximize)

    # x[i] = 1 if player i is in the 15-man squad
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in range(n)}
    # y[(i, w)] = 1 if player i starts in week w; c[(i, w)] = 1 if captain
    y = {(i, w): LpVariable(f"y_{i}_{w}", cat="Binary") for i in range(n) for w in weeks}
    c = {(i, w): LpVariable(f"c_{i}_{w}", cat="Binary") for i in range(n) for w in weeks}

    # Objective: sum over weeks of (starters points + captain bonus)
    prob += (
        lpSum(pts[(names[i], w)] * y[(i, w)] for i in range(n) for w in weeks)
        + lpSum(pts[(names[i], w)] * c[(i, w)] for i in range(n) for w in weeks)
    )

    # Squad constraints (15 players, per-position counts, max 3 per team, budget)
    prob += lpSum(x[i] for i in range(n)) == 15
    for p, cnt in SQUAD_COUNTS.items():
        prob += lpSum(x[i] for i in range(n) if pos[names[i]] == p) == cnt
    for t in set(team.values()):
        prob += lpSum(x[i] for i in range(n) if team[names[i]] == t) <= 3
    prob += lpSum(x[i] * cost[names[i]] for i in range(n)) <= budget

    # Per-week constraints: best XI with a valid formation + one captain,
    # all drawn from the squad.
    for w in weeks:
        prob += lpSum(y[(i, w)] for i in range(n)) == 11
        for p, (lo, hi) in FORMATION_BOUNDS.items():
            idx_p = [i for i in range(n) if pos[names[i]] == p]
            prob += lpSum(y[(i, w)] for i in idx_p) >= lo
            prob += lpSum(y[(i, w)] for i in idx_p) <= hi
        prob += lpSum(c[(i, w)] for i in range(n)) == 1
        for i in range(n):
            prob += y[(i, w)] <= x[i]
            prob += c[(i, w)] <= y[(i, w)]

    prob.solve(PULP_CBC_CMD(msg=False, timeLimit=time_limit))

    squad_names = [names[i] for i in range(n) if x[i].value() is not None and x[i].value() > 0.5]

    squad = [
        {
            "name": nm,
            "team": team[nm],
            "position": pos[nm],
            "cost": float(round(cost[nm] / 10, 1)),
            "avg_points": float(round(avg_pts[nm], 2)),
        }
        for nm in squad_names
    ]
    squad.sort(key=lambda p: -p["avg_points"])

    week_results = []
    total = 0.0
    for w in weeks:
        starters = [names[i] for i in range(n) if y[(i, w)].value() is not None and y[(i, w)].value() > 0.5]
        captains = [names[i] for i in range(n) if c[(i, w)].value() is not None and c[(i, w)].value() > 0.5]
        captain = captains[0] if captains else None

        d = sum(1 for nm in starters if pos[nm] == "Defender")
        m = sum(1 for nm in starters if pos[nm] == "Midfielder")
        f = sum(1 for nm in starters if pos[nm] == "Forward")

        expected = float(sum(pts[(nm, w)] for nm in starters))
        if captain is not None:
            expected += float(pts[(captain, w)])

        bench = [nm for nm in squad_names if nm not in starters]

        week_results.append({
            "gameweek": w,
            "formation": f"{d}-{m}-{f}",
            "expected_points": round(expected, 1),
            "captain": captain,
            "starters": [
                {
                    "name": nm,
                    "position": pos[nm],
                    "points": float(round(pts[(nm, w)], 1)),
                    "role": "captain" if nm == captain else "",
                }
                for nm in sorted(starters, key=lambda nm: -pts[(nm, w)])
            ],
            "bench": [
                {"name": nm, "position": pos[nm], "points": float(round(pts[(nm, w)], 1))}
                for nm in sorted(bench, key=lambda nm: -pts[(nm, w)])
            ],
        })
        total += expected

    return squad, week_results, total


def publish_squad(gameweek=None, num_weeks=1):
    if gameweek is None:
        gameweek = 1

    df = load_long(gameweek, num_weeks)
    weeks = sorted(df["gameweek"].unique())
    if not weeks:
        print("No gameweeks to optimize.")
        return

    print(f"Optimizing {num_weeks}-GW squad (best XI each week)...")
    squad, week_results, total = optimize_squad(df)

    avg = round(total / len(weeks), 1)

    output = {
        "gameweek": gameweek,
        "num_weeks": num_weeks,
        "note": (
            f"Squad selected to maximize the best starting XI each of GWs "
            f"{weeks[0]}-{weeks[-1]}; each week's XI + captain is the optimal "
            f"11 from that squad."
        ),
        "squad": squad,
        "weeks": week_results,
        "total_expected_points": round(total, 1),
        "avg_expected_points_per_week": avg,
    }

    path = os.path.join(PUBLISH_DIR, "squad.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Published {path}")

    print(f"\nBest squad ({len(squad)} players), avg {avg} pts/week over {len(weeks)} GWs:")
    for p in squad:
        print(f"  {p['name']:<35s} {p['team']:<12s} {p['position']:<11s} £{p['cost']}m  avg {p['avg_points']} pts")

    for wk in week_results:
        print(f"\nGW {wk['gameweek']} — {wk['formation']} — {wk['expected_points']} pts")
        for s in wk["starters"]:
            cap = " (C)" if s["role"] == "captain" else ""
            print(f"  {s['name']:<35s} {s['position']:<11s} {s['points']} pts{cap}")
        print("  Bench:")
        for b in wk["bench"]:
            print(f"  {b['name']:<35s} {b['position']:<11s} {b['points']} pts")


if __name__ == "__main__":
    gw = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    nw = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    publish_squad(gameweek=gw, num_weeks=nw)
