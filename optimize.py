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

import config

DATA_DIR = config.DATA_DIR
PUBLISH_DIR = config.PUBLISH_DIR

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
    points plus static player info.

    The pool is restricted to players who appear in the *current* gameweek's
    predictions, so the optimizer only considers the same "established" players
    shown in the predictions table."""
    from model import predict as model_predict

    universe = None
    rows = []
    for gw in range(gameweek, gameweek + num_weeks):
        x_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
        if not os.path.exists(x_path):
            print(f"  X_{gw}.csv missing, skipping")
            continue
        X = pd.read_csv(x_path)
        pred_df, _ = model_predict(gw, verbose=False)
        if universe is None:
            universe = set(pred_df["full_name"])
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
    if universe:
        df = df[df["full_name"].isin(universe)]
    df = df.dropna(
        subset=["predicted_points", "current_fpl_cost", "position", "team_name"]
    )
    return df


def optimize_squad(df, budget=1000, time_limit=120, current_names=None, num_transfers=None, forbid_squads=None):
    """Solve the combined squad + per-week starting-XI problem.

    If ``current_names`` and ``num_transfers`` are given, the squad is anchored
    to the current team: exactly ``num_transfers`` of those players are dropped
    (replaced by new signings). ``forbid_squads`` is a list of exact 15-player
    squads (sets of names) to exclude, used to enumerate alternatives.

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

    # A player may be missing from some gameweeks (e.g. in-season vs pre-season
    # feature sets); fall back to their average across the weeks they appear in.
    def pts_at(name: str, week: int) -> float:
        return pts.get((name, week), avg_pts[name])

    prob = LpProblem("Squad", LpMaximize)

    # x[i] = 1 if player i is in the 15-man squad
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in range(n)}
    # y[(i, w)] = 1 if player i starts in week w; c[(i, w)] = 1 if captain
    y = {(i, w): LpVariable(f"y_{i}_{w}", cat="Binary") for i in range(n) for w in weeks}
    c = {(i, w): LpVariable(f"c_{i}_{w}", cat="Binary") for i in range(n) for w in weeks}

    # Objective: sum over weeks of (starters points + captain bonus)
    prob += (
        lpSum(pts_at(names[i], w) * y[(i, w)] for i in range(n) for w in weeks)
        + lpSum(pts_at(names[i], w) * c[(i, w)] for i in range(n) for w in weeks)
    )

    # Squad constraints (15 players, per-position counts, max 3 per team, budget)
    prob += lpSum(x[i] for i in range(n)) == 15
    for p, cnt in SQUAD_COUNTS.items():
        prob += lpSum(x[i] for i in range(n) if pos[names[i]] == p) == cnt
    for t in set(team.values()):
        prob += lpSum(x[i] for i in range(n) if team[names[i]] == t) <= 3
    prob += lpSum(x[i] * cost[names[i]] for i in range(n)) <= budget

    # Transfer count: drop exactly num_transfers of the current squad.
    if current_names and num_transfers is not None:
        current_idx = [i for i in range(n) if names[i] in current_names]
        prob += lpSum(x[i] for i in current_idx) == len(current_idx) - num_transfers

    # Exclude previously-found squads (for enumerating K-best alternatives).
    if forbid_squads:
        for fs in forbid_squads:
            idx_fs = [i for i in range(n) if names[i] in fs]
            if len(idx_fs) == 15:
                prob += lpSum(x[i] for i in idx_fs) <= 14

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

        expected = float(sum(pts_at(nm, w) for nm in starters))
        if captain is not None:
            expected += float(pts_at(captain, w))

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
                    "points": float(round(pts_at(nm, w), 1)),
                    "role": "captain" if nm == captain else "",
                }
                for nm in sorted(starters, key=lambda nm: -pts_at(nm, w))
            ],
            "bench": [
                {"name": nm, "position": pos[nm], "points": float(round(pts_at(nm, w), 1))}
                for nm in sorted(bench, key=lambda nm: -pts_at(nm, w))
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


def _player_dict(df, name, team_lookup=None):
    sub = df[df["full_name"] == name]
    if len(sub) > 0:
        row = sub.iloc[0]
        return {
            "name": name,
            "team": row["team_name"],
            "position": row["position"],
            "cost": float(round(row["current_fpl_cost"] / 10, 1)),
            "avg_points": float(round(sub["predicted_points"].mean(), 2)),
        }
    p = (team_lookup or {}).get(name, {})
    return {
        "name": name,
        "team": p.get("team"),
        "position": p.get("position"),
        "cost": p.get("cost"),
        "avg_points": None,
    }


def publish_transfers(gameweek=None, num_weeks=5, max_transfers=5, num_options=3):
    """Recommend transfers that maximize expected points over ``num_weeks``,
    anchored to the manager's current squad.

    For each transfer count 1..max_transfers, enumerates up to ``num_options``
    distinct alternatives (K-best via forbidden-solution iteration), each with
    the transfer-in players' upcoming fixtures. Writes transfers.json."""
    if gameweek is None:
        gameweek = 1

    from fpl import get_my_team, get_upcoming_fixtures, mapped_to_raw_team_map

    df = load_long(gameweek, num_weeks)
    weeks = sorted(df["gameweek"].unique())
    if not weeks:
        print("No gameweeks to optimize.")
        return

    team = get_my_team(config.MY_TEAM_ID)
    current_names = [p["name"] for p in team["starters"] + team["bench"]]
    current_set = set(current_names)
    team_lookup = {p["name"]: p for p in team["starters"] + team["bench"]}

    # Ensure every current player is in the pool: add missing ones (bench fodder
    # with no model prediction) with 0 predicted points, so they're correctly
    # counted as "held" rather than phantom transfers.
    extra_rows = []
    for n in current_names:
        if n in df["full_name"].values:
            continue
        p = team_lookup.get(n, {})
        if not p.get("position"):
            continue
        team_name = config.TEAM_MAP.get(p.get("team"), p.get("team"))
        cost = float(p.get("cost") or 0.0) * 10
        for w in weeks:
            extra_rows.append({
                "full_name": n,
                "position": p.get("position"),
                "team_name": team_name,
                "current_fpl_cost": cost,
                "gameweek": w,
                "predicted_points": 0.0,
            })
    if extra_rows:
        df = pd.concat([df, pd.DataFrame(extra_rows)], ignore_index=True)

    # Budget = current team value + money in the bank (in 0.1m units).
    budget = int((team.get("team_value", 100.0) + team.get("bank", 0.0)) * 10)

    fixtures_map = get_upcoming_fixtures(gameweek, num_weeks)
    raw_map = mapped_to_raw_team_map()

    def attach_fixtures(pd_player):
        raw = raw_map.get(pd_player.get("team"), pd_player.get("team"))
        pd_player["fixtures"] = fixtures_map.get(raw, [])
        return pd_player

    # Baseline: keep the current squad (0 transfers).
    _, _, base_total = optimize_squad(
        df, budget=budget, current_names=current_set, num_transfers=0
    )
    baseline = round(base_total, 1)

    options = {}
    for t in range(1, max_transfers + 1):
        opts = []
        forbidden = []
        for _ in range(num_options):
            squad, _, total = optimize_squad(
                df, budget=budget, current_names=current_set,
                num_transfers=t, forbid_squads=forbidden,
            )
            squad_names = {p["name"] for p in squad}
            out = [n for n in current_names if n not in squad_names]
            ins = [n for n in squad_names if n not in current_set]
            if not ins:
                break
            opts.append({
                "expected_points": round(total, 1),
                "gain": round(total - baseline, 1),
                "out": [_player_dict(df, n, team_lookup) for n in out],
                "in": [attach_fixtures(_player_dict(df, n, team_lookup)) for n in ins],
            })
            forbidden.append(squad_names)
        options[str(t)] = opts

    current_squad = [_player_dict(df, n, team_lookup) for n in current_names]

    # Wildcard squad: best 15-man team from scratch with a £100m budget.
    wildcard_squad, wildcard_weeks, wildcard_total = optimize_squad(df, budget=1000)

    output = {
        "gameweek": gameweek,
        "num_weeks": num_weeks,
        "team_name": team.get("team_name"),
        "manager": team.get("manager"),
        "bank": team.get("bank"),
        "team_value": team.get("team_value"),
        "current_squad": current_squad,
        "baseline_points": baseline,
        "options": options,
        "wildcard": {
            "budget": 100.0,
            "total_expected_points": round(wildcard_total, 1),
            "avg_expected_points_per_week": round(wildcard_total / len(weeks), 1),
            "squad": wildcard_squad,
            "weeks": wildcard_weeks,
        },
    }

    path = os.path.join(PUBLISH_DIR, "transfers.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Published {path}")

    print(f"\nTransfer recommendations (over {len(weeks)} GWs, budget £{budget / 10:.1f}m, baseline {baseline} pts):")
    for t, opts in options.items():
        if not opts:
            continue
        best = opts[0]
        outs = ", ".join(p["name"] for p in best["out"])
        ins = ", ".join(p["name"] for p in best["in"])
        print(f"  {t} transfer(s), {len(opts)} option(s): OUT {outs} -> IN {ins}  ({best['expected_points']} pts, +{best['gain']})")

    return output


if __name__ == "__main__":
    gw = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    nw = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    publish_squad(gameweek=gw, num_weeks=nw)
