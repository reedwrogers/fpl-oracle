#!/usr/bin/env python3
"""FPL Team Optimizer — integer programming via pulp.

Usage:
  python optimize.py                # optimize for current GW
  python optimize.py --weeks 3      # optimize across next 3 GWs (averaged predictions)
"""

import json
import os
import sys

import numpy as np
import pandas as pd
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, PULP_CBC_CMD

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PUBLISH_DIR = "/var/www/reedrogers/data"
PREDICTIONS_PATH = os.path.join(PUBLISH_DIR, "predictions.csv")


def load_players(gameweek=None, num_weeks=1):
    if num_weeks > 1:
        preds = []
        for gw in range(gameweek, gameweek + num_weeks):
            x_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
            if not os.path.exists(x_path):
                print(f"  X_{gw}.csv missing, skipping")
                continue
            X = pd.read_csv(x_path)
            # predict each GW via model
            from model import predict as model_predict
            pred_df, _ = model_predict(gw, verbose=False)
            X_sub = X[["full_name", "current_fpl_cost", "team_name", "player_position"]]
            merged = pred_df.merge(X_sub, on="full_name", how="left")
            merged["position"] = merged["position"].fillna(merged["player_position"])
            merged["team_name"] = merged["team_name_x"].fillna(merged["team_name_y"])
            merged["gameweek"] = gw
            preds.append(merged)
            print(f"  GW {gw}: {len(merged)} players predicted")

        all_preds = pd.concat(preds, ignore_index=True)
        # Average predicted_points per player across gameweeks
        avg = all_preds.groupby(
            ["full_name", "team_name", "position", "current_fpl_cost"]
        )["predicted_points"].mean().reset_index()
        avg = avg.rename(columns={"predicted_points": "avg_predicted_points"})

        # Also keep the first GW's predicted_points for reference
        gw1_preds = preds[0][["full_name", "predicted_points"]].rename(
            columns={"predicted_points": "predicted_points_gw1"}
        )
        avg = avg.merge(gw1_preds, on="full_name", how="left")

        avg = avg.dropna(subset=["current_fpl_cost", "position", "team_name"])
        avg = avg.reset_index(drop=True)

        actual_gw = gameweek
        print(f"  Averaged across {len(preds)} GWs: {len(avg)} players")
        return avg, actual_gw, True
    else:
        pred_df = pd.read_csv(PREDICTIONS_PATH)
        gw = int(pred_df["gameweek"].iloc[0]) if gameweek is None else gameweek

        x_path = os.path.join(DATA_DIR, f"X_{gw}.csv")
        if not os.path.exists(x_path):
            raise FileNotFoundError(f"{x_path} not found.")

        X = pd.read_csv(x_path)
        df = pred_df.merge(
            X[["full_name", "current_fpl_cost", "team_name", "player_position"]],
            on="full_name", how="left",
        )
        df["position"] = df["position"].fillna(df["player_position"])
        df["team_name"] = df["team_name_x"].fillna(df["team_name_y"])
        df = df.dropna(subset=["current_fpl_cost", "position", "team_name"])
        df = df.reset_index(drop=True)
        return df, gw, False


def _build_squad(players, budget=1000, points_col="predicted_points"):
    prob = LpProblem("Squad", LpMaximize)
    indices = players.index.tolist()
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in indices}
    prob += lpSum(x[i] * players.loc[i, points_col] for i in indices)
    prob += lpSum(x[i] for i in indices) == 15
    for pos, count in [
        ("Goalkeeper", 2), ("Defender", 5), ("Midfielder", 5), ("Forward", 3)
    ]:
        prob += lpSum(
            x[i] for i in indices if players.loc[i, "position"] == pos
        ) == count
    for team in players["team_name"].unique():
        prob += lpSum(
            x[i] for i in indices if players.loc[i, "team_name"] == team
        ) <= 3
    prob += lpSum(
        x[i] * players.loc[i, "current_fpl_cost"] for i in indices
    ) <= budget
    prob.solve(PULP_CBC_CMD(msg=False))
    selected = [i for i in indices if x[i].value() == 1]
    return players.loc[selected].copy()


def _starting_xi(squad, def_count, mid_count, fwd_count, points_col="predicted_points"):
    prob = LpProblem("XI", LpMaximize)
    indices = squad.index.tolist()
    start = {i: LpVariable(f"s_{i}", cat="Binary") for i in indices}
    cap = {i: LpVariable(f"c_{i}", cat="Binary") for i in indices}
    prob += lpSum(
        start[i] * squad.loc[i, points_col] for i in indices
    ) + lpSum(cap[i] * squad.loc[i, points_col] for i in indices)
    prob += lpSum(start[i] for i in indices) == 11
    prob += lpSum(cap[i] for i in indices) == 1
    for i in indices:
        prob += cap[i] <= start[i]
    gk = [i for i in indices if squad.loc[i, "position"] == "Goalkeeper"]
    df = [i for i in indices if squad.loc[i, "position"] == "Defender"]
    md = [i for i in indices if squad.loc[i, "position"] == "Midfielder"]
    fw = [i for i in indices if squad.loc[i, "position"] == "Forward"]
    prob += lpSum(start[i] for i in gk) == 1
    prob += lpSum(start[i] for i in df) == def_count
    prob += lpSum(start[i] for i in md) == mid_count
    prob += lpSum(start[i] for i in fw) == fwd_count
    prob.solve(PULP_CBC_CMD(msg=False))
    starters = [i for i in indices if start[i].value() == 1]
    captain_idx = next(i for i in indices if cap[i].value() == 1)
    bench = [i for i in indices if i not in starters]

    def _players(ilist, role_fn):
        row_data = []
        for i in ilist:
            d = {"name": squad.loc[i, "full_name"],
                 "team": squad.loc[i, "team_name"],
                 "position": squad.loc[i, "position"],
                 "cost": round(squad.loc[i, "current_fpl_cost"] / 10, 1),
                 "points": round(squad.loc[i, points_col], 1),
                 "role": role_fn(i)}
            # Include per-GW breakdown if available
            gw1_col = "predicted_points_gw1"
            if gw1_col in squad.columns:
                d["gw_points"] = [
                    round(squad.loc[i, gw1_col], 1),
                ]
            row_data.append(d)
        return row_data

    starters_data = _players(starters, lambda i: "captain" if i == captain_idx else "")
    bench_data = _players(bench, lambda _: "")

    expected = round(sum(
        squad.loc[i, points_col] * (2 if i == captain_idx else 1) for i in starters
    ), 1)
    total_cost = round(squad["current_fpl_cost"].sum() / 10, 1)

    return {
        "formation": f"{def_count}-{mid_count}-{fwd_count}",
        "expected_points": expected,
        "cost": total_cost,
        "starters": starters_data,
        "bench": bench_data,
    }


def compute_all_formations(players, points_col="predicted_points"):
    squad = _build_squad(players, points_col=points_col)
    formations = []
    for d, m, f in [
        (3, 4, 3), (3, 5, 2), (4, 3, 3), (4, 4, 2), (4, 5, 1), (5, 3, 2), (5, 4, 1)
    ]:
        try:
            result = _starting_xi(squad, d, m, f, points_col=points_col)
            formations.append(result)
        except Exception:
            continue
    formations.sort(key=lambda x: x["expected_points"], reverse=True)
    return formations


def publish_squad(gameweek=None, num_weeks=1):
    players, gw, is_multi = load_players(gameweek, num_weeks)

    points_col = "avg_predicted_points" if is_multi else "predicted_points"
    label = f"{num_weeks}-GW average" if is_multi else f"GW {gw}"

    print(f"Optimizing squad for {label} ({len(players)} players)...")
    formations = compute_all_formations(players, points_col=points_col)

    # For multi-GW, include per-GW breakdowns
    output = {
        "gameweek": gw,
        "num_weeks": num_weeks,
        "formations": formations,
    }
    if is_multi and len(formations) > 0:
        output["note"] = f"Optimized across GWs {gw}–{gw + num_weeks - 1} using averaged predictions"

    path = os.path.join(PUBLISH_DIR, "squad.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Published {path} — {len(formations)} formations")

    best = formations[0]
    print(f"\nBest: {best['formation']} — £{best['cost']}m — {best['expected_points']} pts ({label})")
    for p in best["starters"]:
        role = " (C)" if p["role"] == "captain" else ""
        print(f"  {p['name']:<35s} {p['team']:<12s} £{p['cost']}m  {p['points']} pts{role}")
    print("Bench:")
    for p in best["bench"]:
        print(f"  {p['name']:<35s} {p['team']:<12s} £{p['cost']}m  {p['points']} pts")


if __name__ == "__main__":
    gw = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    nw = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    publish_squad(gameweek=gw, num_weeks=nw)
