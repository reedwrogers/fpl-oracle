import json
import os
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from scipy.stats import spearmanr

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PUBLISH_DIR = "/var/www/reedrogers/data"
UNDERSTAT_SEASON = "2026"


def predict(gameweek: int, verbose: bool = True):
    files = os.listdir(DATA_DIR)
    pattern = re.compile(r"^(X|y)_(\d+)\.csv$")

    file_map: dict[int, dict[str, str]] = {}
    for f in files:
        match = pattern.match(f)
        if match:
            kind, num = match.groups()
            file_map.setdefault(int(num), {})[kind] = f

    # Collect paired X/y data
    all_pairs = {
        gw: pair
        for gw, pair in file_map.items()
        if "X" in pair and "y" in pair
    }

    # Prefer prior gameweeks only; fall back to all available if none exist
    prior_gws = [gw for gw in all_pairs if gw < gameweek]
    train_gws = prior_gws if prior_gws else sorted(all_pairs.keys())
    if not train_gws:
        raise ValueError("No training data found.")

    merged_dfs = []
    for gw in train_gws:
        pair = all_pairs[gw]
        X = pd.read_csv(os.path.join(DATA_DIR, pair["X"]))
        y = pd.read_csv(os.path.join(DATA_DIR, pair["y"]))
        merged = X.merge(y, on="full_name", how="inner")
        merged = merged[merged["minutes_last_3"] >= 180]
        merged["gameweek"] = gw
        merged_dfs.append(merged)

    if not merged_dfs:
        raise ValueError("No training data found.")

    train_df = pd.concat(merged_dfs, ignore_index=True)

    if verbose:
        print(
            f"Training on {len(train_df)} instances across {len(merged_dfs)} gameweeks"
        )

    X_train = train_df.drop(columns=["gw_points", "gw_minutes", "full_name", "gameweek"])
    y_train = train_df["gw_points"]

    categorical = ["team_name", "player_position"]
    numeric = X_train.columns.difference(categorical)

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
            ("num", "passthrough", numeric),
        ]
    )

    model = Pipeline(
        steps=[
            ("prep", preprocessor),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.01,
                    max_depth=4,
                    max_iter=200,
                    min_samples_leaf=20,
                    random_state=42,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    if gameweek not in file_map or "X" not in file_map[gameweek]:
        raise ValueError(f"X_{gameweek}.csv not found")

    X_latest = pd.read_csv(os.path.join(DATA_DIR, file_map[gameweek]["X"]))
    X_latest_filtered = X_latest[X_latest["minutes_last_3"] >= 180]
    if len(X_latest_filtered) == 0:
        X_latest_filtered = X_latest

    preds = model.predict(
        X_latest_filtered.drop(columns=["full_name", "gw_minutes"], errors="ignore")
    )

    pred_df = pd.DataFrame(
        {
            "full_name": X_latest_filtered["full_name"],
            "team_name": X_latest_filtered["team_name"],
            "position": X_latest_filtered["player_position"],
            "predicted_points": np.round(preds, 2),
        }
    )

    metrics = None
    if "y" in file_map.get(gameweek, {}):
        y_actual = pd.read_csv(os.path.join(DATA_DIR, file_map[gameweek]["y"]))
        pred_df = pred_df.merge(
            y_actual[["full_name", "gw_points"]], on="full_name", how="left"
        )
        pred_df.rename(columns={"gw_points": "actual_points"}, inplace=True)
        metrics = _evaluate(pred_df, gameweek, verbose=verbose)

    return pred_df.sort_values("predicted_points", ascending=False), metrics


def _evaluate(pred_df, gameweek, verbose=True):
    scored = pred_df.dropna(subset=["actual_points"])

    def tier_mae(df, label):
        if len(df) == 0:
            return None
        return mean_absolute_error(df["actual_points"], df["predicted_points"])

    mae = mean_absolute_error(scored["actual_points"], scored["predicted_points"])
    rmse = np.sqrt(mean_squared_error(scored["actual_points"], scored["predicted_points"]))
    r2 = r2_score(scored["actual_points"], scored["predicted_points"])
    rho, p_value = spearmanr(scored["actual_points"], scored["predicted_points"])

    top_predicted = set(scored.nlargest(20, "predicted_points")["full_name"])
    top_actual = set(scored.nlargest(20, "actual_points")["full_name"])
    overlap = len(top_predicted & top_actual)

    if verbose:
        baseline_mae = np.mean(
            np.abs(scored["actual_points"] - scored["actual_points"].mean())
        )
        print(f"\nGW{gameweek} Evaluation ({len(scored)} players with actuals)")
        print(f"  Overall MAE: {mae:.2f}  (baseline: {baseline_mae:.2f})")
        print(f"  RMSE       : {rmse:.2f}")
        print(f"  R\N{SUPERSCRIPT TWO}         : {r2:.3f}")
        print(f"  Spearman   : {rho:.3f}  (p={p_value:.3f})")
        print("  ---")
        t0 = tier_mae(scored[scored["actual_points"] == 0], "0 pts")
        if t0 is not None:
            print(
                f"    0 pts ({len(scored[scored['actual_points'] == 0]):>3} players): MAE {t0:.2f}"
            )
        t15 = tier_mae(
            scored[
                (scored["actual_points"] >= 1) & (scored["actual_points"] <= 5)
            ],
            "1-5 pts",
        )
        if t15 is not None:
            print(
                f"    1-5 pts ({len(scored[(scored['actual_points'] >= 1) & (scored['actual_points'] <= 5)]):>3} players): MAE {t15:.2f}"
            )
        t6 = tier_mae(scored[scored["actual_points"] >= 6], "6+ pts (haul)")
        if t6 is not None:
            print(
                f"    6+ pts (haul) ({len(scored[scored['actual_points'] >= 6]):>3} players): MAE {t6:.2f}"
            )
        print(f"  Top-20 Precision: {overlap}/20 players correctly identified")

    return {
        "gameweek": gameweek,
        "n_players": len(scored),
        "mae": round(mae, 2),
        "rmse": round(rmse, 2),
        "r2": round(r2, 3),
        "spearman": round(rho, 3),
        "top20_precision": f"{overlap}/20",
    }


def find_latest_gameweek():
    pattern = re.compile(r"^X_(\d+)\.csv$")
    gws = []
    for f in os.listdir(DATA_DIR):
        m = pattern.match(f)
        if m:
            gws.append(int(m.group(1)))
    if not gws:
        raise ValueError("No X_*.csv files found in data/")

    latest = max(gws)
    # During pre-season, prefer GW 1 over stale old-season gameweeks
    y_path = os.path.join(DATA_DIR, f"y_{latest}.csv")
    if not os.path.exists(y_path) and 1 in gws:
        return 1
    return latest


def compute_metrics_json(gameweek=None):
    if gameweek is None:
        gameweek = find_latest_gameweek()

    files = os.listdir(DATA_DIR)
    pattern = re.compile(r"^(X|y)_(\d+)\.csv$")

    file_map: dict[int, dict[str, str]] = {}
    for f in files:
        match = pattern.match(f)
        if match:
            kind, num = match.groups()
            file_map.setdefault(int(num), {})[kind] = f

    test_gws = sorted(
        n
        for n in file_map
        if "X" in file_map[n] and "y" in file_map[n]
    )

    backtest = []
    for gw_test in test_gws:
        try:
            _, metrics = predict(gw_test, verbose=False)
            if metrics:
                backtest.append(metrics)
        except ValueError:
            continue

    latest_eval = backtest[-1] if backtest else {}

    return {
        "last_updated": datetime.now().isoformat(),
        "current_gameweek": gameweek,
        "total_training_gws": len(test_gws),
        "latest_eval": latest_eval,
        "backtest": backtest,
    }


def publish(gameweek=None):
    if gameweek is None:
        gameweek = find_latest_gameweek()

    print(f"Publishing predictions for GW {gameweek}...")

    pred_df, _ = predict(gameweek, verbose=True)

    files = os.listdir(DATA_DIR)
    pattern = re.compile(r"^(X|y)_(\d+)\.csv$")
    file_map: dict[int, dict[str, str]] = {}
    for f in files:
        match = pattern.match(f)
        if match:
            kind, num = match.groups()
            file_map.setdefault(int(num), {})[kind] = f

    csv_df = pred_df[["full_name", "team_name", "position", "predicted_points"]].copy()
    csv_df["actual_points"] = pred_df.get("actual_points", "")
    csv_df["gameweek"] = gameweek

    # Merge cost from X data
    X_path = os.path.join(DATA_DIR, f"X_{gameweek}.csv")
    if os.path.exists(X_path):
        X = pd.read_csv(X_path)
        csv_df = csv_df.merge(
            X[["full_name", "current_fpl_cost"]].drop_duplicates(subset="full_name"),
            on="full_name", how="left"
        )
        csv_df["cost"] = (csv_df["current_fpl_cost"].fillna(0) / 10).round(1)
    else:
        csv_df["cost"] = ""

    if "y" in file_map.get(gameweek, {}):
        y_actual = pd.read_csv(os.path.join(DATA_DIR, file_map[gameweek]["y"]))
        csv_df = csv_df.merge(
            y_actual[["full_name", "gw_minutes"]], on="full_name", how="left"
        )
        csv_df["gw_minutes"] = csv_df["gw_minutes"].fillna(0).astype(int)
    else:
        csv_df["gw_minutes"] = ""

    csv_df = csv_df[
        [
            "full_name",
            "team_name",
            "position",
            "predicted_points",
            "actual_points",
            "gameweek",
            "gw_minutes",
            "cost",
        ]
    ]

    os.makedirs(PUBLISH_DIR, exist_ok=True)

    csv_path = os.path.join(PUBLISH_DIR, "predictions.csv")
    csv_df.to_csv(csv_path, index=False)
    print(f"  Wrote {csv_path} ({len(csv_df)} players)")

    snapshot_path = os.path.join(
        PUBLISH_DIR, f"predictions_{UNDERSTAT_SEASON}_{gameweek}.csv"
    )
    csv_df.to_csv(snapshot_path, index=False)
    print(f"  Archived {snapshot_path}")

    metrics = compute_metrics_json(gameweek)
    metrics_path = os.path.join(PUBLISH_DIR, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  Wrote {metrics_path}")

    # Run optimizer (default: optimize across next 3 GWs for fixture-proof squad)
    print("  Running team optimizer...")
    try:
        from optimize import publish_squad
        publish_squad(gameweek=gameweek, num_weeks=5)
    except Exception as e:
        print(f"  Optimizer skipped: {e}")

    return csv_df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python model.py <gameweek> [--backtest] [--publish]")
        print("  python model.py 5                  predict & evaluate GW 5")
        print("  python model.py 5 --backtest       evaluate all GWs up to 5")
        print("  python model.py 5 --publish        predict GW 5 and publish to web")
        print("  python model.py --publish-latest   auto-detect latest GW and publish")
        sys.exit(1)

    publish_mode = "--publish" in sys.argv
    publish_latest = "--publish-latest" in sys.argv
    backtest = "--backtest" in sys.argv

    if publish_latest:
        gw = find_latest_gameweek()
        publish(gw)
    elif publish_mode:
        gw = int(sys.argv[1])
        publish(gw)
    elif backtest:
        gw = int(sys.argv[1])
        pattern = re.compile(r"^(X)_(\d+)\.csv$")
        all_gws = set()
        for f in os.listdir(DATA_DIR):
            m = pattern.match(f)
            if m:
                all_gws.add(int(m.group(2)))

        test_gws = sorted(
            n
            for n in all_gws
            if n <= gw and os.path.exists(os.path.join(DATA_DIR, f"y_{n}.csv"))
        )
        print(f"Backtesting {len(test_gws)} gameweeks up to GW {gw}...")
        for gw_test in test_gws:
            try:
                predict(gw_test)
            except ValueError as e:
                print(f"  Skipping GW {gw_test}: {e}")
    else:
        gw = int(sys.argv[1])
        pred_df, _ = predict(gw)
        print(f"\nTop 10 predicted for GW {gw}:")
        print(pred_df.head(10).to_string(index=False))
