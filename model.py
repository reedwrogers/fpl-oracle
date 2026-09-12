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

import config

DATA_DIR = config.DATA_DIR
PUBLISH_DIR = config.PUBLISH_DIR
UNDERSTAT_SEASON = config.SEASON


def _minutes_threshold(gameweek: int) -> int:
    """Minimum recent minutes for a player to be considered 'established'.

    Scales with the number of games played: 60% of the available minutes in the
    rolling last-3-game window. That's ~1 game at GW2, ~2 games at GW3, and 3
    games (162 min) from GW4 onward."""
    window = min(max(gameweek - 1, 1), 3)
    return round(0.6 * window * 90)


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

    # Train on every completed gameweek except the target (avoids leakage and
    # carries prior-season data forward across the season reset).
    train_gws = sorted(gw for gw in all_pairs if gw != gameweek)
    if not train_gws:
        raise ValueError("No training data found.")

    merged_dfs = []
    for gw in train_gws:
        pair = all_pairs[gw]
        X = pd.read_csv(os.path.join(DATA_DIR, pair["X"]))
        y = pd.read_csv(os.path.join(DATA_DIR, pair["y"]))
        merged = X.merge(y, on="full_name", how="inner")
        filtered = merged[merged["minutes_last_3"] >= _minutes_threshold(gw)].copy()
        if filtered.empty:
            continue
        filtered["gameweek"] = gw
        merged_dfs.append(filtered)

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
    X_latest_filtered = X_latest[X_latest["minutes_last_3"] >= _minutes_threshold(gameweek)]
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


def _round(v, nd=3):
    """Round a float to nd places; NaN/inf -> None for JSON safety."""
    try:
        v = float(v)
        return round(v, nd) if np.isfinite(v) else None
    except (TypeError, ValueError):
        return None


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

    def pos_summary(sub):
        if len(sub) == 0:
            return None
        rho_s, _ = spearmanr(sub["actual_points"], sub["predicted_points"])
        return {
            "n": int(len(sub)),
            "mae": _round(mean_absolute_error(sub["actual_points"], sub["predicted_points"]), 2),
            "rmse": _round(np.sqrt(mean_squared_error(sub["actual_points"], sub["predicted_points"])), 2),
            "spearman": _round(rho_s, 3),
        }

    by_position = {}
    if "position" in scored.columns:
        for pos in ["Goalkeeper", "Defender", "Midfielder", "Forward"]:
            s = pos_summary(scored[scored["position"] == pos])
            if s is not None:
                by_position[pos] = s

    if verbose:
        baseline_mae = np.mean(
            np.abs(scored["actual_points"] - scored["actual_points"].mean())
        )
        print(f"\nGW{gameweek} Evaluation ({len(scored)} players with actuals)")
        print(f"  Overall MAE: {mae:.2f}  (baseline: {baseline_mae:.2f})")
        print(f"  RMSE       : {rmse:.2f}")
        print(f"  R\N{SUPERSCRIPT TWO}         : {r2:.3f}")
        print(f"  Spearman   : {rho:.3f}  (p={p_value:.3f})")
        for pos, s in by_position.items():
            print(
                f"    {pos:<12s} n={s['n']:>3}  MAE {s['mae']}  RMSE {s['rmse']}  Spearman {s['spearman']}"
            )
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
        "mae": _round(mae, 2),
        "rmse": _round(rmse, 2),
        "r2": _round(r2, 3),
        "spearman": _round(rho, 3),
        "top20_precision": f"{overlap}/20",
        "by_position": by_position,
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

    # Predict the live gameweek while it is still in progress, otherwise the
    # upcoming gameweek. Pre-season this points at GW 1 (already generated by
    # `pipeline.py preseason`); in-season it advances only as each gameweek
    # finishes, never publishing a future gameweek early.
    try:
        from fpl import (
            get_current_gameweek,
            get_latest_finished_gameweek,
            get_next_gameweek,
        )

        live = get_current_gameweek()
        finished = get_latest_finished_gameweek()
        if (
            live is not None
            and live > finished
            and os.path.exists(os.path.join(DATA_DIR, f"X_{live}.csv"))
        ):
            return live

        upcoming = get_next_gameweek()
    except Exception:
        upcoming = None

    if upcoming is not None and os.path.exists(
        os.path.join(DATA_DIR, f"X_{upcoming}.csv")
    ):
        return upcoming

    if 1 in gws:
        return 1
    return max(gws)


def _chronological_gws(gws) -> list[int]:
    """Order gameweeks across the season reset: prior-season gameweeks (the
    tail we retain, gw >= 20) first, then the current season (gw < 20)."""
    gws = sorted(set(int(g) for g in gws))
    prior = [g for g in gws if g >= 20]
    current = [g for g in gws if g < 20]
    return prior + current


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

    test_gws = _chronological_gws(
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


def write_correlation_json() -> None:
    """Correlation matrix of every numeric feature across the full dataset,
    written for the site's heatmap."""
    pattern = re.compile(r"^X_(\d+)\.csv$")
    frames = []
    for f in sorted(os.listdir(DATA_DIR)):
        if not pattern.match(f):
            continue
        try:
            frames.append(pd.read_csv(os.path.join(DATA_DIR, f)))
        except Exception:
            continue
    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)
    exclude = {"full_name", "team_name", "player_position", "gameweek"}
    numeric = [
        c for c in df.columns
        if c not in exclude and pd.api.types.is_numeric_dtype(df[c]) and df[c].std() > 0
    ]

    corr = df[numeric].corr()
    payload = {
        "columns": list(corr.columns),
        "matrix": corr.round(3).where(pd.notna(corr), None).values.tolist(),
    }
    os.makedirs(PUBLISH_DIR, exist_ok=True)
    path = os.path.join(PUBLISH_DIR, "correlation.json")
    with open(path, "w") as f:
        json.dump(payload, f)
    print(f"  Wrote {path}")


def publish(gameweek=None):
    if gameweek is None:
        gameweek = find_latest_gameweek()

    print(f"Publishing predictions for GW {gameweek}...")

    pred_df, _ = predict(gameweek, verbose=True)

    # Merge predicted points onto the full feature matrix so the published table
    # carries every statistic the model used (no actuals/gw_minutes — those
    # aren't known ahead of a gameweek).
    X_path = os.path.join(DATA_DIR, f"X_{gameweek}.csv")
    X = pd.read_csv(X_path)

    csv_df = pred_df[["full_name", "predicted_points"]].merge(
        X, on="full_name", how="inner"
    )
    csv_df = csv_df.rename(columns={"player_position": "position"})
    csv_df["cost"] = (csv_df["current_fpl_cost"].fillna(0) / 10).round(1)

    feature_cols = [
        c for c in config.FEATURE_COLUMNS
        if c not in ("full_name", "team_name", "player_position", "current_fpl_cost")
    ]
    # Lead with the most-consumed columns; everything else keeps schema order.
    lead_cols = [
        "full_name", "team_name", "position", "predicted_points",
        "xg_per_90", "xag_per_90",
        "team_league_position", "opponent_league_position", "points_last_3",
    ]
    csv_df = csv_df[
        lead_cols + [c for c in (["cost"] + feature_cols) if c not in lead_cols]
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

    write_correlation_json()

    # Run the transfer recommender (anchored to the manager's current squad).
    print("  Running transfer recommender...")
    try:
        from optimize import publish_transfers
        publish_transfers(gameweek=gameweek, num_weeks=5)
    except Exception as e:
        print(f"  Transfer recommender skipped: {e}")

    # Refresh the best-XI squad file so the download never goes stale.
    print("  Running squad optimizer...")
    try:
        from optimize import publish_squad
        publish_squad(gameweek=gameweek, num_weeks=1)
    except Exception as e:
        print(f"  Squad optimizer skipped: {e}")

    # Refresh the mini-league view (standings, ownership, differentials).
    print("  Running mini-league publisher...")
    try:
        from minileague import publish as publish_minileague
        publish_minileague()
    except Exception as e:
        print(f"  Mini-league publisher skipped: {e}")

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
