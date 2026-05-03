import os
import re
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from scipy.stats import spearmanr

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

def predict(gameweek: int):
    files = os.listdir(DATA_DIR)
    pattern = re.compile(r'^(X|y)_(\d+)\.csv$')

    file_map = {}
    for f in files:
        match = pattern.match(f)
        if match:
            kind, num = match.groups()
            file_map.setdefault(int(num), {})[kind] = f

    merged_dfs = []
    for gw, pair in file_map.items():
        if gw < gameweek and "X" in pair and "y" in pair:
            X = pd.read_csv(os.path.join(DATA_DIR, pair["X"]))
            y = pd.read_csv(os.path.join(DATA_DIR, pair["y"]))
            merged = X.merge(y, on="full_name", how="inner")
            merged = merged[merged["minutes_last_3"] >= 180]
            merged["gameweek"] = gw
            merged_dfs.append(merged)

    if not merged_dfs:
        raise ValueError("No training data found.")

    train_df = pd.concat(merged_dfs, ignore_index=True)
    print(f"Training on {len(train_df)} instances across {len(merged_dfs)} gameweeks")

    X_train = train_df.drop(columns=["gw_points", "gw_minutes", "full_name", "gameweek"])
    y_train = train_df["gw_points"]

    categorical = ["team_name", "player_position"]
    numeric = X_train.columns.difference(categorical)

    preprocessor = ColumnTransformer(transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ("num", "passthrough", numeric),
    ])

    model = Pipeline(steps=[
        ("prep", preprocessor),
        ("model", HistGradientBoostingRegressor(
            learning_rate=0.01,
            max_depth=4,
            max_iter=200,
            min_samples_leaf=20,
            random_state=42
        ))
    ])

    model.fit(X_train, y_train)

    if gameweek not in file_map or "X" not in file_map[gameweek]:
        raise ValueError(f"X_{gameweek}.csv not found")

    X_latest = pd.read_csv(os.path.join(DATA_DIR, file_map[gameweek]["X"]))
    X_latest = X_latest[X_latest["minutes_last_3"] >= 180]

    preds = model.predict(X_latest.drop(columns=["full_name", "gw_minutes"], errors="ignore"))

    pred_df = pd.DataFrame({
        "full_name":        X_latest["full_name"],
        "team_name":        X_latest["team_name"],
        "position":         X_latest["player_position"],
        "predicted_points": np.round(preds, 2),
    })

    if "y" in file_map.get(gameweek, {}):
        y_actual = pd.read_csv(os.path.join(DATA_DIR, file_map[gameweek]["y"]))
        pred_df = pred_df.merge(y_actual[["full_name", "gw_points"]], on="full_name", how="left")
        pred_df.rename(columns={"gw_points": "actual_points"}, inplace=True)
        _evaluate(pred_df, gameweek)

    return pred_df.sort_values("predicted_points", ascending=False)


def _evaluate(pred_df, gameweek):
    scored = pred_df.dropna(subset=["actual_points"])

    def tier_mae(df, label):
        if len(df) == 0:
            return
        m = mean_absolute_error(df["actual_points"], df["predicted_points"])
        print(f"    {label} ({len(df):>3} players): MAE {m:.2f}")

    mae = mean_absolute_error(scored["actual_points"], scored["predicted_points"])
    rmse = np.sqrt(mean_squared_error(scored["actual_points"], scored["predicted_points"]))
    r2 = r2_score(scored["actual_points"], scored["predicted_points"])
    rho, p_value = spearmanr(scored["actual_points"], scored["predicted_points"])

    baseline_mae = np.mean(np.abs(scored["actual_points"] - scored["actual_points"].mean()))
    print(f"\nGW{gameweek} Evaluation ({len(scored)} players with actuals)")
    print(f"  Overall MAE: {mae:.2f}  (baseline: {baseline_mae:.2f})")
    print(f"  RMSE       : {rmse:.2f}")
    print(f"  R²         : {r2:.3f}")
    print(f"  Spearman   : {rho:.3f}  (p={p_value:.3f})")
    print(f"  ---")
    tier_mae(scored[scored["actual_points"] == 0], "0 pts")
    tier_mae(scored[(scored["actual_points"] >= 1) & (scored["actual_points"] <= 5)], "1-5 pts")
    tier_mae(scored[scored["actual_points"] >= 6], "6+ pts (haul)")

    top_n_precision(scored, n=20)


def top_n_precision(df, n=20):
    top_predicted = set(df.nlargest(n, "predicted_points")["full_name"])
    top_actual = set(df.nlargest(n, "actual_points")["full_name"])
    overlap = len(top_predicted & top_actual)
    print(f"  Top-{n} Precision: {overlap}/{n} players correctly identified")


if __name__ == "__main__":
    pred_df = predict(35)
    print(f"\nTop 10 predicted:\n{pred_df.head(10).to_string(index=False)}")
