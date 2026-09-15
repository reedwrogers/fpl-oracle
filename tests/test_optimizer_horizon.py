"""Optimizer horizon stays multi-week when future X files are missing.

load_long used to skip gameweeks without X_<gw>.csv on disk, so a 5-week
transfer ask collapsed to whatever files existed (often just the current GW).
Future weeks are now built in memory (current form + fixture context) and never
written to data/.
"""

import pandas as pd

import config
import features
import model
import optimize


class _DummyModel:
    def predict(self, X):
        return [4.0] * len(X)


def _train_frame():
    return pd.DataFrame(
        {
            "full_name": ["A", "B", "A", "B"],
            "team_name": ["T1", "T2", "T1", "T2"],
            "player_position": ["Forward", "Midfielder", "Forward", "Midfielder"],
            "current_fpl_cost": [100, 90, 100, 90],
            "points_last_3": [5, 3, 6, 2],
            "gw_points": [4, 3, 5, 2],
            "gw_minutes": [90, 90, 90, 90],
            "gameweek": [1, 1, 2, 2],
        }
    )


def test_load_long_covers_weeks_without_x_files(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(optimize, "DATA_DIR", tmp_path)

    pd.DataFrame(
        {
            "full_name": ["A", "B"],
            "team_name": ["T1", "T2"],
            "player_position": ["Forward", "Midfielder"],
            "current_fpl_cost": [100, 90],
        }
    ).to_csv(tmp_path / "X_5.csv", index=False)

    def fake_predict(gw, verbose=False):
        assert gw == 5
        return (
            pd.DataFrame(
                {
                    "full_name": ["A", "B"],
                    "position": ["Forward", "Midfielder"],
                    "team_name": ["T1", "T2"],
                    "predicted_points": [5.0, 3.0],
                }
            ),
            None,
        )

    def fake_build(gw, season=config.SEASON):
        assert gw in (6, 7)
        return pd.DataFrame(
            {
                "full_name": ["A", "B", "C"],
                "team_name": ["T1", "T2", "T3"],
                "player_position": ["Forward", "Midfielder", "Forward"],
                "current_fpl_cost": [100, 90, 80],
                "points_last_3": [5, 3, 9],
            }
        )

    monkeypatch.setattr(model, "predict", fake_predict)
    monkeypatch.setattr(features, "build_features", fake_build)
    monkeypatch.setattr(model, "_training_data", lambda exclude_gw=None: _train_frame())
    monkeypatch.setattr(model, "_fit", lambda df, verbose=False: _DummyModel())

    df = optimize.load_long(5, 3)

    assert sorted(df["gameweek"].unique()) == [5, 6, 7]
    # Pool anchored to the current GW: C never enters the optimizer.
    assert set(df["full_name"]) == {"A", "B"}
    assert len(df) == 6
    # In-memory weeks leave no files behind.
    assert not (tmp_path / "X_6.csv").exists()
    assert not (tmp_path / "X_7.csv").exists()


def test_load_long_skips_unbuildable_future_week(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(optimize, "DATA_DIR", tmp_path)

    pd.DataFrame(
        {
            "full_name": ["A"],
            "team_name": ["T1"],
            "player_position": ["Forward"],
            "current_fpl_cost": [100],
        }
    ).to_csv(tmp_path / "X_5.csv", index=False)

    monkeypatch.setattr(
        model,
        "predict",
        lambda gw, verbose=False: (
            pd.DataFrame(
                {
                    "full_name": ["A"],
                    "position": ["Forward"],
                    "team_name": ["T1"],
                    "predicted_points": [5.0],
                }
            ),
            None,
        ),
    )

    def fake_build(gw, season=config.SEASON):
        if gw == 6:
            raise ValueError("no fixtures that far out")
        return pd.DataFrame(
            {
                "full_name": ["A"],
                "team_name": ["T1"],
                "player_position": ["Forward"],
                "current_fpl_cost": [100],
                "points_last_3": [5],
            }
        )

    monkeypatch.setattr(features, "build_features", fake_build)
    monkeypatch.setattr(model, "_training_data", lambda exclude_gw=None: _train_frame())
    monkeypatch.setattr(model, "_fit", lambda df, verbose=False: _DummyModel())

    df = optimize.load_long(5, 3)

    assert sorted(df["gameweek"].unique()) == [5, 7]
