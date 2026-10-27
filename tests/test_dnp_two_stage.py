"""Availability flags + two-stage (DNP-aware) predictions.

A Doubtful/injured player must predict below an identical available one:
stage A (will he play?) gates stage B (how many points if he plays).
"""

import numpy as np
import pandas as pd

import config
import features
import fpl
import model


def _fake_bootstrap(*, status="a", chance=None):
    return {
        "elements": [
            {
                "id": 1,
                "first_name": "Fit",
                "second_name": "Player",
                "team": 1,
                "element_type": 3,
                "now_cost": 80,
                "minutes": 500,
                "selected_by_percent": 10.0,
                "influence": 100.0,
                "creativity": 50.0,
                "threat": 60.0,
                "ict_index": 20.0,
                "penalties_order": None,
                "status": status,
                "chance_of_playing_this_round": chance,
                "chance_of_playing_next_round": chance,
                "news": "",
            }
        ],
        "teams": [{"id": 1, "name": "Arsenal"}],
        "element_types": [{"id": 3, "singular_name": "Midfielder"}],
    }


def test_get_players_carries_availability_flags(monkeypatch):
    monkeypatch.setattr(
        fpl, "_get_json", lambda url: _fake_bootstrap(status="d", chance=25)
    )
    df = fpl.get_players()
    assert df["player_status"].iloc[0] == "d"
    assert df["chance_of_playing_this_round"].iloc[0] == 25
    assert df["chance_of_playing_next_round"].iloc[0] == 25


def _train_frame(n_per_group=40):
    rng = np.random.RandomState(7)
    rows = []
    for i in range(n_per_group):
        rows.append({  # available regular: plays, scores
            "full_name": "Avail",
            "team_name": "T1",
            "player_position": "Midfielder",
            "player_status": "a",
            "chance_of_playing_this_round": 100.0,
            "chance_of_playing_next_round": 100.0,
            "points_last_3": 12,
            "minutes_last_3": 270,
            "gw_points": int(rng.randint(5, 10)),
            "gw_minutes": 90,
            "gameweek": 1,
        })
        rows.append({  # injured: misses games, scores 0
            "full_name": "Hurt",
            "team_name": "T1",
            "player_position": "Midfielder",
            "player_status": "i",
            "chance_of_playing_this_round": 0.0,
            "chance_of_playing_next_round": 0.0,
            "points_last_3": 12,
            "minutes_last_3": 270,
            "gw_points": 0,
            "gw_minutes": 0,
            "gameweek": 1,
        })
    return pd.DataFrame(rows)


def _profile(name, status, chance):
    return pd.DataFrame([{
        "full_name": name,
        "team_name": "T1",
        "player_position": "Midfielder",
        "player_status": status,
        "chance_of_playing_this_round": chance,
        "chance_of_playing_next_round": chance,
        "points_last_3": 12,
        "minutes_last_3": 270,
    }])


def test_flagged_dnp_predicts_below_identical_available_player():
    m = model._fit(_train_frame(), verbose=False)
    avail = m.predict(_profile("Avail", "a", 100.0))[0]
    hurt = m.predict(_profile("Hurt", "i", 0.0))[0]
    assert hurt < avail
    assert hurt < 1.0  # gated by P(plays) ~= 0, not by the 5-9 point scale


def test_old_schema_frame_without_flag_columns_still_predicts():
    # X files written before flags existed lack the columns entirely.
    train = _train_frame().drop(columns=[
        "player_status",
        "chance_of_playing_this_round",
        "chance_of_playing_next_round",
    ])
    m = model._fit(train, verbose=False)
    preds = m.predict(_profile("Avail", "a", 100.0).drop(columns=[
        "player_status",
        "chance_of_playing_this_round",
        "chance_of_playing_next_round",
    ]))
    assert len(preds) == 1 and np.isfinite(preds[0])


def test_flag_toggle_off_neutralises_real_flag_values(monkeypatch):
    monkeypatch.setattr(config, "USE_AVAILABILITY_FLAGS", False)
    df = _profile("Hurt", "i", 0.0)
    out = features._fill_availability_defaults(df)
    assert out["player_status"].iloc[0] == "a"
    assert out["chance_of_playing_this_round"].iloc[0] == 100.0


def test_flag_toggle_on_preserves_real_flag_values(monkeypatch):
    monkeypatch.setattr(config, "USE_AVAILABILITY_FLAGS", True)
    df = _profile("Hurt", "i", 0.0)
    out = features._fill_availability_defaults(df)
    assert out["player_status"].iloc[0] == "i"
    assert out["chance_of_playing_this_round"].iloc[0] == 0.0


def test_all_played_training_frame_predicts():
    train = _train_frame()
    train = train[train["gw_minutes"] > 0].copy()
    assert train["gw_minutes"].gt(0).all()
    m = model._fit(train, verbose=False)
    preds = m.predict(_profile("Avail", "a", 100.0))
    assert len(preds) == 1 and np.isfinite(preds[0])
