"""A live scoreline is not a finished gameweek (GW4 Leeds–Newcastle lesson).

FPL fixtures carry a score as soon as a game kicks off, while ``finished``
flips only after full time. Recording actuals off score presence froze a
30-minute partial snapshot of GW4 to disk, and the write-once guard would
have kept it forever.
"""

import pandas as pd
import pytest

import config
import fpl
import pipeline


def _events():
    return {
        "events": [
            {"id": 1, "is_current": False, "is_next": False, "finished": True},
            {"id": 2, "is_current": False, "is_next": False, "finished": True},
            {"id": 3, "is_current": False, "is_next": False, "finished": True},
            {"id": 4, "is_current": True, "is_next": False, "finished": False},
            {"id": 5, "is_current": False, "is_next": True, "finished": False},
        ],
        "elements": [],
        "teams": [],
        "element_types": [],
    }


def _fixtures(gw4_finished: bool):
    rows = []
    for gw in (3, 4):
        for i in range(2):
            rows.append({
                "event": gw,
                "started": True,
                "finished": True if gw == 3 else gw4_finished,
                "finished_provisional": True,
                "team_h": 1,
                "team_a": 2,
                "team_h_score": 2,
                "team_a_score": 1,
            })
    return rows


@pytest.fixture
def fake_api(monkeypatch):
    state = {"gw4_finished": False}

    def fake_get_json(url: str):
        if url.endswith("/bootstrap-static/"):
            return _events()
        if url.endswith("/fixtures/"):
            return _fixtures(state["gw4_finished"])
        raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(fpl, "_get_json", fake_get_json)
    return state


def test_live_scoreline_is_not_finished(fake_api):
    assert fpl.get_latest_finished_gameweek() == 3
    assert fpl.is_gameweek_finished(3) is True
    assert fpl.is_gameweek_finished(4) is False


def test_finished_flag_advances(fake_api):
    fake_api["gw4_finished"] = True
    assert fpl.get_latest_finished_gameweek() == 4
    assert fpl.is_gameweek_finished(4) is True


def test_provisional_actuals_removed(fake_api, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    for name in ("y_1.csv", "y_3.csv", "y_4.csv", "y_30.csv"):
        (tmp_path / name).write_text("full_name,gw_points,gw_minutes\n")

    assert pipeline.remove_provisional_actuals() is True
    remaining = sorted(p.name for p in tmp_path.glob("y_*.csv"))
    # y_4 was provisional -> removed; finished y_1/y_3 and prior-season
    # y_30 (out of current-season scope) are untouched.
    assert remaining == ["y_1.csv", "y_3.csv", "y_30.csv"]
    assert pipeline.remove_provisional_actuals() is False
