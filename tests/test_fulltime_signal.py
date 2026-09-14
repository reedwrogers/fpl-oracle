"""90-minute live signal + bonus delay gates early actuals (GW4 evening lesson).

The official ``finished`` flags lagged the Leeds–Newcastle full time by hours
while the live endpoint already showed 90-minute players. A 90' appearance
from the last kickoff slot is the cheap completion signal (one live call, not
~60 element-summary calls) — but actuals are recorded only 4h after first
sighting so bonus points have landed.
"""

from datetime import datetime, timedelta, timezone

import config
import fpl
import pipeline


def _bootstrap(last_kickoff):
    del last_kickoff
    return {
        "events": [
            {"id": 3, "is_current": False, "is_next": False, "finished": True},
            {"id": 4, "is_current": True, "is_next": False, "finished": False},
            {"id": 5, "is_current": False, "is_next": True, "finished": False},
        ],
        # ids 1-2: last-slot teams (Leeds/Newcastle stand-ins); id 5: earlier slot.
        "elements": [
            {"id": 1, "team": 13},
            {"id": 2, "team": 13},
            {"id": 3, "team": 17},
            {"id": 4, "team": 17},
            {"id": 5, "team": 1},
        ],
        "teams": [],
        "element_types": [],
    }


def _fixtures(last_kickoff):
    base = {
        "started": True,
        "finished": False,
        "finished_provisional": True,
        "team_h_score": 4,
        "team_a_score": 1,
    }
    return [
        {**base, "event": 4, "kickoff_time": "2026-09-12T14:00:00Z",
         "team_h": 1, "team_a": 2},
        {**base, "event": 4, "kickoff_time": last_kickoff,
         "team_h": 13, "team_a": 17},
    ]


def _live(minutes):
    return {"elements": [
        {"id": pid, "stats": {"minutes": m}} for pid, m in minutes.items()
    ]}


def _fake_api(monkeypatch, minutes, last_kickoff="2026-09-14T19:00:00Z"):
    def fake_get_json(url: str):
        if url.endswith("/bootstrap-static/"):
            return _bootstrap(last_kickoff)
        if url.endswith("/fixtures/"):
            return _fixtures(last_kickoff)
        if url.endswith("/event/4/live/"):
            return _live(minutes)
        raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(fpl, "_get_json", fake_get_json)


def test_no_signal_while_last_game_live(monkeypatch):
    _fake_api(monkeypatch, {1: 65, 2: 65, 3: 65, 4: 65, 5: 90})
    assert fpl.last_slot_fulltime_observed(4) is False


def test_earlier_slot_90_alone_is_not_signal(monkeypatch):
    # Finished earlier game has 90s, last game still playing: not over.
    _fake_api(monkeypatch, {1: 10, 2: 0, 3: 72, 4: 82, 5: 90})
    assert fpl.last_slot_fulltime_observed(4) is False


def test_signal_when_last_slot_has_90(monkeypatch):
    _fake_api(monkeypatch, {1: 90, 2: 15, 3: 90, 4: 90, 5: 90})
    assert fpl.last_slot_fulltime_observed(4) is True


def test_bonus_delay_gates_readiness(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    now = datetime.now(timezone.utc)

    assert pipeline.fulltime_ready(4, now) is False  # never observed

    (tmp_path / ".gw4_fulltime").write_text(
        (now - timedelta(hours=1)).isoformat())
    assert pipeline.fulltime_ready(4, now) is False  # bonus may be missing

    (tmp_path / ".gw4_fulltime").write_text(
        (now - timedelta(hours=5)).isoformat())
    assert pipeline.fulltime_ready(4, now) is True


def test_deliberate_actuals_survive_provisional_sweep(tmp_path, monkeypatch):
    """y_4 recorded after the bonus delay is kept; a fresh sighting is not."""
    _fake_api(monkeypatch, {1: 90})
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    now = datetime.now(timezone.utc)

    (tmp_path / "y_4.csv").write_text("full_name,gw_points,gw_minutes\n")
    (tmp_path / ".gw4_fulltime").write_text(
        (now - timedelta(hours=5)).isoformat())
    assert pipeline.remove_provisional_actuals() is False
    assert (tmp_path / "y_4.csv").exists()

    (tmp_path / ".gw4_fulltime").write_text(
        (now - timedelta(hours=1)).isoformat())
    assert pipeline.remove_provisional_actuals() is True
    assert not (tmp_path / "y_4.csv").exists()
