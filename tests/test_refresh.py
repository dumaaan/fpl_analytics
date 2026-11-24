import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import fpl_analytics.pipeline as pipeline
import fpl_analytics.refresh as refresh
from fpl_analytics.schedule import build_plist


@pytest.fixture
def tmp_data(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PIPELINE_LOCK_PATH", tmp_path / ".pipeline.lock")
    monkeypatch.setattr(pipeline, "DATA_DIR", tmp_path)
    monkeypatch.setattr(refresh, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(refresh, "LAST_RUN_PATH", tmp_path / "logs" / "last_run.json")
    return tmp_path


def test_pipeline_lock_blocks_a_second_run(tmp_data):
    with pipeline.pipeline_lock():
        with pytest.raises(pipeline.PipelineBusy):
            with pipeline.pipeline_lock():
                pass
    with pipeline.pipeline_lock():  # released after the first run ends
        pass


def test_refresh_retries_then_succeeds_without_alert(tmp_data):
    calls, sleeps, alerts = [], [], []

    def flaky_run():
        calls.append(1)
        if len(calls) < 2:
            raise ConnectionError("FPL is down")
        print("pipeline done")

    record = refresh.run_refresh(run=flaky_run, sleep=sleeps.append, alert=lambda *a: alerts.append(a),
                                 attempts=3, retry_minutes=15)
    assert record["status"] == "success" and record["attempts"] == 2
    assert sleeps == [15 * 60] and alerts == []
    saved = json.loads((tmp_data / "logs" / "last_run.json").read_text())
    assert saved["status"] == "success"
    log = Path(record["log"]).read_text()
    assert "ConnectionError: FPL is down" in log and "pipeline done" in log


def test_refresh_alerts_after_the_last_failed_attempt(tmp_data):
    alerts = []

    def broken_run():
        raise ValueError("bad response")

    record = refresh.run_refresh(run=broken_run, sleep=lambda s: None, alert=lambda t, m: alerts.append(m),
                                 attempts=2)
    assert record["status"] == "failed" and record["attempts"] == 2
    assert "ValueError: bad response" in record["error"]
    assert len(alerts) == 1 and "previous data" in alerts[0]


def test_refresh_skips_when_another_run_holds_the_lock(tmp_data):
    def busy_run():
        raise pipeline.PipelineBusy("busy")

    record = refresh.run_refresh(run=busy_run, sleep=lambda s: None, alert=lambda *a: None)
    assert record["status"] == "skipped" and record["attempts"] == 1


def test_is_stale():
    now = datetime(2026, 9, 15, 12, tzinfo=timezone.utc)
    assert not refresh.is_stale(now - timedelta(hours=30), now, hours=36)
    assert refresh.is_stale(now - timedelta(hours=40), now, hours=36)
    assert refresh.is_stale(None, now)


def test_plist_runs_the_venv_script_daily_with_absolute_paths():
    plist = build_plist(Path("/venv/bin/fpl-refresh"), 18, 30)
    assert plist["ProgramArguments"] == ["/venv/bin/fpl-refresh"]
    assert plist["StartCalendarInterval"] == {"Hour": 18, "Minute": 30}
    assert Path(plist["WorkingDirectory"]).is_absolute()
    assert plist["RunAtLoad"] is False
