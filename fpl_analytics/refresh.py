"""`poetry run fpl-refresh`: the scheduled data refresh.

Runs the pipeline with retries, writes one log file per run to data/logs,
records the outcome in data/logs/last_run.json for the dashboard, and shows a
macOS notification if every attempt fails. launchd starts it every day; see
`fpl_analytics/schedule.py`.
"""

import json
import subprocess
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from typing import Callable, TextIO

from fpl_analytics.config import (
    LAST_RUN_PATH,
    LOG_DIR,
    REFRESH_ATTEMPTS,
    REFRESH_LOGS_TO_KEEP,
    REFRESH_RETRY_MINUTES,
    STALE_DATA_HOURS,
)
from fpl_analytics.pipeline import PipelineBusy


class _Tee:
    """Write to several text streams at once."""

    def __init__(self, *streams: TextIO):
        self.streams = streams

    def write(self, text: str) -> int:
        for stream in self.streams:
            stream.write(text)
        return len(text)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def read_last_run() -> dict | None:
    try:
        return json.loads(LAST_RUN_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _write_last_run(record: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LAST_RUN_PATH.write_text(json.dumps(record, indent=2))


def is_stale(last_refresh: datetime | None, now: datetime | None = None, hours: float = STALE_DATA_HOURS) -> bool:
    if last_refresh is None:
        return True
    return ((now or _now()) - last_refresh).total_seconds() > hours * 3600


def notify(title: str, message: str) -> None:
    """macOS notification; silently does nothing where osascript is missing."""
    safe = lambda text: text.replace("\\", " ").replace('"', "'")[:200]  # noqa: E731
    script = f'display notification "{safe(message)}" with title "{safe(title)}"'
    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True, timeout=10)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


def _prune_logs(keep: int = REFRESH_LOGS_TO_KEEP) -> None:
    for old in sorted(LOG_DIR.glob("refresh-*.log"), reverse=True)[keep:]:
        old.unlink()


def run_refresh(
    run: Callable[[], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    alert: Callable[[str, str], None] = notify,
    attempts: int = REFRESH_ATTEMPTS,
    retry_minutes: float = REFRESH_RETRY_MINUTES,
) -> dict:
    """Run the pipeline up to `attempts` times. Returns the last_run record.
    Status is "success", "skipped" (another run holds the lock) or "failed"."""
    if run is None:
        from fpl_analytics.pipeline import run as pipeline_run

        run = pipeline_run

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = _now()
    log_path = LOG_DIR / f"refresh-{started:%Y%m%dT%H%M%SZ}.log"
    record = {"status": "running", "started": started.isoformat(), "finished": None, "attempts": 0,
              "error": None, "log": str(log_path)}
    _write_last_run(record)

    with open(log_path, "w", encoding="utf-8") as log:
        streams = (log, sys.__stdout__) if sys.__stdout__ and sys.__stdout__.isatty() else (log,)
        tee = _Tee(*streams)
        with redirect_stdout(tee), redirect_stderr(tee):
            for attempt in range(1, attempts + 1):
                record["attempts"] = attempt
                _write_last_run(record)
                print(f"[{_now():%Y-%m-%d %H:%M:%S} UTC] Refresh attempt {attempt} of {attempts}")
                try:
                    run()
                    record.update(status="success", error=None)
                    break
                except PipelineBusy as exc:
                    print(f"Skipped: {exc}")
                    record.update(status="skipped", error=None)
                    break
                except Exception as exc:  # noqa: BLE001 - log, retry, then report
                    traceback.print_exc()
                    record.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:500])
                    if attempt < attempts:
                        print(f"Trying again in {retry_minutes:g} minutes.")
                        tee.flush()
                        sleep(retry_minutes * 60)

    record["finished"] = _now().isoformat()
    _write_last_run(record)
    _prune_logs()
    if record["status"] == "failed":
        alert("FPL Analytics: data refresh failed",
              f"{record['error']} The dashboard keeps the previous data. Log: {log_path.name}")
    return record


def main() -> None:
    record = run_refresh()
    print(f"Refresh {record['status']} after {record['attempts']} attempt(s). Log: {record['log']}")
    sys.exit(1 if record["status"] == "failed" else 0)


if __name__ == "__main__":
    main()
