"""`poetry run fpl-schedule`: manage the daily refresh with launchd (macOS).

  install [--time HH:MM]   run `fpl-refresh` every day at this local time
  status                   show the schedule, the last run and its log
  run-now                  start the scheduled job now, the same way launchd does
  uninstall                remove the schedule

launchd, not cron: if the Mac is asleep at the scheduled time, launchd runs
the job when it wakes; cron would skip that day.
"""

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path

from fpl_analytics.config import LOG_DIR, PROJECT_ROOT, REFRESH_HOUR, REFRESH_MINUTE, SCHEDULE_LABEL
from fpl_analytics.refresh import read_last_run

PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / f"{SCHEDULE_LABEL}.plist"


def _domain() -> str:
    return f"gui/{os.getuid()}"


def refresh_program() -> Path:
    """The `fpl-refresh` script in this virtual environment, by absolute path:
    launchd does not load the shell profile, so PATH and Poetry are not there."""
    return Path(sys.executable).parent / "fpl-refresh"


def build_plist(program: Path, hour: int, minute: int) -> dict:
    return {
        "Label": SCHEDULE_LABEL,
        "ProgramArguments": [str(program)],
        "WorkingDirectory": str(PROJECT_ROOT),
        "StartCalendarInterval": {"Hour": hour, "Minute": minute},
        "RunAtLoad": False,
        # fpl-refresh writes its own log per run; this file only catches start-up errors.
        "StandardOutPath": str(LOG_DIR / "launchd.log"),
        "StandardErrorPath": str(LOG_DIR / "launchd.log"),
        "EnvironmentVariables": {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "PYTHONIOENCODING": "utf-8"},
    }


def _launchctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["launchctl", *args], capture_output=True, text=True)


def install(hour: int, minute: int) -> None:
    program = refresh_program()
    if not program.exists():
        sys.exit(f"{program} does not exist. Run `poetry install` first.")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    _launchctl("bootout", _domain(), str(PLIST_PATH))  # replace an older version; fails harmlessly if none
    with open(PLIST_PATH, "wb") as handle:
        plistlib.dump(build_plist(program, hour, minute), handle)
    result = _launchctl("bootstrap", _domain(), str(PLIST_PATH))
    if result.returncode != 0:
        sys.exit(f"launchctl could not load the schedule: {result.stderr.strip() or result.stdout.strip()}")
    print(f"Installed: the data refresh runs every day at {hour:02d}:{minute:02d} (this Mac's local time).")
    print(f"Schedule file: {PLIST_PATH}")
    print("Test it now with: poetry run fpl-schedule run-now")


def uninstall() -> None:
    _launchctl("bootout", _domain(), str(PLIST_PATH))
    if PLIST_PATH.exists():
        PLIST_PATH.unlink()
        print("Removed the daily data refresh.")
    else:
        print("No daily data refresh is installed.")


def run_now() -> None:
    result = _launchctl("kickstart", f"{_domain()}/{SCHEDULE_LABEL}")
    if result.returncode != 0:
        sys.exit("The schedule is not loaded. Run `poetry run fpl-schedule install` first.")
    print("Started. The refresh takes a few minutes. Check it with: poetry run fpl-schedule status")


def status() -> None:
    if not PLIST_PATH.exists():
        print("Daily refresh: not installed. Install it with: poetry run fpl-schedule install")
    else:
        with open(PLIST_PATH, "rb") as handle:
            interval = plistlib.load(handle)["StartCalendarInterval"]
        loaded = _launchctl("print", f"{_domain()}/{SCHEDULE_LABEL}")
        state = "loaded" if loaded.returncode == 0 else "NOT loaded (run install again)"
        running = "running now" if "state = running" in loaded.stdout else "waiting"
        print(f"Daily refresh: every day at {interval['Hour']:02d}:{interval['Minute']:02d} local time, "
              f"{state}, {running}.")
    last = read_last_run()
    if last is None:
        print("Last run: none yet.")
        return
    print(f"Last run: {last['status']} (started {last['started']}, finished {last['finished']}, "
          f"attempts {last['attempts']}).")
    if last.get("error"):
        print(f"Error: {last['error']}")
    print(f"Log: {last['log']}")


def main() -> None:
    if sys.platform != "darwin":
        sys.exit("fpl-schedule uses launchd and works on macOS only. On Linux, add a cron job for fpl-refresh.")
    parser = argparse.ArgumentParser(description="Manage the daily data refresh (launchd).")
    commands = parser.add_subparsers(dest="command", required=True)
    install_cmd = commands.add_parser("install", help="Run the refresh every day.")
    install_cmd.add_argument("--time", default=f"{REFRESH_HOUR:02d}:{REFRESH_MINUTE:02d}",
                             help="Local time as HH:MM (default %(default)s).")
    commands.add_parser("status", help="Show the schedule and the last run.")
    commands.add_parser("run-now", help="Start the scheduled job now.")
    commands.add_parser("uninstall", help="Remove the schedule.")
    args = parser.parse_args()

    if args.command == "install":
        try:
            hour, minute = (int(part) for part in args.time.split(":"))
            assert 0 <= hour < 24 and 0 <= minute < 60
        except (ValueError, AssertionError):
            sys.exit("Use --time HH:MM, for example --time 18:30.")
        install(hour, minute)
    elif args.command == "status":
        status()
    elif args.command == "run-now":
        run_now()
    else:
        uninstall()


if __name__ == "__main__":
    main()
