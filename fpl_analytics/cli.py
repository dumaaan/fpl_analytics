import sys
from pathlib import Path


def run_pipeline() -> None:
    """Entry point for `poetry run fpl-pipeline`."""
    from fpl_analytics.pipeline import run

    run()


def run_dashboard() -> None:
    """Entry point for `poetry run fpl-dashboard`. Launches Streamlit."""
    from streamlit.web import cli as st_cli

    app_path = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"
    sys.argv = ["streamlit", "run", str(app_path)]
    sys.exit(st_cli.main())
