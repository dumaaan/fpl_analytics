from pathlib import Path

import duckdb
import pandas as pd

from fpl_analytics.config import DATA_DIR, DB_PATH


def get_connection(path: Path = DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def load_dataframe(conn: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    """Replace a table's contents with a DataFrame's contents.

    Every pipeline run pulls a full snapshot from the FPL API (bootstrap-static
    and element-summary both return complete current-state data, not deltas),
    so a full replace is correct and needs no upsert/merge logic.
    """
    conn.register("_incoming_df", df)
    conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _incoming_df')
    conn.unregister("_incoming_df")


def copy_refresh_log(conn: duckdb.DuckDBPyConnection, old_db_path: Path) -> None:
    """Carry data_refresh_log over from the previous database file, if any."""
    if not old_db_path.exists():
        return
    try:
        conn.execute(f"ATTACH '{old_db_path}' AS previous (READ_ONLY)")
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = 'previous' AND table_name = 'data_refresh_log'"
        ).fetchone()[0]
        if exists:
            conn.execute("CREATE TABLE data_refresh_log AS SELECT * FROM previous.data_refresh_log")
        conn.execute("DETACH previous")
    except duckdb.Error as exc:
        print(f"  Warning: could not copy the refresh log from the previous database: {exc}")
