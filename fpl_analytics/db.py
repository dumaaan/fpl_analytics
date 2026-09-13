import duckdb
import pandas as pd

from fpl_analytics.config import DATA_DIR, DB_PATH


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def load_dataframe(conn: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    """Replace a table's contents with a DataFrame's contents.

    Every pipeline run pulls a full snapshot from the FPL API (bootstrap-static
    and element-summary both return complete current-state data, not deltas),
    so a full replace is correct and needs no upsert/merge logic.
    """
    conn.register("_incoming_df", df)
    conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _incoming_df')
    conn.unregister("_incoming_df")
