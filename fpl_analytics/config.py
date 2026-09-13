from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "fpl.duckdb"

FPL_BASE_URL = "https://fantasy.premierleague.com/api"
BOOTSTRAP_STATIC_URL = f"{FPL_BASE_URL}/bootstrap-static/"
FIXTURES_URL = f"{FPL_BASE_URL}/fixtures/"
ELEMENT_SUMMARY_URL = f"{FPL_BASE_URL}/element-summary/{{player_id}}/"

POSITION_BY_ELEMENT_TYPE = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}

PLAYER_HISTORY_MAX_WORKERS = 15

# Below this many minutes, "per 90" rates are extrapolated from too small a
# sample to mean anything (a single-minute cameo where a player makes one
# tackle "projects" to 90 defensive contributions per 90). Roughly 3 full
# matches is the common analytics convention for a usable per-90 sample.
MIN_MINUTES_FOR_PER_90 = 270
