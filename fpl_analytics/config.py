from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "fpl.duckdb"
# The pipeline builds into this file, then renames it over DB_PATH, so an
# open dashboard never blocks a refresh and never sees a half-written database.
TMP_DB_PATH = DATA_DIR / "fpl.duckdb.tmp"

FPL_BASE_URL = "https://fantasy.premierleague.com/api"
BOOTSTRAP_STATIC_URL = f"{FPL_BASE_URL}/bootstrap-static/"
FIXTURES_URL = f"{FPL_BASE_URL}/fixtures/"
ELEMENT_SUMMARY_URL = f"{FPL_BASE_URL}/element-summary/{{player_id}}/"

POSITION_BY_ELEMENT_TYPE = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
OUTFIELD_POSITIONS = ["DEF", "MID", "FWD"]

# game_config.scoring in bootstrap-static keys per-position values by these
# names ("GKP", not "GK").
SCORING_POSITION_KEYS = {"GK": "GKP", "DEF": "DEF", "MID": "MID", "FWD": "FWD"}

# Scoring steps that game_config.scoring does not state: its "saves" value is
# awarded per 3 saves, and its "goals_conceded" value per 2 goals conceded.
SAVES_PER_SAVE_POINT = 3
GOALS_CONCEDED_PER_DEDUCTION = 2

# Defensive Contribution (DefCon), unchanged for 2026/27. Defenders need 10+
# clearances, blocks, interceptions and tackles (CBIT) in a match; midfielders
# and forwards need 12+ including ball recoveries (CBIRT). The API's
# per-match "defensive_contribution" field already holds the right total for
# each position. Goalkeepers are not eligible.
DEFCON_THRESHOLDS = {"DEF": 10, "MID": 12, "FWD": 12}
# A "near miss" is a match that ends within this many actions of the threshold.
DEFCON_NEAR_MISS_WINDOW = 2

PLAYER_HISTORY_MAX_WORKERS = 15

# Below this many minutes, "per 90" rates are extrapolated from too small a
# sample to mean anything (a single-minute cameo where a player makes one
# tackle "projects" to 90 defensive contributions per 90). Roughly 3 full
# matches is the common analytics convention for a usable per-90 sample.
MIN_MINUTES_FOR_PER_90 = 270

# Rates from a few games are noisy (one good game moves a 3-game hit rate from
# 33% to 67%). Shrunk rates add this many "average" games for the position:
# (hits + k * position_rate) / (games + k). Strong in GW4, negligible by GW20.
SHRINK_PRIOR_GAMES = 4

HAUL_POINTS = 10
BLANK_POINTS = 2
RECENT_FORM_GWS = 4
RECENT_FORM_MIN_MINUTES = 180

# Fixture model: typical Premier League goals per team per match, used only
# as the scale when no match has finished yet. Home sides score about 10%
# more than their average and away sides about 10% less.
LEAGUE_AVG_GOALS_FALLBACK = 1.4
HOME_ADVANTAGE = 1.1
FIXTURE_HORIZON_GWS = 8

RAW_SNAPSHOTS_TO_KEEP = 3
