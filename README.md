# FPL Analytics

A local Fantasy Premier League analytics tool. It has two parts: a data pipeline and a dashboard.

## Stack

- **Python** for both parts.
- **Poetry** for packages and the virtual environment. It sets both up in one command.
- **DuckDB** for the database. It is one file on disk. It needs no server.
- **Streamlit** for the dashboard.
- **Altair** for the charts.
- **pandas** for all data transforms.
- **pytest** for the metric tests.

The tool runs on your machine only. It needs the internet only during a data refresh, to load the FPL API.

## Project layout

```
fpl_analytics/          # the pipeline package
  fetch/                 # calls to the FPL API, raw snapshots
  transform/
    staging.py           # raw JSON -> clean tables
    scoring.py           # FPL scoring rules, points split by source
    teams.py             # team xG, team ratings, fixture model
    marts.py             # every player metric
    metrics.py           # per-90, safe divide, shrunk rates
  db.py                  # DuckDB helpers
  pipeline.py            # runs fetch -> transform -> load, in order

dashboard/               # the Streamlit app
  app.py                 # entry point: navigation and sidebar filters
  views/                 # Gameweek, position pages, Player detail, Fixture planner
  catalog.py             # label, help text and format for every metric
  charts.py              # shared chart-building code
  common.py              # filters, percentiles, table helpers
  queries.py             # shared DuckDB read code

tests/                   # unit tests for the metric formulas

data/                    # gitignored: the DuckDB file and raw API snapshots
```

## Setup

Install [Poetry](https://python-poetry.org/docs/#installation) once, if you do not have it. Then, from the project folder:

1. Install packages. Poetry creates the virtual environment for you, in `.venv`:

   ```bash
   poetry install
   ```

2. Run the pipeline. This fetches FPL data and builds the database file:

   ```bash
   poetry run fpl-pipeline
   ```

   This step calls the FPL API once per player (about 700-800 calls), so it takes a few minutes. Run it again any time you want fresh data, for example before a deadline. You can keep the dashboard open: the pipeline builds a new file and swaps it in at the end, and the dashboard loads the new data on its next page load. The pipeline keeps the 3 newest raw snapshots of each API response.

3. Run the dashboard:

   ```bash
   poetry run fpl-dashboard
   ```

4. Run the tests:

   ```bash
   poetry run pytest
   ```

## Dashboard pages

- **Gameweek** — the next deadline, last gameweek's average and top score, the best picks for each position by projected points, tonight's likely price changes, and the most transferred players.
- **Goalkeepers, Defenders, Midfielders, Forwards** — one page per position, with the same layout:
  - 4 headline cards: best value, top projected points, easiest 5-gameweek run, and closest to a price rise.
  - One sortable table with columns for that position (for example DefCon rate and near misses for defenders, xG share and haul rate for midfielders). Every column header has a help tooltip. Select a row to open the player page.
  - A position chart with median lines and labels, and a chart of where each player's points come from.
- **Player detail** — a percentile profile against the position, the next 5 fixtures, points per gameweek split by source, minutes, cumulative goals vs xG and assists vs xA, price and ownership. Pick a second player to compare the two.
- **Fixture planner** — each team's next 3, 5 or 8 gameweeks, rated by clean-sheet chance (for GK/DEF), goal threat (for MID/FWD) or FPL difficulty. Double and blank gameweeks show in the right column.

The sidebar filters (price range, minimum minutes, teams, available only) apply on every page.

## Metrics

- **Scoring** comes from the API (`game_config.scoring`), so the metrics follow rule changes. The pipeline rebuilds every player's points from these rules and prints a warning if any match does not add up.
- **Per-90 rates** are empty below 270 minutes. Small samples show as missing, not as 0.
- **VAPM** (Value Added per Million) is points above the appearance points, per game played, divided by price.
- **DefCon rate** is shrunk toward the position average for small samples: `(hits + 4 × position rate) / (games + 4)`.
- **Fixture model** — each team's attack and defence is its xG for and against per game, shrunk toward a prior from FPL fixture difficulty. Projected goals for a fixture = league average × team attack × opponent defence, with a 10% home advantage. Clean-sheet chance = `exp(−projected goals against)`.
- **xPts** (expected points) combine a player's per-90 xG, xA, xG conceded, saves, DefCon rate, bonus and cards with the scoring for the position. **xPts next GW** also uses the fixture model, start rate and chance of playing. The window starts at the gameweek of the next deadline.

## Data model

The pipeline writes these tables into `data/fpl.duckdb`:

- `stg_teams`, `stg_fixtures`, `stg_events` — teams, every match this season, and every gameweek (deadline, average score, most captained, chip plays).
- `mart_player_gameweeks` — one row per player per team match, with the team played for, points split by source, and DefCon flags.
- `mart_team_fixture_results` — team xG and goals for each finished match.
- `mart_team_ratings` — each team's attack and defence rating.
- `mart_team_fixtures` — each team's upcoming matches with projected goals, clean-sheet chance and 1-5 ease ratings.
- `mart_players` — one row per player: identity, season totals, and every derived metric.
- `data_refresh_log` — one row per pipeline run.

Each pipeline run replaces these tables in full. The FPL API returns full current-state data each time, not just changes, so there is no merge step.
