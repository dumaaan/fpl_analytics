# FPL Analytics

A local Fantasy Premier League analytics tool. It has two parts: a data pipeline and a dashboard.

## Stack

- **Python** for both parts.
- **DuckDB** for the database. It is one file on disk. It needs no server.
- **Streamlit** for the dashboard.
- **Altair** for the charts.
- **pandas** for all data transforms.

The tool runs on your machine only. It does not need the internet after a data refresh, except to load the FPL API during that refresh.

## Project layout

```
fpl_analytics/          # the pipeline package
  fetch/                 # calls to the FPL API
  transform/             # raw data -> clean tables -> derived metrics
  db.py                  # DuckDB helpers
  pipeline.py            # runs fetch -> transform -> load, in order

dashboard/               # the Streamlit app
  app.py                 # main page: Overview
  pages/                 # Positions, Player Detail, Fixtures
  charts.py              # shared chart-building code
  queries.py             # shared DuckDB read code

data/                    # gitignored: the DuckDB file and raw API snapshots
```

## Setup

1. Create a virtual environment and install packages:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run the pipeline. This fetches FPL data and builds the database file:

   ```bash
   python -m fpl_analytics.pipeline
   ```

   This step calls the FPL API once per player (about 700-800 calls), so it takes a few minutes. Run it again any time you want fresh data — for example, before or after a gameweek.

3. Run the dashboard:

   ```bash
   streamlit run dashboard/app.py
   ```

## Dashboard pages

- **Overview** — filters (position, price, points, minutes) and 8 charts: value, performance vs expectation, form, goalkeepers, differentials, bonus points, overall contribution, and risk. Plus a CSV export button.
- **Positions** — top-10 leaderboards per position, with a percentile bar per player. Plus a "Market Movers" section: biggest price risers/fallers and biggest transfers in/out.
- **Player Detail** — pick one player, then see 6 trend charts across the season: points, minutes, goals vs expected goals, ICT index, price, and ownership.
- **Fixtures** — a difficulty ticker: each team's next 5 fixtures, colored by difficulty.

## Data model

The pipeline writes 4 tables into `data/fpl.duckdb`:

- `stg_teams` — team identity and strength ratings.
- `stg_fixtures` — every match this season, with difficulty ratings.
- `stg_player_gameweek_history` — one row per player per gameweek played. This powers the trend charts.
- `mart_players` — one row per player: identity, season totals, and every derived metric (value added, per-90 stats, consistency score, and so on).

Each pipeline run replaces these tables in full — the FPL API returns full current-state data each time, not just changes, so there is no merge step.
