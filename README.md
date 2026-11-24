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

4. Optional: backtest the model and the advisor on a past season (see [Backtest](#backtest)):

   ```bash
   poetry run fpl-backtest --season 2025-26
   ```

5. Optional: refresh the data every day on its own (macOS, see [Daily refresh](#daily-refresh)):

   ```bash
   poetry run fpl-schedule install
   ```

6. Run the tests:

   ```bash
   poetry run pytest
   ```

## Dashboard pages

- **Gameweek** — the next deadline, last gameweek's average and top score, the best picks for each position by projected points, tonight's likely price changes, and the most transferred players.
- **My team** — enter your FPL team ID (the number after `/entry/` in your Points page address). The page reads your squad, bank, transfers and chips from FPL's public API; it never asks for a password. It then shows:
  - a recommended move: save the transfer, or make 1 or 2 transfers, with the projected gain, any hit, and the bank after;
  - a Keep / Watch / Sell rating for each squad player, with the reasons;
  - the best affordable replacements for any player you pick;
  - the best lineup, captain and bench order for the next gameweek.
  - **Transfers you already made:** FPL keeps transfers private until the next deadline. Add each one (player sold → player bought) to check that it is legal (budget, position, 3 per club), see the change in projected points over the next 1, 3 and 5 gameweeks after any hit, and compare it with the best move the advisor finds from your old squad. The rest of the page then plans from the new squad, bank and remaining free transfers. The transfers are kept in the page address (`&made=<out>-<in>`).

  Plan for the next 1, 3 or 5 gameweeks. Selling prices follow FPL's rule (you keep half of a rise). Free transfers are estimated from your history; you can correct the number. The squad is the one at the last deadline: FPL keeps later transfers private until the next deadline.
- **Goalkeepers, Defenders, Midfielders, Forwards** — one page per position, with the same layout:
  - 4 headline cards: best value, top projected points, easiest 5-gameweek run, and closest to a price rise.
  - One sortable table with columns for that position (for example DefCon rate and near misses for defenders, xG share and haul rate for midfielders). Every column header has a help tooltip. Select a row to open the player page.
  - A position chart with median lines and labels, and a chart of where each player's points come from.
- **Player detail** — a percentile profile against the position, the next 5 fixtures, points per gameweek split by source, minutes, cumulative goals vs xG and assists vs xA, price and ownership. Pick a second player to compare the two.
- **Fixture planner** — each team's next 3, 5 or 8 gameweeks, rated by clean-sheet chance (for GK/DEF), goal threat (for MID/FWD) or FPL difficulty. Double and blank gameweeks show in the right column.

The sidebar filters (price range, minimum minutes, teams, available only) apply on every page.

## Metrics

- **Scoring** comes from the API (`game_config.scoring`), so the metrics follow rule changes. The pipeline rebuilds every player's points from these rules and prints a warning if any match does not add up.
- **Per-90 rates** are empty below 180 minutes (2 full matches). Small samples show as missing, not as 0. The floor was 270; it is lower early in the season because the model pulls small samples toward the position average, and the backtest showed good accuracy for players with 180–269 minutes.
- **VAPM** (Value Added per Million) is points above the appearance points, per game played, divided by price.
- **DefCon rate** is shrunk toward the position average for small samples: `(hits + 4 × position rate) / (games + 4)`.
- **Fixture model** — each team's attack and defence is its xG for and against per game, shrunk toward a prior from FPL fixture difficulty. Projected goals for a fixture = league average × team attack × opponent defence, with a 10% home advantage. Clean-sheet chance = `exp(−projected goals against)`.
- **xPts** (expected points) combine a player's per-90 xG, xA, xG conceded, saves, DefCon rate, bonus and cards with the scoring for the position. The model is in `fpl_analytics/transform/projection.py`:
  - **Minutes:** the chance of a 60+ minute match, the chance of a substitute appearance, and expected minutes, from the player's team matches (each older match counts 90% as much as the next one). Appearance and clean-sheet points use these chances; goals, assists, saves, bonus and cards scale with expected minutes.
  - **Rates:** season per-90 rates, pulled toward the position average as if the player had 900 extra minutes at that average. A player with 300 minutes moves a lot; a player with 3,000 minutes moves little.
  - **Fixtures:** **xPts next GW** uses the fixture model and FPL's chance of playing. The window starts at the gameweek of the next deadline.
  - The settings were chosen with the [backtest](#backtest): tuned on the first half of 2025/26, checked on the second half, and confirmed in the decision replay.

### How the team advisor decides

- Each squad is scored by its **best legal lineup**: the 11 starters' projected points, plus the captain again, plus 10% of the bench. So a swap between two bench players gains almost nothing.
- For every squad player, the advisor tries the strongest affordable players in the same position (budget = bank + selling price, 3-per-club rule, 75%+ chance to play). Pairs of transfers are tried among the best single moves.
- Each transfer beyond your free transfers costs 4 points. A move is recommended only if it gains at least 0.5 projected points per gameweek of the plan after hits. A second transfer must add that much again on top of the best single move.
- **Sell** means unlikely to play, or a replacement clearly scores more. **Watch** means a doubt, few starts, no projection yet (under 180 minutes), a price fall tonight, or a smaller upgrade.

The logic is in `fpl_analytics/advisor.py`, with tests in `tests/test_advisor.py`.

## Daily refresh

`poetry run fpl-schedule install` adds a launchd job that runs `fpl-refresh` every day at 18:30 local time (09:30 UTC on a Mac set to Korea time). That time is after FPL's overnight price changes and after gameweek scores are final. Choose another time with `--time HH:MM`.

- **Sleep:** if the Mac is asleep at that time, the job runs when it wakes. A Mac that is switched off skips the day.
- **Retries:** each run tries up to 3 times, 15 minutes apart, because the network or FPL may not be ready.
- **No overlap:** a lock stops a scheduled and a manual refresh from running at the same time. The second one exits.
- **Logs:** one log file per run in `data/logs/` (the newest 30 are kept). `data/logs/last_run.json` holds the last result.
- **Failures:** macOS shows a notification. The dashboard keeps the previous data and shows an error in the sidebar. If the newest data is older than 36 hours, the sidebar shows a warning.

Other commands:

```bash
poetry run fpl-schedule status
```

```bash
poetry run fpl-schedule run-now
```

```bash
poetry run fpl-schedule uninstall
```

`status` shows the schedule, the last run and its log. `run-now` starts the job the same way launchd does, to test it. `uninstall` removes the job.

## Backtest

The backtest measures how good the projections and the transfer advice really are, on a season that already happened. The model code lives in `fpl_analytics/transform/projection.py`, and both the dashboard and the backtest call it, so a result always describes the model you see.

**Data.** A past season needs per-gameweek files in `data/external/<season>/`: `merged_gw.csv`, `fixtures.csv` and `teams.csv`, in the layout of [vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League) (`data/<season>/gws/merged_gw.csv` and `data/<season>/*.csv`). That repository states no license, so keep the files for personal use; `data/external/` is gitignored. `--season current` uses this season's database instead, once enough gameweeks have finished.

**What it does.**

1. **Projection accuracy.** For each gameweek, it rebuilds the projections with only the matches before that deadline, then compares them with the real points. It compares the model with two simple baselines (season points per game, and form over the last 4 gameweeks). It reports the error, bias, ranking quality, top-10 picks and captain points, early season vs later, and by position. It also checks calibration and how well the minutes estimate predicts who plays.
2. **Decision replay.** It plays the season week by week from 5 realistic starting squads under FPL's rules (budget, selling prices, 3 per club, free transfers, hits, automatic substitutions). It compares holding the squad with the advisor at several gain thresholds.

**Output.** `data/backtest/<season>/report.md` (plain-language findings and tables) and CSV files with every prediction and result.

**Limits.** Past injury news is not in the data, so every player counts as available. The dataset's `xP` column is not used: it is recorded after team news, so it leaks the result. Chips are not played in the replay.

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
