# FPL Analytics

A Fantasy Premier League player-stats dashboard: filter by position/price/points/minutes and explore value, form, differential, and risk metrics across 8 charts, backed by real FPL API data refreshed on a schedule.

## Stack

- **Next.js (App Router) + TypeScript**, deployed on Vercel
- **Postgres on Supabase**, queried with **Drizzle ORM**
- **Recharts** for the scatter/bubble charts
- A **GitHub Actions** cron job refreshes player data from the live FPL API every 3 hours

## How it works

- `scripts/ingest/fetch-and-upsert.ts` fetches `https://fantasy.premierleague.com/api/bootstrap-static/`, computes derived metrics (value added, per-90 stats, etc. — ported from the project's original dbt model) in `scripts/ingest/metrics.ts`, and upserts one row per player into a single `players` table plus a `data_refresh_log` row.
- The app (`app/page.tsx`) reads that table server-side and renders the dashboard — no live API calls on page load, so it's fast and doesn't depend on FPL's API uptime per-request.
- `.github/workflows/refresh-data.yml` runs the ingest script on a schedule (and via manual `workflow_dispatch`).

**Known v1 limitation**: this only stores the current season snapshot, not gameweek-by-gameweek history. The old dashboard's "consistency score" metric required per-gameweek data and was dropped as a result (see the note in the Form Analysis section of the dashboard) — the "Form" chart shows form vs. points-per-game instead.

## Local development

1. Copy `.env.example` to `.env.local` and fill in your Supabase connection strings (Project Settings → Database → Connection string):
   - `DATABASE_URL` — Transaction pooler (port 6543), used by the app
   - `DIRECT_DATABASE_URL` — Session pooler (port 5432), used for migrations
   - `SUPABASE_DB_URL` — Session pooler, used by the ingest script (this one also needs to be set as a GitHub Actions repo secret for the scheduled refresh)

2. Install dependencies and set up the database:

   ```bash
   npm install
   npm run db:generate   # generate SQL migration from lib/db/schema.ts
   npm run db:migrate    # apply it to Supabase
   npm run ingest        # fetch live FPL data and seed the players table
   ```

3. Run the app:

   ```bash
   npm run dev
   ```

## Deploying

1. Push to GitHub, import the repo in Vercel, and set `DATABASE_URL` as a Vercel environment variable.
2. Add `SUPABASE_DB_URL` as a GitHub Actions repository secret (Settings → Secrets and variables → Actions) so the scheduled refresh workflow can write to the database.
3. Trigger the workflow once manually (Actions tab → Refresh FPL data → Run workflow) to confirm the scheduled refresh works end to end.
