import { getTableColumns, sql } from "drizzle-orm";
import { db, closeDb } from "./db";
import { players, dataRefreshLog } from "@/lib/db/schema";
import { buildPlayerRow, computePositionAverages } from "./metrics";
import type { FplBootstrapStatic } from "./fpl-types";

const BOOTSTRAP_STATIC_URL =
  "https://fantasy.premierleague.com/api/bootstrap-static/";

async function fetchWithRetry(
  url: string,
  retries = 2,
  backoffMs = 1000,
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      return res;
    } catch (err) {
      lastError = err;
      if (attempt < retries) {
        await new Promise((r) => setTimeout(r, backoffMs * (attempt + 1)));
      }
    }
  }
  throw lastError;
}

// Upsert-all-columns idiom for drizzle-orm/postgres: set every non-key
// column to its `excluded.<col>` value from the incoming row.
function buildConflictUpdateSet(exclude: string[]) {
  const columns = getTableColumns(players);
  const updateSet: Record<string, ReturnType<typeof sql.raw>> = {};
  for (const [key, column] of Object.entries(columns)) {
    if (exclude.includes(key)) continue;
    updateSet[key] = sql.raw(`excluded.${column.name}`);
  }
  return updateSet;
}

async function main() {
  console.log("Fetching FPL bootstrap-static data...");
  const res = await fetchWithRetry(BOOTSTRAP_STATIC_URL);
  const data = (await res.json()) as FplBootstrapStatic;

  const teamNameById = new Map(data.teams.map((t) => [t.id, t.name]));
  const positionAverages = computePositionAverages(data.elements);

  const rows = data.elements.map((el) =>
    buildPlayerRow(el, teamNameById.get(el.team) ?? "Unknown", positionAverages),
  );

  console.log(`Upserting ${rows.length} players...`);
  await db
    .insert(players)
    .values(rows)
    .onConflictDoUpdate({
      target: players.id,
      set: buildConflictUpdateSet(["id"]),
    });

  await db.insert(dataRefreshLog).values({
    playerCount: rows.length,
    status: "success",
  });

  console.log(`Done. Refreshed ${rows.length} players.`);
}

main()
  .catch(async (err) => {
    console.error("Ingest failed:", err);
    try {
      await db.insert(dataRefreshLog).values({
        playerCount: 0,
        status: "error",
      });
    } catch {
      // best effort — if we can't even log the failure, just exit non-zero below
    }
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDb();
  });
