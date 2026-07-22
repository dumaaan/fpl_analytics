import { desc } from "drizzle-orm";
import { db } from "./client";
import { players, dataRefreshLog, type Player } from "./schema";

export async function getAllPlayers(): Promise<Player[]> {
  return db.select().from(players).orderBy(desc(players.totalPoints));
}

export async function getLastRefresh(): Promise<Date | null> {
  const [row] = await db
    .select({ refreshedAt: dataRefreshLog.refreshedAt })
    .from(dataRefreshLog)
    .orderBy(desc(dataRefreshLog.refreshedAt))
    .limit(1);
  return row?.refreshedAt ?? null;
}
