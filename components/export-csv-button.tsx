"use client";

import type { Player } from "@/lib/types";
import { toCsv, downloadCsv } from "@/lib/csv";

const EXPORT_COLUMNS: (keyof Player)[] = [
  "webName",
  "teamName",
  "position",
  "price",
  "totalPoints",
  "pointsPerGame",
  "minutes",
  "goalsScored",
  "assists",
  "cleanSheets",
  "form",
  "selectedByPercent",
  "ictIndex",
  "expectedGoals",
  "expectedAssists",
  "pointsPerMillion",
  "valueAdded",
];

export function ExportCsvButton({ players }: { players: Player[] }) {
  return (
    <button
      onClick={() => {
        const csv = toCsv(players, EXPORT_COLUMNS);
        downloadCsv(`fpl-players-${new Date().toISOString().slice(0, 10)}.csv`, csv);
      }}
      className="rounded border border-neutral-300 px-3 py-1.5 text-sm text-neutral-700 hover:bg-neutral-100 dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-800"
    >
      Export CSV ({players.length} players)
    </button>
  );
}
