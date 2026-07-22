"use client";

import { useMemo, useState } from "react";
import type { Player, Position } from "@/lib/types";
import { FilterSidebar } from "@/components/filter-sidebar";
import { ScatterChart } from "@/components/charts/scatter-chart";
import { TopValueTable } from "@/components/tables/top-value-table";
import { ExportCsvButton } from "@/components/export-csv-button";
import { LastUpdated } from "@/components/last-updated";

const POSITION_ORDER = ["GK", "DEF", "MID", "FWD"];

function fmt1(v: unknown) {
  return typeof v === "number" ? v.toFixed(1) : String(v ?? "");
}
function fmt2(v: unknown) {
  return typeof v === "number" ? v.toFixed(2) : String(v ?? "");
}

export function DashboardClient({
  players,
  lastUpdated,
}: {
  players: Player[];
  lastUpdated: Date | null;
}) {
  const bounds = useMemo(() => {
    const prices = players.map((p) => p.price);
    const points = players.map((p) => p.totalPoints);
    const games = players.map((p) => p.minutes / 90);
    return {
      minPrice: Math.min(...prices, 0),
      maxPrice: Math.max(...prices, 15),
      minPoints: Math.min(...points, 0),
      maxPoints: Math.max(...points, 1),
      minGames: Math.min(...games, 0),
      maxGames: Math.max(...games, 1),
    };
  }, [players]);

  const [position, setPosition] = useState<Position>("All");
  const [minPrice, setMinPrice] = useState(bounds.minPrice);
  const [minPoints, setMinPoints] = useState(bounds.minPoints);
  const [minGames, setMinGames] = useState(bounds.minGames);

  const filtered = useMemo(() => {
    return players.filter(
      (p) =>
        (position === "All" || p.position === position) &&
        p.price >= minPrice &&
        p.totalPoints >= minPoints &&
        p.minutes >= minGames * 90,
    );
  }, [players, position, minPrice, minPoints, minGames]);

  const gkData = useMemo(
    () => filtered.filter((p) => p.position === "GK"),
    [filtered],
  );

  const riskData = useMemo(
    () => filtered.filter((p) => p.minutesPerGoalInvolvement !== null),
    [filtered],
  );

  return (
    <div className="mx-auto flex max-w-7xl flex-col gap-8 px-6 py-8 lg:flex-row">
      <FilterSidebar
        bounds={bounds}
        position={position}
        onPositionChange={setPosition}
        minPrice={minPrice}
        onMinPriceChange={setMinPrice}
        minPoints={minPoints}
        onMinPointsChange={setMinPoints}
        minGames={minGames}
        onMinGamesChange={setMinGames}
      />

      <main className="min-w-0 flex-1 space-y-10">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-xl font-semibold text-neutral-900 dark:text-neutral-100">
              Fantasy Premier League Insights
            </h1>
            <LastUpdated date={lastUpdated} />
          </div>
          <ExportCsvButton players={filtered} />
        </div>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Value Analysis
          </h2>
          <ScatterChart
            data={filtered}
            title="Total Points vs Price (size: Value Added)"
            xKey="price"
            yKey="totalPoints"
            xLabel="Price (£m)"
            yLabel="Total Points"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="valueAdded"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "price", label: "Price", format: (v) => `£${fmt1(v)}m` },
              { key: "totalPoints", label: "Total Points" },
              { key: "valueAdded", label: "Value Added", format: fmt2 },
              { key: "pointsPerMillion", label: "Pts / £m", format: fmt1 },
            ]}
          />
          <div className="mt-4">
            <h3 className="mb-2 text-sm font-semibold text-neutral-900 dark:text-neutral-100">
              Top 10 Players by Value Added
            </h3>
            <TopValueTable players={filtered} />
          </div>
        </section>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Performance vs Expectation
          </h2>
          <ScatterChart
            data={filtered}
            title="Expected Goal Involvement vs Actual Goal Involvement per 90"
            xKey="xgiPer90"
            yKey="goalInvolvementPer90"
            xLabel="xGI per 90"
            yLabel="Goal Involvement per 90"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="minutes"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "xgiPer90", label: "xGI / 90", format: fmt2 },
              { key: "goalInvolvementPer90", label: "GI / 90", format: fmt2 },
              { key: "goalInvolvementOverperformance", label: "Overperformance", format: fmt2 },
              { key: "minutes", label: "Minutes" },
            ]}
          />
        </section>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Form Analysis
          </h2>
          <p className="mb-2 text-xs text-neutral-500 dark:text-neutral-400">
            The old dashboard&apos;s &quot;consistency score&quot; required per-gameweek
            history that this v1 doesn&apos;t ingest (see README) — shown here as
            form vs points-per-game instead.
          </p>
          <ScatterChart
            data={filtered}
            title="Form vs Points per Game (size: Total Points)"
            xKey="form"
            yKey="pointsPerGame"
            xLabel="Form"
            yLabel="Points per Game"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="totalPoints"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "form", label: "Form", format: fmt1 },
              { key: "pointsPerGame", label: "Pts / Game", format: fmt1 },
              { key: "totalPoints", label: "Total Points" },
              { key: "selectedByPercent", label: "Selected By", format: (v) => `${fmt1(v)}%` },
            ]}
          />
        </section>

        {(position === "GK" || position === "All") && gkData.length > 0 && (
          <section>
            <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
              Goalkeeper Special
            </h2>
            <ScatterChart
              data={gkData}
              title="Saves per 90 vs Clean Sheet % (size: Total Points)"
              xKey="savesPer90"
              yKey="cleanSheetPercentage"
              xLabel="Saves per 90"
              yLabel="Clean Sheet %"
              sizeKey="totalPoints"
              tooltipFields={[
                { key: "webName", label: "Player" },
                { key: "teamName", label: "Team" },
                { key: "savesPer90", label: "Saves / 90", format: fmt2 },
                { key: "cleanSheetPercentage", label: "Clean Sheet %", format: fmt2 },
                { key: "goalsConcededPer90", label: "Conceded / 90", format: fmt2 },
                { key: "totalPoints", label: "Total Points" },
              ]}
            />
          </section>
        )}

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Differential Picks
          </h2>
          <ScatterChart
            data={filtered}
            title="Total Points vs Selection % (size: Value Added)"
            xKey="selectedByPercent"
            yKey="totalPoints"
            xLabel="Selected By %"
            yLabel="Total Points"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="valueAdded"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "selectedByPercent", label: "Selected By", format: (v) => `${fmt1(v)}%` },
              { key: "totalPoints", label: "Total Points" },
              { key: "valueAdded", label: "Value Added", format: fmt2 },
              { key: "pointsPerMillion", label: "Pts / £m", format: fmt1 },
            ]}
          />
        </section>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Bonus Point Analysis
          </h2>
          <ScatterChart
            data={filtered}
            title="Bonus Points per 90 vs BPS per 90"
            xKey="bpsPer90"
            yKey="bonusPointsPer90"
            xLabel="BPS per 90"
            yLabel="Bonus Points per 90"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="totalPoints"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "bpsPer90", label: "BPS / 90", format: fmt1 },
              { key: "bonusPointsPer90", label: "Bonus / 90", format: fmt2 },
              { key: "totalPoints", label: "Total Points" },
            ]}
          />
        </section>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Overall Contribution
          </h2>
          <ScatterChart
            data={filtered}
            title="ICT Index per 90 vs Total Contribution per 90"
            xKey="ictIndexPer90"
            yKey="totalContributionPer90"
            xLabel="ICT Index per 90"
            yLabel="Total Contribution per 90"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="totalPoints"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "ictIndexPer90", label: "ICT / 90", format: fmt2 },
              { key: "totalContributionPer90", label: "Contribution / 90", format: fmt2 },
              { key: "totalPoints", label: "Total Points" },
            ]}
          />
        </section>

        <section>
          <h2 className="mb-3 text-lg font-medium text-neutral-900 dark:text-neutral-100">
            Risk Analysis
          </h2>
          <ScatterChart
            data={riskData}
            title="Card Rate vs Minutes per Goal Involvement"
            xKey="cardRate"
            yKey="minutesPerGoalInvolvement"
            xLabel="Card Rate"
            yLabel="Minutes per Goal Involvement"
            colorKey="position"
            colorOrder={POSITION_ORDER}
            sizeKey="totalPoints"
            tooltipFields={[
              { key: "webName", label: "Player" },
              { key: "teamName", label: "Team" },
              { key: "cardRate", label: "Card Rate", format: fmt2 },
              { key: "minutesPerGoalInvolvement", label: "Min / GI", format: fmt1 },
              { key: "totalPoints", label: "Total Points" },
            ]}
          />
        </section>
      </main>
    </div>
  );
}
