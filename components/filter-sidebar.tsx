"use client";

import { POSITIONS, type Position } from "@/lib/types";

interface Bounds {
  minPrice: number;
  maxPrice: number;
  minPoints: number;
  maxPoints: number;
  minGames: number;
  maxGames: number;
}

interface FilterSidebarProps {
  bounds: Bounds;
  position: Position;
  onPositionChange: (p: Position) => void;
  minPrice: number;
  onMinPriceChange: (v: number) => void;
  minPoints: number;
  onMinPointsChange: (v: number) => void;
  minGames: number;
  onMinGamesChange: (v: number) => void;
}

export function FilterSidebar({
  bounds,
  position,
  onPositionChange,
  minPrice,
  onMinPriceChange,
  minPoints,
  onMinPointsChange,
  minGames,
  onMinGamesChange,
}: FilterSidebarProps) {
  return (
    <aside className="w-full shrink-0 space-y-6 lg:w-64">
      <h2 className="text-sm font-semibold text-neutral-900 dark:text-neutral-100">
        Filters
      </h2>

      <div>
        <label className="mb-1 block text-xs text-neutral-600 dark:text-neutral-400">
          Position
        </label>
        <select
          value={position}
          onChange={(e) => onPositionChange(e.target.value as Position)}
          className="w-full rounded border border-neutral-300 bg-white px-2 py-1.5 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
        >
          {POSITIONS.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
      </div>

      <div>
        <label className="mb-1 flex justify-between text-xs text-neutral-600 dark:text-neutral-400">
          <span>Minimum Price</span>
          <span>£{minPrice.toFixed(1)}m</span>
        </label>
        <input
          type="range"
          min={bounds.minPrice}
          max={bounds.maxPrice}
          step={0.1}
          value={minPrice}
          onChange={(e) => onMinPriceChange(Number(e.target.value))}
          className="w-full"
        />
      </div>

      <div>
        <label className="mb-1 flex justify-between text-xs text-neutral-600 dark:text-neutral-400">
          <span>Minimum Total Points</span>
          <span>{minPoints}</span>
        </label>
        <input
          type="range"
          min={bounds.minPoints}
          max={bounds.maxPoints}
          step={1}
          value={minPoints}
          onChange={(e) => onMinPointsChange(Number(e.target.value))}
          className="w-full"
        />
      </div>

      <div>
        <label className="mb-1 flex justify-between text-xs text-neutral-600 dark:text-neutral-400">
          <span>Minimum Games Played</span>
          <span>{minGames.toFixed(1)}</span>
        </label>
        <input
          type="range"
          min={bounds.minGames}
          max={bounds.maxGames}
          step={0.5}
          value={minGames}
          onChange={(e) => onMinGamesChange(Number(e.target.value))}
          className="w-full"
        />
      </div>
    </aside>
  );
}
