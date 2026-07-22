// Validated categorical palette (see dataviz skill references/palette.md).
// Fixed slot order — colors are assigned to positions, never cycled by rank.
export const CATEGORICAL_LIGHT = [
  "#2a78d6", // slot 1 blue
  "#1baf7a", // slot 2 aqua
  "#eda100", // slot 3 yellow
  "#008300", // slot 4 green
];

export const CATEGORICAL_DARK = [
  "#3987e5",
  "#199e70",
  "#c98500",
  "#008300",
];

// Fixed order so a position always gets the same color regardless of filters.
export const POSITION_ORDER = ["GK", "DEF", "MID", "FWD"] as const;

export const CHART_INK_LIGHT = {
  primary: "#0b0b0b",
  secondary: "#52514e",
  muted: "#898781",
  grid: "#e1e0d9",
  surface: "#fcfcfb",
};

export const CHART_INK_DARK = {
  primary: "#ffffff",
  secondary: "#c3c2b7",
  muted: "#898781",
  grid: "#2c2c2a",
  surface: "#1a1a19",
};

export const SINGLE_SERIES_LIGHT = "#2a78d6";
export const SINGLE_SERIES_DARK = "#3987e5";
