// Subset of the FPL bootstrap-static response we actually use.
// Note: the API returns many numeric fields as strings (e.g. "3.4").

export interface FplElement {
  id: number;
  code: number;
  web_name: string;
  first_name: string;
  second_name: string;
  team: number;
  element_type: number;
  now_cost: number;
  total_points: number;
  points_per_game: string;
  minutes: number;
  goals_scored: number;
  assists: number;
  clean_sheets: number;
  goals_conceded: number;
  own_goals: number;
  penalties_saved: number;
  penalties_missed: number;
  yellow_cards: number;
  red_cards: number;
  saves: number;
  bonus: number;
  bps: number;
  starts: number;
  influence: string;
  creativity: string;
  threat: string;
  ict_index: string;
  expected_goals: string;
  expected_assists: string;
  expected_goal_involvements: string;
  expected_goals_conceded: string;
  selected_by_percent: string;
  form: string;
}

export interface FplTeam {
  id: number;
  name: string;
}

export interface FplBootstrapStatic {
  elements: FplElement[];
  teams: FplTeam[];
}

export const POSITION_BY_ELEMENT_TYPE: Record<number, string> = {
  1: "GK",
  2: "DEF",
  3: "MID",
  4: "FWD",
};
