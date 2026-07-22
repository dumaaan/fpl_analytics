import type { NewPlayer } from "@/lib/db/schema";
import { POSITION_BY_ELEMENT_TYPE, type FplElement } from "./fpl-types";

function per90(stat: number, minutes: number): number {
  return minutes > 0 ? (stat * 90) / minutes : 0;
}

/** Average total_points per position, used for the value_added metric. */
export function computePositionAverages(
  elements: FplElement[],
): Map<string, number> {
  const sums = new Map<string, { total: number; count: number }>();
  for (const el of elements) {
    const position = POSITION_BY_ELEMENT_TYPE[el.element_type] ?? "Unknown";
    const entry = sums.get(position) ?? { total: 0, count: 0 };
    entry.total += el.total_points;
    entry.count += 1;
    sums.set(position, entry);
  }
  const averages = new Map<string, number>();
  for (const [position, { total, count }] of sums) {
    averages.set(position, count > 0 ? total / count : 0);
  }
  return averages;
}

/**
 * Ports the derived-metric formulas from the old dbt model
 * (fpl_data/models/fpl_player_stats.sql) to TypeScript, computed at ingest
 * time instead of via a SQL view. consistency_score is intentionally
 * dropped — it was a no-op (stddev over a single row) without per-gameweek
 * history, which is out of scope for v1.
 */
export function buildPlayerRow(
  el: FplElement,
  teamName: string,
  positionAveragePoints: Map<string, number>,
): NewPlayer {
  const position = POSITION_BY_ELEMENT_TYPE[el.element_type] ?? "Unknown";
  const price = el.now_cost / 10.0;
  const minutes = el.minutes;

  const goalsPer90 = per90(el.goals_scored, minutes);
  const assistsPer90 = per90(el.assists, minutes);
  const expectedGoals = parseFloat(el.expected_goals) || 0;
  const expectedAssists = parseFloat(el.expected_assists) || 0;
  const xgPer90 = per90(expectedGoals, minutes);
  const xaPer90 = per90(expectedAssists, minutes);

  const avgPointsForPosition = positionAveragePoints.get(position) ?? 0;

  return {
    id: el.id,
    code: el.code,
    webName: el.web_name,
    firstName: el.first_name,
    secondName: el.second_name,
    teamId: el.team,
    teamName,
    elementType: el.element_type,
    position,

    nowCost: el.now_cost,
    totalPoints: el.total_points,
    pointsPerGame: parseFloat(el.points_per_game) || 0,
    minutes,
    goalsScored: el.goals_scored,
    assists: el.assists,
    cleanSheets: el.clean_sheets,
    goalsConceded: el.goals_conceded,
    ownGoals: el.own_goals,
    penaltiesSaved: el.penalties_saved,
    penaltiesMissed: el.penalties_missed,
    yellowCards: el.yellow_cards,
    redCards: el.red_cards,
    saves: el.saves,
    bonus: el.bonus,
    bps: el.bps,
    starts: el.starts,
    influence: parseFloat(el.influence) || 0,
    creativity: parseFloat(el.creativity) || 0,
    threat: parseFloat(el.threat) || 0,
    ictIndex: parseFloat(el.ict_index) || 0,
    expectedGoals,
    expectedAssists,
    expectedGoalInvolvements: parseFloat(el.expected_goal_involvements) || 0,
    expectedGoalsConceded: parseFloat(el.expected_goals_conceded) || 0,
    selectedByPercent: parseFloat(el.selected_by_percent) || 0,
    form: parseFloat(el.form) || 0,

    price,
    goalsPer90,
    assistsPer90,
    xgPer90,
    xaPer90,
    pointsPerMillion: price > 0 ? el.total_points / price : 0,
    pointsPer90: per90(el.total_points, minutes),
    valueAdded: price > 0 ? (el.total_points - avgPointsForPosition) / price : 0,
    goalInvolvementPer90: goalsPer90 + assistsPer90,
    xgiPer90: xgPer90 + xaPer90,
    goalInvolvementOverperformance:
      el.goals_scored + el.assists - (expectedGoals + expectedAssists),
    cleanSheetPercentage:
      minutes > 0 ? el.clean_sheets / (minutes / 90) : 0,
    savesPer90: per90(el.saves, minutes),
    goalsConcededPer90: per90(el.goals_conceded, minutes),
    bonusPointsPer90: per90(el.bonus, minutes),
    bpsPer90: per90(el.bps, minutes),
    ictIndexPer90: per90(parseFloat(el.ict_index) || 0, minutes),
    totalContributionPer90:
      minutes > 0
        ? ((el.goals_scored * 4 +
            el.assists * 3 +
            el.clean_sheets * 4 -
            el.goals_conceded * 0.5 +
            el.saves * 0.5) *
            90) /
          minutes
        : 0,
    minutesPerGoalInvolvement:
      el.goals_scored + el.assists > 0
        ? minutes / (el.goals_scored + el.assists)
        : null,
    pointsPerMinute: minutes > 0 ? el.total_points / minutes : 0,
    cardRate: per90(el.yellow_cards + el.red_cards * 2, minutes),
  };
}
