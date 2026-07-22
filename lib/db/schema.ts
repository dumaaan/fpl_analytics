import {
  pgTable,
  integer,
  text,
  real,
  timestamp,
  serial,
} from "drizzle-orm/pg-core";

export const players = pgTable("players", {
  id: integer("id").primaryKey(), // FPL element id
  code: integer("code").notNull(),
  webName: text("web_name").notNull(),
  firstName: text("first_name").notNull(),
  secondName: text("second_name").notNull(),
  teamId: integer("team_id").notNull(),
  teamName: text("team_name").notNull(),
  elementType: integer("element_type").notNull(),
  position: text("position").notNull(), // GK / DEF / MID / FWD

  // Raw counting stats
  nowCost: integer("now_cost").notNull(),
  totalPoints: integer("total_points").notNull(),
  pointsPerGame: real("points_per_game").notNull(),
  minutes: integer("minutes").notNull(),
  goalsScored: integer("goals_scored").notNull(),
  assists: integer("assists").notNull(),
  cleanSheets: integer("clean_sheets").notNull(),
  goalsConceded: integer("goals_conceded").notNull(),
  ownGoals: integer("own_goals").notNull(),
  penaltiesSaved: integer("penalties_saved").notNull(),
  penaltiesMissed: integer("penalties_missed").notNull(),
  yellowCards: integer("yellow_cards").notNull(),
  redCards: integer("red_cards").notNull(),
  saves: integer("saves").notNull(),
  bonus: integer("bonus").notNull(),
  bps: integer("bps").notNull(),
  starts: integer("starts").notNull(),
  influence: real("influence").notNull(),
  creativity: real("creativity").notNull(),
  threat: real("threat").notNull(),
  ictIndex: real("ict_index").notNull(),
  expectedGoals: real("expected_goals").notNull(),
  expectedAssists: real("expected_assists").notNull(),
  expectedGoalInvolvements: real("expected_goal_involvements").notNull(),
  expectedGoalsConceded: real("expected_goals_conceded").notNull(),
  selectedByPercent: real("selected_by_percent").notNull(),
  form: real("form").notNull(),

  // Derived metrics (computed at ingest time, see scripts/ingest/metrics.ts)
  price: real("price").notNull(),
  goalsPer90: real("goals_per_90").notNull(),
  assistsPer90: real("assists_per_90").notNull(),
  xgPer90: real("xg_per_90").notNull(),
  xaPer90: real("xa_per_90").notNull(),
  pointsPerMillion: real("points_per_million").notNull(),
  pointsPer90: real("points_per_90").notNull(),
  valueAdded: real("value_added").notNull(),
  goalInvolvementPer90: real("goal_involvement_per_90").notNull(),
  xgiPer90: real("xgi_per_90").notNull(),
  goalInvolvementOverperformance: real(
    "goal_involvement_overperformance",
  ).notNull(),
  cleanSheetPercentage: real("clean_sheet_percentage").notNull(),
  savesPer90: real("saves_per_90").notNull(),
  goalsConcededPer90: real("goals_conceded_per_90").notNull(),
  bonusPointsPer90: real("bonus_points_per_90").notNull(),
  bpsPer90: real("bps_per_90").notNull(),
  ictIndexPer90: real("ict_index_per_90").notNull(),
  totalContributionPer90: real("total_contribution_per_90").notNull(),
  minutesPerGoalInvolvement: real("minutes_per_goal_involvement"), // nullable
  pointsPerMinute: real("points_per_minute").notNull(),
  cardRate: real("card_rate").notNull(),

  updatedAt: timestamp("updated_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export const dataRefreshLog = pgTable("data_refresh_log", {
  id: serial("id").primaryKey(),
  refreshedAt: timestamp("refreshed_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  playerCount: integer("player_count").notNull(),
  status: text("status").notNull(), // 'success' | 'error'
});

export type Player = typeof players.$inferSelect;
export type NewPlayer = typeof players.$inferInsert;
