{{ config(materialized='table') }}

SELECT
  id as player_id,
  -- We don't have fixture and opponent_team in this data, so we'll omit those
  total_points,
  minutes,
  goals_scored,
  assists,
  clean_sheets,
  goals_conceded,
  own_goals,
  penalties_saved,
  penalties_missed,
  yellow_cards,
  red_cards,
  saves,
  bonus,
  bps,
  influence,
  creativity,
  threat,
  ict_index,
  -- Additional relevant columns
  element_type,
  team,
  now_cost,
  selected_by_percent,
  form,
  points_per_game,
  value_form,
  value_season,
  starts,
  expected_goals,
  expected_assists,
  expected_goal_involvements,
  expected_goals_conceded
FROM {{ source('fpl_api', 'players') }}