{{ config(materialized='table') }}

SELECT
  id,
  first_name,
  second_name,
  web_name,
  team,
  element_type,
  now_cost,
  selected_by_percent,
  form,
  points_per_game,
  total_points
FROM {{ source('fpl_api', 'players') }}