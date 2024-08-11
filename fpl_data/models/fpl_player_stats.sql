{{ config(materialized='table') }}

WITH player_base AS (
    SELECT
        p.id,
        p.first_name,
        p.second_name,
        p.web_name,
        p.team,
        p.element_type,
        p.now_cost / 10.0 AS price,  -- Convert to actual price in millions
        CAST(p.selected_by_percent AS FLOAT) AS selected_by_percent,
        CAST(p.form AS FLOAT) AS form,
        CAST(p.points_per_game AS FLOAT) AS points_per_game,
        p.total_points,
        h.minutes,
        h.goals_scored,
        h.assists,
        h.clean_sheets,
        h.goals_conceded,
        h.own_goals,
        h.penalties_saved,
        h.penalties_missed,
        h.yellow_cards,
        h.red_cards,
        h.saves,
        h.bonus,
        h.bps,
        CAST(h.influence AS FLOAT) AS influence,
        CAST(h.creativity AS FLOAT) AS creativity,
        CAST(h.threat AS FLOAT) AS threat,
        CAST(h.ict_index AS FLOAT) AS ict_index,
        h.starts,
        CAST(h.expected_goals AS FLOAT) AS expected_goals,
        CAST(h.expected_assists AS FLOAT) AS expected_assists,
        CAST(h.expected_goal_involvements AS FLOAT) AS expected_goal_involvements,
        CAST(h.expected_goals_conceded AS FLOAT) AS expected_goals_conceded
    FROM {{ ref('dim_players') }} p
    JOIN {{ ref('fact_player_history') }} h ON p.id = h.player_id
)

SELECT
    *,
    CASE 
        WHEN element_type = 1 THEN 'GK'
        WHEN element_type = 2 THEN 'DEF'
        WHEN element_type = 3 THEN 'MID'
        WHEN element_type = 4 THEN 'FWD'
    END AS position,
    CASE 
        WHEN minutes > 0 THEN (goals_scored * 90.0 / minutes) 
        ELSE 0 
    END AS goals_per_90,
    CASE 
        WHEN minutes > 0 THEN (assists * 90.0 / minutes) 
        ELSE 0 
    END AS assists_per_90,
    CASE 
        WHEN minutes > 0 THEN (expected_goals * 90.0 / minutes) 
        ELSE 0 
    END AS xG_per_90,
    CASE 
        WHEN minutes > 0 THEN (expected_assists * 90.0 / minutes) 
        ELSE 0 
    END AS xA_per_90,
    CASE 
        WHEN price > 0 THEN (total_points / price) 
        ELSE 0 
    END AS points_per_million,
    CASE 
        WHEN minutes > 0 THEN (total_points * 90.0 / minutes) 
        ELSE 0 
    END AS points_per_90,
    CASE 
        WHEN starts > 0 THEN (minutes / starts) 
        ELSE 0 
    END AS minutes_per_start
FROM player_base