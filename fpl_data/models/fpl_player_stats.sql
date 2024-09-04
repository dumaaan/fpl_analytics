{{ config(materialized='table') }}

WITH player_base AS (
    SELECT
        p.*,
        CAST(p.now_cost AS DECIMAL(5,1)) / 10.0 AS price,
        CAST(p.selected_by_percent AS FLOAT) AS selected_by_percent,
        CAST(p.form AS FLOAT) AS form,
        CAST(p.points_per_game AS FLOAT) AS points_per_game,
        CAST(p.total_points AS INTEGER) AS total_points
    FROM {{ ref('dim_players') }} p
),
player_stats AS (
    SELECT
        s.*,
        CAST(s.minutes AS INTEGER) AS minutes,
        CAST(s.goals_scored AS INTEGER) AS goals_scored,
        CAST(s.assists AS INTEGER) AS assists,
        CAST(s.clean_sheets AS INTEGER) AS clean_sheets,
        CAST(s.goals_conceded AS INTEGER) AS goals_conceded,
        CAST(s.own_goals AS INTEGER) AS own_goals,
        CAST(s.penalties_saved AS INTEGER) AS penalties_saved,
        CAST(s.penalties_missed AS INTEGER) AS penalties_missed,
        CAST(s.yellow_cards AS INTEGER) AS yellow_cards,
        CAST(s.red_cards AS INTEGER) AS red_cards,
        CAST(s.saves AS INTEGER) AS saves,
        CAST(s.bonus AS INTEGER) AS bonus,
        CAST(s.bps AS INTEGER) AS bps,
        CAST(s.influence AS FLOAT) AS influence,
        CAST(s.creativity AS FLOAT) AS creativity,
        CAST(s.threat AS FLOAT) AS threat,
        CAST(s.ict_index AS FLOAT) AS ict_index,
        CAST(s.expected_goals AS FLOAT) AS expected_goals,
        CAST(s.expected_assists AS FLOAT) AS expected_assists,
        CAST(s.expected_goal_involvements AS FLOAT) AS expected_goal_involvements,
        CAST(s.expected_goals_conceded AS FLOAT) AS expected_goals_conceded,
        CAST(s.starts AS INTEGER) AS starts
    FROM {{ ref('fact_player_history') }} s
)

SELECT
    p.*,
    s.minutes,
    s.goals_scored,
    s.assists,
    s.clean_sheets,
    s.goals_conceded,
    s.own_goals,
    s.penalties_saved,
    s.penalties_missed,
    s.yellow_cards,
    s.red_cards,
    s.saves,
    s.bonus,
    s.bps,
    s.influence,
    s.creativity,
    s.threat,
    s.ict_index,
    s.expected_goals,
    s.expected_assists,
    s.expected_goal_involvements,
    s.expected_goals_conceded,
    CASE 
        WHEN CAST(p.element_type AS INTEGER) = 1 THEN 'GK'
        WHEN CAST(p.element_type AS INTEGER) = 2 THEN 'DEF'
        WHEN CAST(p.element_type AS INTEGER) = 3 THEN 'MID'
        WHEN CAST(p.element_type AS INTEGER) = 4 THEN 'FWD'
        ELSE 'Unknown'
    END AS position,
    CASE 
        WHEN CAST(s.minutes AS INTEGER) > 0 THEN (CAST(s.goals_scored AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) 
        ELSE 0 
    END AS goals_per_90,
    CASE 
        WHEN CAST(s.minutes AS INTEGER) > 0 THEN (CAST(s.assists AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) 
        ELSE 0 
    END AS assists_per_90,
    CASE 
        WHEN CAST(s.minutes AS INTEGER) > 0 THEN (CAST(s.expected_goals AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) 
        ELSE 0 
    END AS xG_per_90,
    CASE 
        WHEN CAST(s.minutes AS INTEGER) > 0 THEN (CAST(s.expected_assists AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) 
        ELSE 0 
    END AS xA_per_90,
    CASE 
        WHEN CAST(p.price AS FLOAT) > 0 THEN (CAST(p.total_points AS FLOAT) / CAST(p.price AS FLOAT)) 
        ELSE 0 
    END AS points_per_million,
    CASE 
        WHEN CAST(s.minutes AS INTEGER) > 0 THEN (CAST(p.total_points AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) 
        ELSE 0 
    END AS points_per_90,
    CASE 
        WHEN CAST(s.starts AS INTEGER) > 0 THEN (CAST(s.minutes AS FLOAT) / CAST(s.starts AS FLOAT)) 
        ELSE 0 
    END AS minutes_per_start,
    CURRENT_TIMESTAMP AS last_updated
FROM player_base p
JOIN player_stats s ON p.id = s.player_id