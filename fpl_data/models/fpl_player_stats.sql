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
),
base_metrics AS (
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
        s.starts,
        CASE 
            WHEN CAST(p.element_type AS INTEGER) = 1 THEN 'GK'
            WHEN CAST(p.element_type AS INTEGER) = 2 THEN 'DEF'
            WHEN CAST(p.element_type AS INTEGER) = 3 THEN 'MID'
            WHEN CAST(p.element_type AS INTEGER) = 4 THEN 'FWD'
            ELSE 'Unknown'
        END AS position,
        CASE WHEN CAST(s.minutes AS FLOAT) > 0 THEN (CAST(s.goals_scored AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) ELSE 0 END AS goals_per_90,
        CASE WHEN CAST(s.minutes AS FLOAT) > 0 THEN (CAST(s.assists AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) ELSE 0 END AS assists_per_90,
        CASE WHEN CAST(s.minutes AS FLOAT) > 0 THEN (CAST(s.expected_goals AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) ELSE 0 END AS xG_per_90,
        CASE WHEN CAST(s.minutes AS FLOAT) > 0 THEN (CAST(s.expected_assists AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) ELSE 0 END AS xA_per_90,
        CASE WHEN CAST(p.price AS FLOAT) > 0 THEN (CAST(p.total_points AS FLOAT) / CAST(p.price AS FLOAT)) ELSE 0 END AS points_per_million,
        CASE WHEN CAST(s.minutes AS FLOAT) > 0 THEN (CAST(p.total_points AS FLOAT) * 90.0 / CAST(s.minutes AS FLOAT)) ELSE 0 END AS points_per_90,
        CASE WHEN CAST(s.starts AS FLOAT) > 0 THEN (CAST(s.minutes AS FLOAT) / CAST(s.starts AS FLOAT)) ELSE 0 END AS minutes_per_start
    FROM player_base p
    JOIN player_stats s ON p.id = s.player_id
),
advanced_metrics AS (
    SELECT 
        *,
        -- Value Metrics
        (CAST(total_points AS FLOAT) - (SELECT AVG(CAST(total_points AS FLOAT)) FROM base_metrics b WHERE b.position = base_metrics.position)) / CAST(price AS FLOAT) AS value_added,
        
        -- Consistency Metrics
        STDDEV(CAST(points_per_game AS FLOAT)) OVER (PARTITION BY id) AS consistency_score,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(starts AS FLOAT) / (CAST(minutes AS FLOAT) / 90) ELSE 0 END AS start_percentage,
        
        -- Attacking Metrics
        CAST(goals_per_90 AS FLOAT) + CAST(assists_per_90 AS FLOAT) AS goal_involvement_per_90,
        CAST(xG_per_90 AS FLOAT) + CAST(xA_per_90 AS FLOAT) AS xGI_per_90,
        (CAST(goals_scored AS FLOAT) + CAST(assists AS FLOAT)) - (CAST(expected_goals AS FLOAT) + CAST(expected_assists AS FLOAT)) AS goal_involvement_overperformance,
        
        -- Defensive Metrics
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(clean_sheets AS FLOAT) / (CAST(minutes AS FLOAT) / 90) ELSE 0 END AS clean_sheet_percentage,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(saves AS FLOAT) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS saves_per_90,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(goals_conceded AS FLOAT) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS goals_conceded_per_90,
        
        -- Bonus Point Metrics
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(bonus AS FLOAT) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS bonus_points_per_90,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(bps AS FLOAT) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS bps_per_90,
        
        -- Overall Performance Metrics
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(ict_index AS FLOAT) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS ict_index_per_90,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN 
            (CAST(goals_scored AS FLOAT) * 4 + CAST(assists AS FLOAT) * 3 + CAST(clean_sheets AS FLOAT) * 4 - CAST(goals_conceded AS FLOAT) * 0.5 + CAST(saves AS FLOAT) * 0.5) * 90 / CAST(minutes AS FLOAT) 
        ELSE 0 END AS total_contribution_per_90,
        
        -- Efficiency Metrics
        CASE WHEN (CAST(goals_scored AS FLOAT) + CAST(assists AS FLOAT)) > 0 THEN CAST(minutes AS FLOAT) / (CAST(goals_scored AS FLOAT) + CAST(assists AS FLOAT)) ELSE NULL END AS minutes_per_goal_involvement,
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN CAST(total_points AS FLOAT) / CAST(minutes AS FLOAT) ELSE 0 END AS points_per_minute,
        
        -- Risk Metrics
        CASE WHEN CAST(minutes AS FLOAT) > 0 THEN (CAST(yellow_cards AS FLOAT) + CAST(red_cards AS FLOAT) * 2) * 90 / CAST(minutes AS FLOAT) ELSE 0 END AS card_rate
    FROM base_metrics
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS last_updated
FROM advanced_metrics
ORDER BY total_points DESC