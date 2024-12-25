{{
    config(
        materialized = 'incremental',
        unique_key   = 'user_id',
        cluster_by   = 'user_id',
        tags         = ['reseed', 'warehouse'],
    )
}}


WITH recent_games AS (
    SELECT 
        user_id,
        day_created_at,
        usergames_played
    FROM 
        {{ ref('jdi_day_padme_game_play') }} 
    WHERE 
        day_created_at >= DATEADD(DAY, -7, CURRENT_DATE)
)

SELECT 
    user_id,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -7, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_7,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -6, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_6,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -5, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_5,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -4, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_4,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -3, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_3,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -2, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_2,
    MAX(CASE WHEN day_created_at = DATEADD(DAY, -1, CURRENT_DATE) THEN usergames_played ELSE 0 END) AS day_1,
    day_7 + day_6 + day_5 + day_4 + day_3 + day_2 + day_1 AS day_all,
    round((day_7 + day_6 + day_5 + day_4 + day_3 + day_2 + day_1)/7,2) AS week_avg,
    round((day_4 + day_3 + day_2 + day_1)/4,2) AS l4_avg,
    ceil((week_avg + l4_avg + day_1)/3) as day_weighted_usergames
FROM 
    recent_games
GROUP BY 
    user_id
ORDER BY 
    day_weighted_usergames desc
