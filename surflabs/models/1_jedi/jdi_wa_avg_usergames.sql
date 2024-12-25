{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

select
    app_id,
    user_id,
    AVG(usergames_played) as games_per_day
from {{ ref('jdi_day_padme_game_play') }}
where game_status = '04. finished'
    and payout_status in ('win', 'no_win')
    and day_created_at >= dateadd(day, -{{ var('lookback_days_for_avg_games') }}, CURRENT_DATE)
    and day_created_at < CURRENT_DATE
{{ dbt_utils.group_by(2) }}
