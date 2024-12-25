{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

select
  app_id,
  user_id,
  game_type,
  is_fake,
  sum(usergames_played) as total_games,
  sum(CASE WHEN payout_status = 'win' THEN usergames_played ELSE 0 END) AS winning_games,
  winning_games/total_games as win_rate
from {{ ref('jdi_day_padme_game_play') }}
where game_status = '04. finished'
    and payout_status in ('win', 'no_win')
{{ dbt_utils.group_by(4) }}
