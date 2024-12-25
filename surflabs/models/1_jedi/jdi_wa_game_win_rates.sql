{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

with

game_stats as (
  select 
    app_id,
    game_type,
    is_fake,
    user_id,
    sum(usergames_played) AS total_games,
    sum(case when payout_status = 'win' then usergames_played else 0 end) as winning_games
  from {{ ref('jdi_day_padme_game_play') }}
  where payout_status in ('no_win', 'win')
    and game_status = '04. finished'
  {{ dbt_utils.group_by(4) }}
)

select 
  app_id,
  game_type,
  is_fake,
  sum(total_games) as total_games,
  avg(winning_games/total_games) as average_win_rate,
  stddev(winning_games/total_games) as stdev_win_rate,
  average_win_rate - {{ var('multiplier_confidence_interval') }} * stdev_win_rate as ci_low,
  average_win_rate + {{ var('multiplier_confidence_interval') }} * stdev_win_rate as ci_high
from game_stats
where total_games >= {{ var('min_games_per_user_for_win_rate') }}
{{ dbt_utils.group_by(3) }}
