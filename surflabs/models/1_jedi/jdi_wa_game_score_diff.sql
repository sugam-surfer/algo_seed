{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

with

score_differences as (
    select
        app_id,
        game_type,
        room_id,
        or_entry_fee_amount,
        match_id,
        abs(max(score) - min(score)) as score_difference
    from {{ ref('stg_padme_game_play') }}
    where game_status like '04%'
        and is_fake = 'normal'
        and created_at >= dateadd(day, -{{ var('lookback_days_for_score_diff') }}, current_date)
    {{ dbt_utils.group_by(5) }}
),

stats as (
    select
        app_id,
        game_type,
        room_id,
        or_entry_fee_amount,
        avg(score_difference) as avg_diff,
        stddev(score_difference) as stddev_diff,
        count(*) as match_count
    from score_differences
    {{ dbt_utils.group_by(4) }}
)

select
    app_id,
    game_type,
    room_id,
    or_entry_fee_amount,
    avg_diff as avg_score_difference,
    stddev_diff as stddev_score_difference,
    match_count,
    avg_diff - 2 * stddev_diff as lower_bound,
    avg_diff + 2 * stddev_diff as upper_bound
from stats
having match_count > {{ var('min_games_for_score_diff') }}
