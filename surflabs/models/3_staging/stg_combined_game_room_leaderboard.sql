{{
    config(
        materialized    = 'table',
        tags            = ['sch_4hrs', 'sch_daily', 'off_chain', 'crypto_unicorns', 'leaderboard', 'warehouse'],
    )
}}

with

game_room as (
    select
        raw_combined_game_room_leaderboard.*,
        prize_pool.key as position,
        prize_pool.value as winnings
    from {{ ref('raw_combined_game_room_leaderboard') }},
        lateral flatten(input => prize_pool) as prize_pool
)

select
    *,
    {{ surrogate_key_w_null(['game_room_id', 'position']) }} as game_room_position_id
from game_room
