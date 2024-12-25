{{
    config(
        materialized    = 'table',
        tags            = ['sch_4hrs', 'sch_daily', 'off_chain', 'crypto_unicorns', 'leaderboard', 'warehouse'],
    )
}}
-- TODO: This should be incremental

with

skywalker_game_rooms as (
    select
        id as game_room_id,
        app_id,
        null as room_type,
        name as game_room_name,
        fee_cur as fee_currency,
        fee_amt as fee_amount,
        prize_cur as prize_currency,
        prize_amt as prize_amount,
        cur_icon as currency_icon,
        max_players,
        banner_txt as banner_text,
        banner_help_txt as banner_help_text,
        wager_brackets,
        parse_json(prize_pool) as prize_pool,
        xp_multiplier,
        flags,
        created_at,
        updated_at
    from {{ source('skywalker', 'game_room') }}
),

trivia_prize_pool_prep as (
    select id, to_char(index+1) as key, value::number(10,2) as value
    from {{ source('padme', 'game_room') }},
    lateral flatten(INPUT => parse_json(prize_pool):rewards, outer => true) as prize_pool
),

trivia_prize_pool as (
    select distinct id, object_agg(key, value) over (partition by id) as prize_pool
    from trivia_prize_pool_prep
),

padme_game_rooms as (
    select
        game_room.id as game_room_id,
        app_id,
        typ as room_type,
        name as game_room_name,
        parse_json(entry_fee):currency::string as fee_currency,
        parse_json(entry_fee):amount::float as fee_amount,
        parse_json(game_room.prize_pool):currency::string as prize_currency,
        parse_json(game_room.prize_pool):rewards[0]::float as prize_amount,
        null as currency_icon,
        max_players,
        null as banner_text,
        null as banner_help_text,
        null as wager_brackets,
        trivia_prize_pool.prize_pool as prize_pool,
        xp_multiplier,
        flags,
        created_at,
        updated_at
    from {{ source('padme', 'game_room') }}
    left join trivia_prize_pool on trivia_prize_pool.id = game_room.id
)

select *
from skywalker_game_rooms

union all

select *
from padme_game_rooms


/*
game_rooms as (
    select
        *
    from {{ source('skywalker', 'game_room') }}
)

select
    id as game_room_id,
    app_id,
    name as game_room_name,
    fee_cur as fee_currency,
    fee_amt as fee_amount,
    prize_cur as prize_currency,
    prize_amt as prize_amount,
    cur_icon as currency_icon,
    max_players,
    banner_txt as banner_text,
    banner_help_txt as banner_help_text,
    wager_brackets,
    parse_json(prize_pool) as prize_pool,
    flags,
    created_at,
    updated_at,
    xp_multiplier
from game_rooms*/
