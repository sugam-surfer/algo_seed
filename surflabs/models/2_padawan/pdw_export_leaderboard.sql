{{
    config(
        materialized    = 'incremental',
        unique_key      = 'game_play_id',
        cluster_by      = 'game_start_time::date',
        tags            = ['reseed', 'events', 'leaderboard', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

last_user_record as (
    select
        user_id,
        country_code,
        device_type,
        event_ts,
        rank() over(partition by user_id order by event_ts desc) as rank_ts
    from {{ ref('stg_ae_events') }}
    {{ dbt_utils.group_by(4) }}
),

game_details as (
    select
        event_data:Award:award_match_id::string as game_play_id,
        try_parse_json(event_data:Award:award_breakdown::string) as game_details_data,
        inserted_at
    from {{ ref('stg_ae_events') }}
    where event_name = 'Award' and event_data:Award:award_name::string = 'score'
)

select
    stg_padme_game_play_leaderboard.app_id,
    stg_padme_game_play_leaderboard.id as game_play_id,
    stg_padme_game_play_leaderboard.user_id as user_id, -- internal, not shared in the API
    user_ident.ident_value as social_wallet_addr,
    case
        when user_ident.ident_type like 'web3%' then right(user_ident.ident_type, len(user_ident.ident_type)-charindex('_',user_ident.ident_type))
        else user_ident.ident_type
    end as chain_type,
    last_user_record.country_code,
    last_user_record.device_type,
    user_ident.friendly_name as social_account,
    parse_json(user_app_data.data):n::string as username,
    object_construct(
        'wallets',
        array_construct(
            object_construct(
                'address',
                parse_json(parse_json(data):d):laguna_leaderboard:lb_wallet[0]:address::string,
                'time',
                to_char(
                    try_to_timestamp(
                        parse_json(parse_json(data):d):laguna_leaderboard:lb_wallet[0]:time::string,
                        'DD/MM/YYYY HH12:MI:SS AM'
                    ),
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                )
            )
        )
    ) AS wallets,
    user.created_at as install_date,
    game_room_name as match_name,
    game_type as match_type,
    stg_padme_game_play_leaderboard.max_players as num_players,
    stg_padme_game_play_leaderboard.position as placement,
    ifnull(ifnull(entry_fee_curr_hc, right(entry_fee_curr_bonus_hc,len(entry_fee_curr_bonus_hc)-1)),'soft') as currency_type,
    case
        when match_type = 'soft' then cast(entry_fee_amount_sc as number(20,10))
        else ifnull(cast(entry_fee_amount_bonus_hc as number(20,10)),0) + ifnull(cast(entry_fee_amount_hc as number(20,10)),0)
    end as entry_fee,
    ifnull(parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[placement]::number(20,10),0) as reward,
    case
        when array_size(parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards) = 1
            then parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[0]::float
        when array_size(parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards) = 2
            then parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[0]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[1]::float
        when array_size(parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards) = 3
            then parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[0]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[1]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[2]::float
        when array_size(parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards) = 4
            then parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[0]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[1]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[2]::float + parse_json(stg_padme_game_play_leaderboard.prize_pool):rewards[3]::float
    end as prize_pool_calc,
    cast((entry_fee*num_players-prize_pool_calc)/num_players as number(20,10)) as rake,
    match_id,
    score as final_score,
    game_details.game_details_data,
    stg_padme_game_play_leaderboard.created_at as game_start_time,
    ended_at as game_end_time,
    stg_padme_game_play_leaderboard.inserted_at -- internal, not shared in the API
from {{ ref('stg_padme_game_play_leaderboard') }}
left join {{ ref('stg_combined_game_room_leaderboard') }}
    on stg_combined_game_room_leaderboard.game_room_id = stg_padme_game_play_leaderboard.room_id
left join {{ source('padme', 'user_app_data') }}
    on user_app_data.user_id = stg_padme_game_play_leaderboard.user_id
left join {{ source('padme', 'user') }}
    on user.id = stg_padme_game_play_leaderboard.user_id
left join {{ source('padme', 'user_ident') }}
    on user_ident.user_id = stg_padme_game_play_leaderboard.user_id
left join last_user_record
    on last_user_record.user_id = stg_padme_game_play_leaderboard.user_id
left join game_details
    on game_details.game_play_id = stg_padme_game_play_leaderboard.id
where
    stg_padme_game_play_leaderboard.created_at >= '2024-04-20'
    and game_status like '4%'
    and last_user_record.rank_ts = 1
    {% if is_incremental() -%}
    and (stg_padme_game_play_leaderboard.inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }}) or
        game_details.inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})) -- two conditions for incremental logic: updated gameplay or new data from the client
    {%- endif %}
