{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags            = ['reseed', 'leaderboard', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

game_play as (
select
        * exclude (GAME_ROOM_NAME, GAME_ROOM_FLAGS, GAME_ROOM_ORDER, GAME_ROOM_TYP)
    from {{ ref('raw_padme_game_play') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)

select
    game_play.* exclude (match_id, entry_fee, original_entry_fee),
    right(game_play.match_id, length(game_play.match_id) - position('_', game_play.match_id)) as match_id,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_sc,
    case
        when left(parse_json(entry_fee)[0]:currency::string,1) = 'B' and right(parse_json(entry_fee)[0]:currency::string,len(parse_json(entry_fee)[0]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when left(parse_json(entry_fee)[1]:currency::string,1) = 'B' and right(parse_json(entry_fee)[1]:currency::string,len(parse_json(entry_fee)[1]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when left(parse_json(entry_fee)[2]:currency::string,1) = 'B' and right(parse_json(entry_fee)[2]:currency::string,len(parse_json(entry_fee)[2]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_bonus_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_sc,
    case
        when left(parse_json(entry_fee)[0]:currency::string,1) = 'B' and right(parse_json(entry_fee)[0]:currency::string,len(parse_json(entry_fee)[0]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when left(parse_json(entry_fee)[1]:currency::string,1) = 'B' and right(parse_json(entry_fee)[1]:currency::string,len(parse_json(entry_fee)[1]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when left(parse_json(entry_fee)[2]:currency::string,1) = 'B' and right(parse_json(entry_fee)[2]:currency::string,len(parse_json(entry_fee)[2]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_bonus_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_hc,
    parse_json(original_entry_fee):currency::string as or_entry_fee_curr,
    parse_json(original_entry_fee):amount::float as or_entry_fee_amount,
    case
        when (entry_fee_curr_sc is not null) then 'soft'
        when ((entry_fee_curr_hc is not null) or (entry_fee_curr_bonus_hc is not null)) then 'hard'
        when ((entry_fee_curr_sc is not null) and ((entry_fee_curr_hc is not null) or (entry_fee_curr_bonus_hc is not null))) then 'both'
        else 'unknown'
    end as game_type,
    case
        when bitand(flags,1)!=0 then '1. started'
        when bitand(flags,2)!=0 then '2. cancelled'
        when bitand(flags,4)!=0 then '3. pending'
        when bitand(flags,8)!=0 then '4. finished'
        when bitand(flags,16)!=0 then '5. expired'
        else '9. unknown'
    end as game_status
from game_play
