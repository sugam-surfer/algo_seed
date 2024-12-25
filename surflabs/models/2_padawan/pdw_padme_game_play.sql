{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags		    = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

game_play_match_null as (
select
        *
    from {{ ref('stg_padme_game_play') }} where match_id is null
),

game_play_match_played as (
select
        *
    from {{ ref('stg_padme_game_play') }}
    {% if is_incremental() -%}
        where match_id in 
        (
        select 
            match_id
        from 
        {{ ref('stg_padme_game_play') }}
        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )
    {%- endif %}
),

game_play as (
select * from game_play_match_null
union all
select * from game_play_match_played
),

tall_game_play as (
select
    * exclude (entry_fee_amount_sc, entry_fee_amount_bonus_hc, entry_fee_amount_hc),
    ifnull(entry_fee_amount_sc,0) as entry_fee_amount_sc,
    ifnull(entry_fee_amount_bonus_hc,0) as entry_fee_amount_bonus_hc,
    ifnull(entry_fee_amount_hc,0) as entry_fee_amount_hc,
    case
        when prize_position_curr in {{ var_list('soft_currency_names') }} then prize_position_amount
        else 0
    end as prize_position_amount_sc,
    case
        when left(prize_position_curr,1) = 'B' and right(prize_position_curr,len(prize_position_curr)-1) in {{ var_list('hard_currency_names') }} then prize_position_amount
        else 0
    end as prize_position_amount_bonus_hc,
    case
        when prize_position_curr in {{ var_list('hard_currency_names') }} then prize_position_amount
        else 0
    end as prize_position_amount_hc,
    case
        when prize_position_curr in {{ var_list('soft_currency_names') }} then prize_position_curr
        else null
    end as prize_position_curr_sc,
    case
        when left(prize_position_curr,1) = 'B' and right(prize_position_curr,len(prize_position_curr)-1) in {{ var_list('hard_currency_names') }} then prize_position_curr
        else null
    end as prize_position_curr_bonus_hc,
    case
        when prize_position_curr in {{ var_list('hard_currency_names') }} then prize_position_curr
        else null
    end as prize_position_curr_hc,
    (ifnull(prize_position_amount_sc,0) - ifnull(entry_fee_amount_sc,0)) as user_delta_amount_sc,
    (ifnull(prize_position_amount_bonus_hc,0) - ifnull(entry_fee_amount_bonus_hc,0)) as user_delta_amount_bonus_hc,
    (ifnull(prize_position_amount_hc,0) - ifnull(entry_fee_amount_hc,0)) as user_delta_amount_hc

from game_play
),

extd_game_play as (
  select tall_game_play.id,
    tall_game_play.match_id, 
    
    entry_fee_curr_sc,
    entry_fee_amount_sc,
    
    entry_fee_curr_bonus_hc,
    entry_fee_amount_bonus_hc,
    ifnull((entry_fee_amount_bonus_hc / stg_exchange_rate_1.rate_close),0) as entry_fee_ex_value_bonus_hc,
    
    entry_fee_curr_hc,
    entry_fee_amount_hc,
    ifnull((entry_fee_amount_hc / stg_exchange_rate_2.rate_close),0) as entry_fee_ex_value_hc,
    
    prize_position_curr_sc,
    prize_position_amount_sc,
    
    prize_position_curr_bonus_hc,
    prize_position_amount_bonus_hc,
    ifnull((prize_position_amount_bonus_hc / stg_exchange_rate_3.rate_close),0) as prize_position_ex_value_bonus_hc,
    
    prize_position_curr_hc,
    prize_position_amount_hc,
    ifnull((prize_position_amount_hc / stg_exchange_rate_4.rate_close),0) as prize_position_ex_value_hc,

    prize_position_amount_sc - entry_fee_amount_sc as user_delta_amount_sc,
    prize_position_ex_value_bonus_hc - entry_fee_ex_value_bonus_hc as user_delta_ex_value_bonus_hc,
    prize_position_ex_value_hc - entry_fee_ex_value_hc as user_delta_ex_value_hc    
    
from tall_game_play
    
left join ahsokatano_4_raw.ref_currency_match ref_currency_match_1
    on tall_game_play.entry_fee_curr_bonus_hc = ref_currency_match_1.currency
left join ahsokatano_4_raw.ref_currency_match ref_currency_match_2
    on tall_game_play.entry_fee_curr_hc = ref_currency_match_2.currency
left join ahsokatano_4_raw.ref_currency_match ref_currency_match_3
    on tall_game_play.prize_position_curr_bonus_hc = ref_currency_match_3.currency
left join ahsokatano_4_raw.ref_currency_match ref_currency_match_4
    on tall_game_play.prize_position_curr_hc = ref_currency_match_4.currency

left join {{ ref('stg_exchange_rate') }} stg_exchange_rate_1
    on stg_exchange_rate_1.ts_start = time_slice(tall_game_play.created_at,10,'minute')
    and stg_exchange_rate_1.target = ref_currency_match_1.currency_match
left join {{ ref('stg_exchange_rate') }} stg_exchange_rate_2
    on stg_exchange_rate_2.ts_start = time_slice(tall_game_play.created_at,10,'minute')
    and stg_exchange_rate_2.target = ref_currency_match_2.currency_match
left join {{ ref('stg_exchange_rate') }} stg_exchange_rate_3
    on stg_exchange_rate_3.ts_start = time_slice(tall_game_play.ended_at,10,'minute')
    and stg_exchange_rate_3.target = ref_currency_match_3.currency_match
left join {{ ref('stg_exchange_rate') }} stg_exchange_rate_4
    on stg_exchange_rate_4.ts_start = time_slice(tall_game_play.ended_at,10,'minute')
    and stg_exchange_rate_4.target = ref_currency_match_4.currency_match
),

match_x_game_play as (
  select 
    match_id, 
    id,
    entry_fee_ex_value_bonus_hc,
    entry_fee_ex_value_hc,
    prize_position_ex_value_bonus_hc,
    prize_position_ex_value_hc,
    user_delta_amount_sc,
    user_delta_ex_value_bonus_hc,
    user_delta_ex_value_hc
  from extd_game_play
),
    
house_cont as (
select 
    match_id,
    sum(-1*user_delta_amount_sc) as house_cont_amount_sc,
    sum(-1*user_delta_ex_value_bonus_hc) as house_cont_ex_value_bonus_hc,
    sum(-1*user_delta_ex_value_hc) as house_cont_ex_value_hc
    from
    extd_game_play
    {{ dbt_utils.group_by(1) }}
),

game_play_count as (
  select match_id, count(id) game_play_count from match_x_game_play {{ dbt_utils.group_by(1) }} 
),

house_cont_pp as (
select 
    house_cont.match_id,
    ifnull((house_cont_amount_sc / game_play_count),0) as house_cont_pp_amount_sc,
    ifnull((house_cont_ex_value_bonus_hc / game_play_count),0) as house_cont_pp_ex_value_bonus_hc,
    ifnull((house_cont_ex_value_hc / game_play_count),0) as house_cont_pp_ex_value_hc
    from
    house_cont left join
    game_play_count
    on house_cont.match_id = game_play_count.match_id
),

final_match_x_game_play as (
    select 
    match_x_game_play.*,
    house_cont_pp.house_cont_pp_amount_sc,
    house_cont_pp.house_cont_pp_ex_value_bonus_hc,
    house_cont_pp.house_cont_pp_ex_value_hc
    from
    match_x_game_play
    left join 
    house_cont_pp 
    on match_x_game_play.match_id = house_cont_pp.match_id
),
    
final_game_play as (
select 
  tall_game_play.*,
  final_match_x_game_play.* exclude (id, match_id, user_delta_amount_sc)
from 
  tall_game_play
  left join
  final_match_x_game_play
  on
  tall_game_play.id = final_match_x_game_play.id
  and tall_game_play.match_id = final_match_x_game_play.match_id
)
  
select 
    * exclude (entry_fee_curr_sc, entry_fee_curr_bonus_hc, entry_fee_curr_hc, entry_fee_amount_sc, entry_fee_amount_bonus_hc, entry_fee_amount_hc, entry_fee_ex_value_bonus_hc, entry_fee_ex_value_hc, PRIZE_POSITION_curr_sc, PRIZE_POSITION_curr_bonus_hc, PRIZE_POSITION_curr_hc, PRIZE_POSITION_amount_sc, PRIZE_POSITION_amount_bonus_hc, PRIZE_POSITION_amount_hc, PRIZE_POSITION_ex_value_bonus_hc, PRIZE_POSITION_ex_value_hc,USER_DELTA_amount_sc, USER_DELTA_amount_bonus_hc, USER_DELTA_amount_hc, USER_DELTA_ex_value_bonus_hc, USER_DELTA_ex_value_hc,     HOUSE_CONT_PP_amount_sc, HOUSE_CONT_PP_ex_value_bonus_hc, HOUSE_CONT_PP_ex_value_hc),
    
    entry_fee_curr_sc, entry_fee_curr_bonus_hc, entry_fee_curr_hc,
    ifnull ( entry_fee_curr_sc , ifnull ( entry_fee_curr_hc , ( right ( entry_fee_curr_bonus_hc , len ( entry_fee_curr_bonus_hc ) - 1 )))) entry_fee_currency_match,
    entry_fee_amount_sc, entry_fee_amount_bonus_hc, entry_fee_amount_hc, entry_fee_ex_value_bonus_hc, entry_fee_ex_value_hc,
    ENTRY_FEE_EX_VALUE_BONUS_HC + ENTRY_FEE_EX_VALUE_HC as ENTRY_FEE_EX_VALUE_all_HC,

    PRIZE_POSITION_curr_sc, PRIZE_POSITION_curr_bonus_hc, PRIZE_POSITION_curr_hc,
    ifnull ( PRIZE_POSITION_curr_sc , ifnull ( PRIZE_POSITION_curr_hc , ( right ( PRIZE_POSITION_curr_bonus_hc , len ( PRIZE_POSITION_curr_bonus_hc ) - 1 )))) PRIZE_POSITION_currency_match,
    PRIZE_POSITION_amount_sc, PRIZE_POSITION_amount_bonus_hc, PRIZE_POSITION_amount_hc, PRIZE_POSITION_ex_value_bonus_hc, PRIZE_POSITION_ex_value_hc,
    PRIZE_POSITION_EX_VALUE_BONUS_HC + PRIZE_POSITION_EX_VALUE_HC as PRIZE_POSITION_EX_VALUE_all_HC,

    USER_DELTA_amount_sc, USER_DELTA_amount_bonus_hc, USER_DELTA_amount_hc, USER_DELTA_ex_value_bonus_hc, USER_DELTA_ex_value_hc,
    user_delta_ex_value_bonus_hc + user_delta_ex_value_hc as user_delta_ex_value_all_hc,

    HOUSE_CONT_PP_amount_sc, HOUSE_CONT_PP_ex_value_bonus_hc, HOUSE_CONT_PP_ex_value_hc,
    HOUSE_CONT_PP_EX_VALUE_BONUS_HC + HOUSE_CONT_PP_EX_VALUE_HC as HOUSE_CONT_PP_EX_VALUE_all_HC,
    
    (user_delta_ex_value_hc + user_delta_ex_value_bonus_hc) > 0 or user_delta_amount_sc > 0 as earning_status
from final_game_play
