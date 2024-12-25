{{
    config(
        materialized = 'incremental',
        unique_key   = ['hour_created_at','user_id','room_id','IS_FAKE'],
        cluster_by   = ['hour_created_at','user_id','room_id','IS_FAKE'],
        tags         = ['reseed', 'warehouse'],
    )
}}

with out_put as 
(   
select 
  user_id,
	app_id,
  room_id,
  max_players,
  payout_status,
	GAME_TYPE,
	GAME_STATUS,
	IS_FAKE,
entry_fee_currency_match,
prize_position_currency_match,
	earning_status,
	date_trunc(hour, created_at) hour_created_at,
  count(id) usergames_played,
  sum(playtime_seconds) as playtime_seconds,
  sum(ENTRY_FEE_AMOUNT_SC) as ENTRY_FEE_AMOUNT_SC,
	sum(ENTRY_FEE_AMOUNT_BONUS_HC) as ENTRY_FEE_AMOUNT_BONUS_HC,
	sum(ENTRY_FEE_AMOUNT_HC) as ENTRY_FEE_AMOUNT_HC,
	sum(PRIZE_POSITION_AMOUNT_SC) as PRIZE_POSITION_AMOUNT_SC, 
	sum(PRIZE_POSITION_AMOUNT_BONUS_HC) as PRIZE_POSITION_AMOUNT_BONUS_HC,
	sum(PRIZE_POSITION_AMOUNT_HC) as PRIZE_POSITION_AMOUNT_HC,
	sum(USER_DELTA_AMOUNT_SC) as USER_DELTA_AMOUNT_SC,
	sum(USER_DELTA_AMOUNT_BONUS_HC) as USER_DELTA_AMOUNT_BONUS_HC,
	sum(USER_DELTA_AMOUNT_HC) as USER_DELTA_AMOUNT_HC,
	sum(ENTRY_FEE_EX_VALUE_BONUS_HC) as ENTRY_FEE_EX_VALUE_BONUS_HC,
	sum(ENTRY_FEE_EX_VALUE_HC) as ENTRY_FEE_EX_VALUE_HC,
	sum(ENTRY_FEE_EX_VALUE_all_HC) as ENTRY_FEE_EX_VALUE_all_HC,
	sum(PRIZE_POSITION_EX_VALUE_BONUS_HC) as PRIZE_POSITION_EX_VALUE_BONUS_HC,
	sum(PRIZE_POSITION_EX_VALUE_HC) as PRIZE_POSITION_EX_VALUE_HC,
	sum(PRIZE_POSITION_EX_VALUE_all_HC) as PRIZE_POSITION_EX_VALUE_all_HC,
	sum(USER_DELTA_EX_VALUE_BONUS_HC) as USER_DELTA_EX_VALUE_BONUS_HC,
	sum(USER_DELTA_EX_VALUE_HC) as USER_DELTA_EX_VALUE_HC,
	sum(USER_DELTA_EX_VALUE_all_HC) as USER_DELTA_EX_VALUE_all_HC,
	sum(HOUSE_CONT_PP_AMOUNT_SC) as HOUSE_CONT_PP_AMOUNT_SC,
	sum(HOUSE_CONT_PP_EX_VALUE_BONUS_HC) as HOUSE_CONT_PP_EX_VALUE_BONUS_HC,
	sum(HOUSE_CONT_PP_EX_VALUE_HC) as HOUSE_CONT_PP_EX_VALUE_HC,
	sum(HOUSE_CONT_PP_EX_VALUE_all_HC) as HOUSE_CONT_PP_EX_VALUE_all_HC,
  max(inserted_at) inserted_at
    from
    {{ ref('pdw_padme_game_play') }}

    {% if is_incremental() %}
        where hour_created_at in 
        (
        select 
            date_trunc(hour, created_at) 
        from 
        {{ ref('pdw_padme_game_play') }}
        where inserted_at >= (
                            select 
                                dateadd(day,-1,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )
    {% endif %}

    {{ dbt_utils.group_by(12) }}
)

select 
    * 
from 
out_put
where 
hour_created_at is not null
