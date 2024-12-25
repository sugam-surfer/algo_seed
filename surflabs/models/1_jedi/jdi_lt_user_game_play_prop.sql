{{
    config(
        materialized = 'incremental',
        unique_key   = 'user_id',
        cluster_by   = 'user_id',
        tags         = ['reseed', 'warehouse'],
    )
}}

    select 
        user_id,
	max(first_game_ts) first_game_ts,
	max(last_game_ts) last_game_ts,	
	max(sc_first_game_ts) sc_first_game_ts,
	max(sc_last_game_ts) sc_last_game_ts,
	max(hc_first_game_ts) hc_first_game_ts,
	max(hc_last_game_ts) hc_last_game_ts,
	max(inserted_at) inserted_at
from
(
select user_id, 
	created_at first_game_ts, null last_game_ts, null sc_first_game_ts, null sc_last_game_ts, null hc_first_game_ts, null hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_all_first_5_game_play') }} where game_number = 1 
union
select user_id, 
	null first_game_ts, created_at last_game_ts, null sc_first_game_ts, null sc_last_game_ts, null hc_first_game_ts, null hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_all_last_game_play') }} where game_number = 1 
union
select user_id, 
	null first_game_ts, null last_game_ts, created_at sc_first_game_ts, null sc_last_game_ts, null hc_first_game_ts, null hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_type_first_5_game_play') }} where game_number = 1 and game_type in ('soft','both')
union
select user_id, 
	null first_game_ts, null last_game_ts, null sc_first_game_ts, created_at sc_last_game_ts, null hc_first_game_ts, null hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_type_last_game_play') }} where game_number = 1 and game_type in ('soft','both') 
union
select user_id, 
	null first_game_ts, null last_game_ts, null sc_first_game_ts, null sc_last_game_ts, created_at hc_first_game_ts, null hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_type_first_5_game_play') }} where game_number = 1 and game_type in ('hard','both') 
union
select user_id, 
	null first_game_ts, null last_game_ts, null sc_first_game_ts, null sc_last_game_ts, null hc_first_game_ts, created_at hc_last_game_ts, inserted_at
from {{ ref('pdw_lt_user_type_last_game_play') }} where game_number = 1 and game_type in ('hard','both') 
) group by 1
