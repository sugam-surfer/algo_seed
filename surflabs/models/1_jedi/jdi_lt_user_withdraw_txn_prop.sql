{{
    config(
        materialized = 'incremental',
        unique_key   = ['user_id','app_id'],
        cluster_by   = ['user_id','app_id'],
        tags         = ['reseed', 'warehouse'],
    )
}}

select  
	user_id, 
	app_id,
	first_withdraw_ts,
	last_withdraw_ts,
	inserted_at
from (
select 
    user_id, app_id,
	max(first_withdraw_ts) first_withdraw_ts,
	max(last_withdraw_ts) last_withdraw_ts,
	max(inserted_at) inserted_at
from
(
select user_id, app_id, 
	created_at first_withdraw_ts, null last_withdraw_ts, inserted_at
from {{ ref('pdw_lt_user_withdraw_first_5_txn') }} where txn_number = 1 
union
select user_id, app_id, 
	null first_withdraw_ts, created_at last_withdraw_ts, inserted_at
from {{ ref('pdw_lt_user_withdraw_last_txn') }} where txn_number = 1 
) {{ dbt_utils.group_by(2) }}
)
