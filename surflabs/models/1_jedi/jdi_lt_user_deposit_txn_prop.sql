{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id'],
        cluster_by	= ['user_id','app_id'],
        tags		= ['reseed', 'warehouse'],
    )
}}

select  
	user_id, 
	app_id,
	first_deposit_ts,
	last_deposit_ts,
	inserted_at
from (
select 
    user_id, app_id,
	max(first_deposit_ts) first_deposit_ts,
	max(last_deposit_ts) last_deposit_ts,
	max(inserted_at) inserted_at
from
(
select user_id, app_id, 
	created_at first_deposit_ts, null last_deposit_ts, inserted_at
from {{ ref('pdw_lt_user_deposit_first_5_txn') }} where txn_number = 1 
union
select user_id, app_id, 
	null first_deposit_ts, created_at last_deposit_ts, inserted_at
from {{ ref('pdw_lt_user_deposit_last_txn') }} where txn_number = 1 
) {{ dbt_utils.group_by(2) }}
)
