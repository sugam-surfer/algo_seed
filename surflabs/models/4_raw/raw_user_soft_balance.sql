
{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id','currency'],
        cluster_by	= ['user_id','app_id','currency'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_soft_balance where dest_table_type = 'dest'"],
        tags            = ['reseed', 'warehouse'],
    )
}}

select 
	USER_ID,
	APP_ID,
	to_double(BALANCE) as balance,
	CREATED_AT,
	UPDATED_AT,
	currency,
	INSERTED_AT
from {{ ref('check_user_soft_balance') }} where dest_table_type = 'dest'
