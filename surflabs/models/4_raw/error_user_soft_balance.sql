{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id','currency'],
        cluster_by	= ['user_id','app_id','currency'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_soft_balance where dest_table_type = 'error'"],
	tags		= ['error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user_soft_balance') }} where dest_table_type = 'error'
