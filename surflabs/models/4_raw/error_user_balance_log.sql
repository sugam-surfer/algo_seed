{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.check_user_balance_log where dest_table_type = 'error'"],
	tags		= ['error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user_balance_log') }} where dest_table_type = 'error'
