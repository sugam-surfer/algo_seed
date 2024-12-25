{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.check_USER_BALANCE_log_new where dest_table_type = 'dest'"],
        tags            = ['reseed', 'warehouse'],
    )
}}

select * exclude (issue_type, dest_table_type) from {{ ref('check_user_balance_log_new') }} where dest_table_type = 'dest'
