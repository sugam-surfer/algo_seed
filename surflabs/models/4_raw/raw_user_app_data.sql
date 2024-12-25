{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id'],
        cluster_by	= ['user_id','app_id'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_app_data where dest_table_type = 'dest'"],
        tags            = ['reseed', 'warehouse'],
    )
}}

select * exclude (issue_type, dest_table_type) from {{ ref('check_user_app_data') }} where dest_table_type = 'dest'
