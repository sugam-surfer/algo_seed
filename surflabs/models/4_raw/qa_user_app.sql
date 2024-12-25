{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id'],
        cluster_by	= ['user_id','app_id'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_app where dest_table_type = 'qa'"],
	tags		= ['reseed', 'qa', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user_app') }} where dest_table_type = 'qa'
