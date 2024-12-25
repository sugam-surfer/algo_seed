{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'id',
	post_hook	= ["delete from ahsokatano_4_raw.check_user where dest_table_type = 'error'"],
	tags		= ['error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user') }} where dest_table_type = 'error'
