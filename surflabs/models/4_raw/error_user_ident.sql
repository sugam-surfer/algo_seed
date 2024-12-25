{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','ident_type'],
        cluster_by	= ['user_id','ident_type'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_ident where dest_table_type = 'error'"],
	tags		= ['error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user_ident') }} where dest_table_type = 'error'
