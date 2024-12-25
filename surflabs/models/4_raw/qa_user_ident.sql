{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','ident_value'],
        cluster_by	= ['user_id','ident_value'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_ident where dest_table_type = 'qa'"],
	tags		= ['reseed', 'qa', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_user_ident') }} where dest_table_type = 'qa'
