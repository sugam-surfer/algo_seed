{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','ident_value'],
        cluster_by	= ['user_id','ident_value'],
	post_hook	= ["delete from ahsokatano_4_raw.check_user_ident where dest_table_type = 'dest'"],
        tags            = ['reseed', 'warehouse'],
    )
}}

select * exclude (issue_type, dest_table_type) from {{ ref('check_user_ident') }} where dest_table_type = 'dest'
