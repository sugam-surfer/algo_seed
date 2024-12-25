{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.check_padme_game_play where dest_table_type = 'error'"],
	tags		= ['error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_padme_game_play') }} where dest_table_type = 'error'
