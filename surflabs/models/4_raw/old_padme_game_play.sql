{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.qa_padme_game_play"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_padme_game_play 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_padme_game_play    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_padme_game_play    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_padme_game_play    
),
    
output_data_1 as (
    select 
	padme_game_play.*
    from {{ source('padme', 'game_play') }} padme_game_play 
    left join final_date
	where padme_game_play.updated_at <= final_date.final_date
),    

output_data as (
    select 
	*
    from output_data_1  
	where app_id in {{ var_list('games') }}
)    


select distinct * from
(	
select * from
(
    select * from output_data 
--	where hour(current_timestamp) in (0,12)
	minus
    select * from final_data
)
union all
(
select * from {{ source('padme', 'game_play') }} 
where id in ( select distinct id from ahsokatano_4_raw.qa_padme_game_play )   
)
)
