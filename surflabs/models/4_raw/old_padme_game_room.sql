{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'id',
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
select max(updated_at) final_date from ahsokatano_4_raw.raw_padme_game_room   
),

final_data as (
select * exclude (inserted_at) from ahsokatano_4_raw.raw_padme_game_room    
),
    
output_data as (
    select 
    game_room.*
	from {{ source('padme', 'game_room') }} game_room 
    left join final_date
    where game_room.updated_at <= final_date.final_date
    and app_id in {{ var_list('games') }}
)    

select * from output_data
--	where hour(current_timestamp) in (0,12)
minus
select * from final_data
