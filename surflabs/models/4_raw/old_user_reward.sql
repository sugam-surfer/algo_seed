{{
    config(
        materialized	= 'incremental',
        unique_key 	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.qa_user_reward"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_user_reward 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_user_reward    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_user_reward    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_user_reward    
),
    
output_data_1 as (
    select 
	to_char(user_reward.id) id, 
	user_reward.* exclude (id)
    from {{ source('padme', 'user_reward') }} user_reward 
    left join final_date
	where user_reward.updated_at <= final_date.final_date
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
    select 
	to_char(user_reward.id) id, 
	user_reward.* exclude (id)
    from {{ source('padme', 'user_reward') }} user_reward 
    where to_char(user_reward.id) in ( select distinct to_char(id) from ahsokatano_4_raw.qa_user_reward )   
)
)
	
