{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id', 'app_id'],
        cluster_by	= ['user_id', 'app_id'],
	post_hook	= ["delete from ahsokatano_4_raw.qa_user_app_data"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_user_app_data 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_user_app_data    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_user_app_data    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_user_app_data    
),
    
output_data_1 as (
    select 
	user_app_data.*
    from {{ source('padme', 'user_app_data') }} user_app_data 
	left join final_date
	where user_app_data.updated_at <= final_date.final_date
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
select * from {{ source('padme', 'user_app_data') }} 
where concat(user_id,app_id) in ( select distinct concat(user_id,app_id) from ahsokatano_4_raw.qa_user_app_data )   
)
)
	
