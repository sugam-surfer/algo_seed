{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'id',
	post_hook	= ["delete from ahsokatano_4_raw.qa_user"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_user 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_user    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_user    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_user    
),

output_data_1 as (
    select 
	user.*,
	apps.app_id
    from {{ source('padme', 'user') }} user 
    left join final_date
    left join {{ ref('ref_apps') }} apps on user.company_id = apps.company_id
	where user.updated_at <= final_date.final_date
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
	user.*,
	apps.app_id
    from {{ source('padme', 'user') }} user 
    left join {{ ref('ref_apps') }} apps on user.company_id = apps.company_id
	
where user.id in ( select distinct id from ahsokatano_4_raw.qa_user )   
)
)
	


