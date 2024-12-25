{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id', 'ident_value'],
        cluster_by	= ['user_id', 'ident_value'],
	post_hook	= ["delete from ahsokatano_4_raw.qa_user_ident"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_user_ident 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_user_ident    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_user_ident    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_user_ident    
),
    
output_data as (
    select 
	user_ident.*    
    from {{ source('padme', 'user_ident') }} user_ident 
    left join final_date
    	where user_ident.updated_at <= final_date.final_date
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
select * from {{ source('padme', 'user_ident') }} 
where concat(user_id,ident_value) in ( select distinct concat(user_id,ident_value) from ahsokatano_4_raw.qa_user_ident )   
)
)
	
