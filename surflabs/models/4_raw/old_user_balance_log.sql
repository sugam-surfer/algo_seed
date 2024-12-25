{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["delete from ahsokatano_4_raw.qa_user_balance_log"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
    select 
	max(created_at) final_date 
    from ahsokatano_4_raw.check_user_balance_log 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_user_balance_log    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_user_balance_log    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_user_balance_log    
),

output_data_1 as (
    select 
	USER_BALANCE_LOG.* exclude (app_id),
	case when (USER_BALANCE_LOG.app_id is null or USER_BALANCE_LOG.app_id = '') then app.app_id else USER_BALANCE_LOG.app_id end as app_id
    from {{ source('padme', 'user_balance_log') }} USER_BALANCE_LOG 
    left join final_date
    left join {{ source('padme', 'user') }} usr on usr.id = USER_BALANCE_LOG.user_id
    left join {{ ref('ref_apps') }} app on app.company_id = usr.company_id
	where USER_BALANCE_LOG.created_at <= final_date.final_date
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
	USER_BALANCE_LOG.* exclude (app_id),
	case when (USER_BALANCE_LOG.app_id is null or USER_BALANCE_LOG.app_id = '') then app.app_id else USER_BALANCE_LOG.app_id end as app_id
    from {{ source('padme', 'user_balance_log') }} USER_BALANCE_LOG 
    left join {{ source('padme', 'user') }} usr on usr.id = USER_BALANCE_LOG.user_id
    left join {{ ref('ref_apps') }} app on app.company_id = usr.company_id

where USER_BALANCE_LOG.id in ( select distinct id from ahsokatano_4_raw.qa_user_balance_log )   
)
)
	
