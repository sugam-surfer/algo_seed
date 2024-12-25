{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'id',
	post_hook	= ["delete from ahsokatano_4_raw.qa_hansolo_trivia_transactions"],
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with
	
final_date as (
    select 
	max(updated_at) final_date 
    from ahsokatano_4_raw.check_hansolo_trivia_transactions 
	where dest_table_type = 'GOD'   
),

final_data as (
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.qa_hansolo_trivia_transactions    
	union all
    select * exclude (inserted_at) from ahsokatano_4_raw.raw_hansolo_trivia_transactions    
	union all
    select * exclude (inserted_at, issue_type) from ahsokatano_4_raw.error_hansolo_trivia_transactions    
),

output_data_1 as (
    select 
	hansolo_trivia_transactions.*, null as user_id
    from {{ source('hansolo', 'trivia_transactions') }} hansolo_trivia_transactions 
    	left join final_date
	where hansolo_trivia_transactions.updated_at <= final_date.final_date
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
select *, null as user_id from {{ source('hansolo', 'trivia_transactions') }} 
where id in ( select distinct id from ahsokatano_4_raw.qa_hansolo_trivia_transactions )   
)
)
