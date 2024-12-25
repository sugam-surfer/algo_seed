
{{
    config(
        materialized	= 'incremental',
        unique_key	= ['user_id','app_id','currency'],
        cluster_by	= ['user_id','app_id','currency'],
	post_hook	= ["truncate table ahsokatano_4_raw.old_user_hard_balance"],
	tags		= ['reseed', 'check', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

source_data_2 as (
    select
        *, 
    from {{ source('padme', 'user_hard_balance') }}
        {% if is_incremental() -%}
    where updated_at > (select ifnull(max(updated_at),'1900-01-01 00:00:00') from {{ this }})
        {%- endif %}
),

source_data_1 as (
    select
        user_hard_balance.* exclude (app_id), 
	case when (user_hard_balance.app_id is null or user_hard_balance.app_id = '') then app.app_id else user_hard_balance.app_id end as app_id,
	{{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at,
	'NA' surr_key 
    from source_data_2 user_hard_balance
    left join {{ source('padme', 'user') }} usr on usr.id = user_hard_balance.user_id
    left join {{ ref('ref_apps') }} app on app.company_id = usr.company_id
),

source_data as (
    select 
	* 
    from source_data_1 
	where app_id in {{ var_list('games') }}
),
	
old_data as (
    select
        old_user_hard_balance.* exclude (app_id), 
	case when (old_user_hard_balance.app_id is null or old_user_hard_balance.app_id = '') then app.app_id else old_user_hard_balance.app_id end as app_id,
	{{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at,
        'NA' surr_key 
    from {{ ref('old_user_hard_balance') }} old_user_hard_balance	
    left join {{ source('padme', 'user') }} usr on usr.id = old_user_hard_balance.user_id
    left join {{ ref('ref_apps') }} app on app.company_id = usr.company_id
),

source_inc_data as (
select distinct * from (
    select * from source_data
	union all
    select * from old_data
)),

pre_exist_label as (
    SELECT 
    	sic.*,
    	CASE
        	WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.error_user_hard_balance d1 WHERE sic.user_id = d1.user_id and sic.app_id = d1.app_id and sic.currency = d1.currency) THEN 'error'
        	WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.raw_user_hard_balance d2 WHERE sic.user_id = d2.user_id and sic.app_id = d2.app_id and sic.currency = d2.currency) THEN 'dest'
        	WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.qa_user_hard_balance d3 WHERE sic.user_id = d3.user_id and sic.app_id = d3.app_id and sic.currency = d3.currency) THEN 'qa'
        	ELSE NULL
    	END AS dest_table_type
    FROM source_inc_data sic
),


pre_exist_final_error as (
    select 
	m.* exclude (dest_table_type), 
	n.issue_type as issue_type,
	m.dest_table_type
    from pre_exist_label m
    left join ahsokatano_4_raw.error_user_hard_balance n
    on m.user_id = n.user_id
    and m.app_id = n.app_id
    and m.currency = n.currency
	where dest_table_type = 'error'
),

pre_exist_final_dest as (
    select 
	m.* exclude (dest_table_type), 
	null as issue_type,
	m.dest_table_type
    from pre_exist_label m
	where dest_table_type = 'dest'
),

pre_exist_final_qa as (
    select 
	m.* exclude (dest_table_type), 
	n.issue_type as issue_type,
	m.dest_table_type
    from pre_exist_label m
    left join ahsokatano_4_raw.qa_user_hard_balance n
    on m.user_id = n.user_id
    and m.app_id = n.app_id
    and m.currency = n.currency
	where dest_table_type = 'qa'
),

non_pre_exist as (
    select 
	* exclude (dest_table_type) 
    from pre_exist_label 
	where dest_table_type is null
),

error_handling as (
    SELECT 
	main.*,
	CASE
		WHEN main.user_id NOT IN (SELECT id FROM {{ ref('raw_user') }}) THEN 11
		when x.id is null then 12
        	WHEN (
            		SELECT COUNT(*) 
            		FROM non_pre_exist 	
            			WHERE user_id = main.user_id
				and app_id = main.app_id
				AND currency = main.currency
        	) > 1 THEN 21
        	WHEN EXISTS (
            		SELECT 1 
            		FROM ahsokatano_4_raw.raw_user_hard_balance 
            			WHERE user_id = main.user_id 
            			AND 'NA' <> main.surr_key
        	) THEN 31
        	ELSE 0
    	END AS issue_number
    FROM non_pre_exist main
    left join
    {{ ref('raw_user') }} x
    ON main.user_id = x.id 
    AND main.app_id = x.app_id
),

non_pre_exist_final as (
    select 
	* exclude (issue_number), 
	case issue_number 
		when 0 then 'OK'
		when 11 then 'user_id flags column issue'
		when 12 then 'user_id app_id NA in raw_user'
		when 21 then 'DUPLICATE in source'
		when 31 then 'SURROGATE key difference'
	end as issue_type,
	case issue_number 
		when 0 then 'dest'
		when 11 then 'qa'
		when 12 then 'qa'
		when 21 then 'qa'
		when 31 then 'qa'
	end as dest_table_type
    from error_handling
),

final as (
    select * from pre_exist_final_error
	union all
    select * from pre_exist_final_dest
	union all
    select * from pre_exist_final_qa
	union all
    select * from non_pre_exist_final
),

god_record as (
    select
	'GOD' user_id,
	'GOD' currency,
	balance, created_at, updated_at,
	'GOD' app_id,
	* exclude (user_id, currency, balance, created_at, updated_at, app_id, issue_type, dest_table_type),
	'GOD' issue_type,
	'GOD' dest_table_type
    from final
	order by updated_at desc limit 1
)

select * exclude (surr_key) from final	
	union all
select * exclude (surr_key) from god_record
