{{
    config(
        materialized 	= 'incremental',
        unique_key 	= 'id',
        cluster_by	= 'created_at::date',
	post_hook	= ["truncate table ahsokatano_4_raw.old_padme_game_play"],
	tags		= ['reseed', 'check', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

source_data_2 as (
    select
        *  
    from {{ source('padme', 'game_play') }}
        {% if is_incremental() -%}
    where updated_at > (select ifnull(max(updated_at),'1900-01-01 00:00:00') from {{ this }})
        {%- endif %}
),

source_data_1 as (
    select
        * , 
	{{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at,
        'NA' surr_key 
    from source_data_2
),

source_data as (
    select 
	* 
    from source_data_1 
	where app_id in {{ var_list('games') }}	
),

old_data as (
    select
        * , 
	{{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at,
        'NA' surr_key 
    from {{ ref('old_padme_game_play') }}	
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
		WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.error_padme_game_play d1 WHERE sic.id = d1.id) THEN 'error'
		WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.raw_padme_game_play d2 WHERE sic.id = d2.id) THEN 'dest'
		WHEN EXISTS (SELECT 1 FROM ahsokatano_4_raw.qa_padme_game_play d3 WHERE sic.id = d3.id) THEN 'qa'
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
    left join ahsokatano_4_raw.error_padme_game_play n
    on m.id = n.id
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
    left join ahsokatano_4_raw.qa_padme_game_play n
    on m.id = n.id
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
            			WHERE id = main.id
		) > 1 THEN 21
		WHEN EXISTS (
			SELECT 1 
			FROM ahsokatano_4_raw.raw_padme_game_play 
				WHERE id = main.id 
				AND 'NA' <> main.surr_key
		) THEN 31
		WHEN main.original_entry_fee IS NOT NULL AND parse_json(main.original_prize_pool):currency::string != parse_json(rpr.prize_pool):currency::string THEN 41
        WHEN main.original_entry_fee IS NOT NULL AND parse_json(main.original_entry_fee):currency::string != parse_json(rpr.entry_fee):currency::string THEN 42
		ELSE 0
	END AS issue_number
    FROM 
	non_pre_exist main
	left join {{ ref('raw_user') }} x
	ON main.user_id = x.id 
	AND main.app_id = x.app_id
	left join surf.ahsokatano_4_raw.raw_padme_game_room rpr
    on main.room_id = rpr.id
),

non_pre_exist_final as (
    select 
	* exclude issue_number, 
	case issue_number 
		when 0 then 'OK'
		when 11 then 'user_id flags column issue'
		when 12 then 'user_id app_id NA in raw_user'
		when 21 then 'DUPLICATE in source'
		when 31 then 'SURROGATE key difference'
		when 41 then 'CURRENCY discrepancy in prize'
        when 42 then 'CURRENCY discrepancy in entry fee'
	end as issue_type,
	case issue_number 
		when 0 then 'dest'
		when 11 then 'qa'
		when 12 then 'qa'
		when 21 then 'qa'
		when 31 then 'qa'
		when 41 then 'qa'
        when 42 then 'qa'
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
	'GOD' id,
	* exclude (id, issue_type, dest_table_type),
	'GOD' issue_type,
	'GOD' dest_table_type
    from final
	order by updated_at desc limit 1
)

select * exclude (surr_key) from final	
	union all
select * exclude (surr_key) from god_record
