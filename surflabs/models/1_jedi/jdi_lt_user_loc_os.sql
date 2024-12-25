{{
    config(
        materialized = 'incremental',
        unique_key   = 'user_id',
        cluster_by   = 'user_id',
        tags         = ['reseed', 'events', 'warehouse'],
    )
}}

with

user_last_os_loc as (
	select 
		user_id,
		first_value(os_name) over (partition by user_id order by session_start_timestamp) as os_first,
		first_value(city) over (partition by user_id order by session_start_timestamp) as city_first,
		first_value(country_code) over (partition by user_id order by session_start_timestamp) as country_code_first,
		last_value(os_name) over (partition by user_id order by session_start_timestamp) as os_last,
		last_value(city) over (partition by user_id order by session_start_timestamp) as city_last,
		last_value(country_code) over (partition by user_id order by session_start_timestamp) as country_code_last,
		inserted_at
	from {{ ref('pdw_ae_sessions') }}
	{% if is_incremental() -%}
	where inserted_at > (select dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) from {{ this }})
	{%- endif %}
)

select
	user_id,
	os_first,
	city_first,
	country_code_first,
	os_last,
	city_last,
	country_code_last,
	max(inserted_at) as inserted_at
from user_last_os_loc
{{ dbt_utils.group_by(7) }}
