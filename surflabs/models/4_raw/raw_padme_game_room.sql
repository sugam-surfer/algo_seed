{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
    	post_hook       = ["truncate table ahsokatano_4_raw.old_padme_game_room"],
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

game_rooms as (
    select
        *,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ source('padme', 'game_room') }}
    {% if is_incremental() -%}
    where updated_at > (select ifnull(max(updated_at),'1900-01-01 00:00:00') from {{ this }})
        and app_id in {{ var_list('games') }}
    {%- endif %}
),

old_data as (
    select
        * , 
    {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ ref('old_padme_game_room') }}	
)

select * from game_rooms
union all
select * from old_data
