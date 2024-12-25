{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

trivia_match as (
    select
        *,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ source('skywalker', 'trivia_match') }}
    {% if is_incremental() -%}
    where updated_at > (select ifnull(max(updated_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)

select
    *,
    concat(app_id,'_',to_char(id)) as match_id
from trivia_match
