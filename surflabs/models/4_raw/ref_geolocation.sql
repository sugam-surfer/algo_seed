{{
    config(
        materialized    = 'incremental',
        unique_key      = ['country_code', 'city'],
        cluster_by      = ['continent', 'country_code'],
        tags            = ['ref', 'events', 'reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

select distinct
    data:location:ct::string as continent,
    data:location:tz::string as timezone,
    data:location:cc::string as country_code,
    data:location:sub[0]::string as subdivision1,
    data:location:sub[1]::string as subdivision2,
    data:location:city::string as city,
    {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
from {{ ref('raw_ae_events') }}
{% if is_incremental() -%}
where inserted_at >= (select dateadd(day,-1,ifnull(max(inserted_at),'1900-01-01 00:00:00')) from {{ this }})
{%- endif %}
having data:location:cc::string != ''
