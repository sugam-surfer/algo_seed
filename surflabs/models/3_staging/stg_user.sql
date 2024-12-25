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

user as (
    select
        raw_user.* exclude (attribution),
        concat('[',ifnull(parse_json(attribution):af_status::string, 'Undefined'),']') as attr_af_status,
        concat('[',ifnull(parse_json(attribution):media_source::string,''),']') as attr_media_source,
        concat('[',ifnull(parse_json(attribution):af_channel::string,''),']') as attr_af_channel,
        concat('[',ifnull(parse_json(attribution):campaign::string,''),']') as attr_campaign_name,
        concat('[',ifnull(parse_json(attribution):campaign_id::string,''),']') as attr_campaign_id,
        concat('[',ifnull(parse_json(attribution):af_adset_id::string,''),']') as attr_af_adset_id,
        parse_json(attribution):install_time::datetime as attr_install_time
    from {{ ref('raw_user') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)

select
    *
from user
