{{
    config(
        materialized    = 'incremental',
        unique_key      = 'sdk_replay_id',
        cluster_by      = 'event_ts::date',
        tags            = ['events', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

events as (
    select
        *
    from {{ ref('raw_ae_events') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)

select
    app_id,
    ts event_ts,
    _ts receive_ts,
    sdk_replay_id,
    inserted_at,
    data:user_id::string as user_id,
    data:os::string as os,
    data:app_version::string as app_version,
    data:device::string as device_type,
    parse_json(data:location):city::string as city,
    parse_json(data:location):cc::string as country_code,
    data:name::string as event_name,
    parse_json(data:generic_data:Device):device_token::string as device_token,
    parse_json(data:generic_data:Session):session_id::string as session_id,
    parse_json(data:generic_data:Nft_Data):wallet_addr::string as wallet_addr,
    object_construct(
    'Award', parse_json(data:generic_data:Award),
    'Level', parse_json(data:generic_data:Level), 
    'Session',parse_json(data:generic_data:Session):session_type,
    'Onramp', parse_json(data:generic_data:Custom):onramp_type
    ) as event_data
from events
where data:name::string in {{ var_list('events_list') }}
