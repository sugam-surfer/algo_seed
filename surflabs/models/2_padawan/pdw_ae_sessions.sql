{{
    config(
        materialized	= 'incremental',
        unique_key	= 'session_id',
	tags		= ['events', 'warehouse'],
	)
}}

{{ incremental_message() }}

with

events as (
    select
        *
    from {{ ref('stg_ae_events') }}
    {% if is_incremental() -%}

        where session_id in 
        (
        select 
            session_id
        from 
        {{ ref('stg_ae_events') }}
        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )
	{%- endif %}
),

num_geoloc as (
    select
        session_id,
        count(distinct city) as num_distinct_loc
    from events
    {{ dbt_utils.group_by(1) }}
)

select
    events.session_id,
    user_id,
    app_id,
    app_version,
    device_token,
    wallet_addr,
    {{ os_cleaned('ae') }} as os_name,
    case
        when num_distinct_loc > 1 then 'Unknown'
        else city
    end as city,
    case
        when num_distinct_loc > 1 then 'Unknown'
        else country_code
    end as country_code,
    min(event_ts) as session_start_timestamp,
    max(event_ts) as session_end_timestamp,
    timestampdiff(second,min(event_ts),max(event_ts)) as session_duration,
    count(distinct case when event_name = 'Wallet_Connected' then sdk_replay_id end) as logins,
    max(inserted_at) as inserted_at
    -- Need to change to get the actual xp
	-- count(distinct case when event_name = 'level - end' then 0 end) as games_completed,
    -- Need to change to get the actual xp
    -- sum(case when event_name = 'level - end' then 0 end) as total_xp,
    -- Need to change to get the actual score
    -- sum(case when event_name = 'level - end' then 0 end) as total_score,
from events
left join num_geoloc on num_geoloc.session_id = events.session_id
{{ dbt_utils.group_by(9) }}
