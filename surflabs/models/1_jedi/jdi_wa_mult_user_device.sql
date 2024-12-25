{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

with

q_user as (
    select distinct
        stg_ae_events.app_id as app_id,
        user_id,
        device_token,
        event_ts
    from ahsokatano_3_staging.stg_ae_events
    where event_name = 'Onramp' and device_token is not null
),

mult_user_ids as (
    select
        a.app_id,
        a.user_id,
        a.event_ts as user_event_ts,
        b.user_id as other_user_id,
        b.event_ts as other_user_event_ts
    from q_user a
    left join q_user b on a.device_token = b.device_token
        and a.app_id = b.app_id
        and a.user_id != b.user_id
),

qualified_games as (
    select
        app_id,
        user_id,
        day_created_at
    from {{ ref('jdi_day_padme_game_play') }}
    where
        game_status = '04. finished' and
        game_type = 'hard' and
        is_fake = 'normal'
),

qualified_users as (
    select
        g.app_id,
        m.user_id,
        m.other_user_id,
        g.day_created_at as user_game_date,
        g2.day_created_at AS other_user_game_date
    from qualified_games g
    join mult_user_ids m on g.user_id = m.user_id
    join qualified_games g2 ON g2.user_id = m.other_user_id
    where abs(datediff(day,g.day_created_at, g2.day_created_at)) <= {{ var('max_days_between_game_plays_mult_user_device') }}
)
select
    q.app_id,
    q.user_id,
    count(distinct q.other_user_id) as num_other_user_ids
from qualified_users q
{{ dbt_utils.group_by(2) }}
