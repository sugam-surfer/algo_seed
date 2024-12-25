{{
    config(
        materialized    = 'table',
        tags            = 'warehouse',
    )
}}

with

app as (
    select
        *
    from {{ source('hansolo', 'app') }}
)

select
    id as app_id,
    name as app_name,
    description as app_description,
    company_id,
    ios_id as app_ios_id,
    android_id as app_android_id,
    sdk_api_key as app_sdk_api_key,
    created_at as app_created_at,
    updated_at as app_updated_at,
    flags as app_flags,
    hard_currencies as app_hard_currencies
from app
