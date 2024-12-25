{{
    config(
        materialized = 'table',
        tags = 'unknown',
    )
}}

with

app as (
    select
        *
    from {{ ref('raw_apps') }}
),

company as (
    select
        *
    from {{ ref('raw_companies') }}
)

select
    app.app_id,
    app.app_name,
    app.app_description,
    app.company_id,
    company.company_name,
    app.app_ios_id,
    app.app_android_id,
    app.app_sdk_api_key,
    app.app_created_at,
    app.app_updated_at,
    company.company_created_at,
    company.company_updated_at
from app
inner join company
    on app.company_id = company.company_id
