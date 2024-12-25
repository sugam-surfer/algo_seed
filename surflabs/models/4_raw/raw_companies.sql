{{
    config(
        materialized    = 'table',
        tags            = 'warehouse',
    )
}}

with

company as (
    select
        *
    from {{ source('hansolo', 'company') }}
)

select
    id as company_id,
    name as company_name,
    dev_api_key as company_dev_api_key,
    created_at as company_created_at,
    updated_at as company_updated_at
from company
