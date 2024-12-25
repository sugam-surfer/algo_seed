{{
    config(
        materialized    = 'incremental',
        unique_key      = ['report_date', 'ident_type'],
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

wallets as (
    select
        *
    from {{ ref('raw_web3auth_wallet') }}
    {% if is_incremental() -%}
    where created_at > dateadd(days, -3, current_date())
    {%- endif %}
)

select
    created_at::date as report_date,
    ident_type,
    count(distinct ident_value) as new_wallets,
    sum(init) as funded_wallets
from wallets
{{ dbt_utils.group_by(2) }}
