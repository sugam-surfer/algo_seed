{{
    config(
        materialized  = 'incremental',
        unique_key    = 'id',
        cluster_by    = 'created_at::date',
        tags          = ['on_chain', 'trivia', 'reseed', 'warehouse'],
    )
}}

with

web3auth_wallets as (
    select
        *,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ source('hansolo', 'web3auth_wallets') }}
    {% if is_incremental() -%}
    where updated_at > (select max(updated_at) from {{ this }})
    {%- endif %}
)

select
    *
from web3auth_wallets
