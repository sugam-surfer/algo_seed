{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags            = ['sch_4hrs', 'sch_daily', 'crypto_unicorns', 'reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

withdrawals as (
    select
        *
    from {{ source('skywalker', 'user_token_escrow') }}
    {% if is_incremental() -%}
    where updated_at > (select max(updated_at) from {{ this }})
    {%- endif %}
)

select
    *
from withdrawals
