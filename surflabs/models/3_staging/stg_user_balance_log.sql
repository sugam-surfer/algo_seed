{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = ['created_at', 'inserted_at', 'user_id', 'id', 'reason_category'],
        tags            = ['trivia', 'reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with raw_user_balance_log as (
    select *
    from {{ ref('raw_user_balance_log') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(timestampadd(hour,-2,inserted_at)),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),

stg_prep as (
    select
        raw_user_balance_log.*,
        'NA' ident_type,
        time_slice(raw_user_balance_log.created_at,10,'minute') as ts_start,
        currency_match,
        reason_category, 
        reason_txn_type
    from raw_user_balance_log 
    left join ahsokatano_4_raw.ref_currency_match a
    on raw_user_balance_log.currency = a.currency
    left join ahsokatano_4_raw.ref_transaction_reason b
    on raw_user_balance_log.reason = b.reason
)

select
    stg_prep.*,
    rate_close,
    round(ifnull(change/rate_close,change),2) as ex_value
from stg_prep
left join {{ ref('stg_exchange_rate') }} on stg_exchange_rate.ts_start = stg_prep.ts_start
    and stg_exchange_rate.target = stg_prep.currency_match
