{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags            = ['on_chain', 'trivia', 'reseed', 'warehouse'],
    )
}}

with

hansolo_trivia_transactions_1 as (
    select
        *
    from {{ ref('raw_hansolo_trivia_transactions') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),

hansolo_trivia_transactions as (
    select
        *
    from hansolo_trivia_transactions_1
    where type = 'deposit'
)
    
/*
,

user_ident as (
select distinct user_id, ident_value from
(
select user_id, ident_value from {{ ref('miss_user_ident')}}
union all
select user_id, ident_value from {{ ref('raw_user_ident')}}
))
*/
    
select
    hansolo_trivia_transactions.* exclude user_id,
    case
        when type = 'deposit' and status = 0 then 'not processed'
        when type = 'deposit' and status = 2 then 'rejected'
        when type = 'deposit' and status = 3 then 'processed'
        when type = 'withdraw' and status = 0 then 'requested'
        when type = 'withdraw' and status = 1 then 'approved'
        when type = 'withdraw' and status = 2 then 'rejected'
        when type = 'withdraw' and status = 3 then 'processed'
        when type = 'withdraw' and status = 4 then 'paypal pending'
        when type = 'withdraw' and status = 6 then 'confiscate'
        else 'unknown'
    end as transaction_status,
    case
        when type = 'withdraw' then hansolo_trivia_transactions.decided_at
        when type = 'deposit' then hansolo_trivia_transactions.updated_at
    end as transaction_ts,
    time_slice(hansolo_trivia_transactions.updated_at,10,'minute') as ts_start,
    rate_close,
    ifnull(amount/rate_close,withdraw_fee) as ex_value,
    user_id
from hansolo_trivia_transactions
left join ahsokatano_4_raw.ref_currency_match ref_currency_match
    on hansolo_trivia_transactions.currency = ref_currency_match.currency
left join {{ ref('stg_exchange_rate') }} 
    on stg_exchange_rate.ts_start = time_slice( case
        when type = 'withdraw' then ifnull(hansolo_trivia_transactions.decided_at, to_timestamp_ntz(current_timestamp) )
        when type = 'deposit' then ifnull(hansolo_trivia_transactions.updated_at, to_timestamp_ntz(current_timestamp) )
    end ,10,'minute')
    and stg_exchange_rate.target = ref_currency_match.currency_match
where transaction_status = 'processed'

/*
left join
    user_ident
on hansolo_trivia_transactions.ident_value = user_ident.ident_value
*/
