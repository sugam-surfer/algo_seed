{{
    config(
        materialized    = 'table',
        tags            = 'warehouse',
    )
}}

with

hard_balance as (
    select
        *,
case when left(currency,1) = 'B' and right(currency,len(currency)-1) in {{ var_list('hard_currency_names') }} 
  then right(currency,len(currency)-1) else currency end as currency_match
    from {{ ref('raw_user_hard_balance') }}
),

exchange_rate_1 as (
    select
        target, max(time_slice_index) tsi
    from {{ ref('stg_exchange_rate') }}
        group by 1
),

exchange_rate as (
select a.target, a.rate_close from {{ ref('stg_exchange_rate') }} a
left join exchange_rate_1 b
where a.target = b.target
  and a.time_slice_index = b.tsi
)
    
select
  hard_balance.*, rate_close, ifnull(to_double(balance)/rate_close,to_double(balance)) as ex_value    
from hard_balance
left join exchange_rate
    on hard_balance.currency_match = exchange_rate.target
