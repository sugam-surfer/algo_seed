{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}
 
with distinct_success_deposit_idents as (
    select distinct ident_value
    from {{ ref('stg_hansolo_trivia_transactions') }}
    where type = 'deposit' and status = 3
),
    
filtered_transactions as (
    select
        app_id,
        ident_value,
        type,
        sum(ex_value) as total_val
    from {{ ref('stg_hansolo_trivia_transactions') }}
    where status = 3
    group by app_id, ident_value, type
),
   
transactions as (
    select
        ft.app_id,
        ft.ident_value,
        ft.type,
        ft.total_val
    from filtered_transactions ft
    join distinct_success_deposit_idents di
    on ft.ident_value = di.ident_value
)
    
select
    app_id,
    ident_value,
    coalesce(sum(case when type = 'deposit' then total_val end), 0) as deposits_ex_value,
    coalesce(sum(case when type = 'withdraw' then total_val end), 0) as withdrawals_ex_value,
    coalesce(sum(case when type = 'deposit' then total_val end), 0) / nullif(sum(case when type = 'withdraw' then total_val end), 0) as deposit_per_withdrawal
from transactions
group by app_id, ident_value
