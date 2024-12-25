{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

select
    stg_hansolo_trivia_transactions.id,
    stg_hansolo_trivia_transactions.app_id,
    stg_hansolo_trivia_transactions.ident_type,
    stg_hansolo_trivia_transactions.ident_value,
    stg_hansolo_trivia_transactions.user_id,
    stg_hansolo_trivia_transactions.nominated_address,
    stg_hansolo_trivia_transactions.created_at,
    currency,
    amount,
    ex_value
from {{ ref('stg_hansolo_trivia_transactions') }}
where type = 'withdraw'
    and status = 0
    --and stg_hansolo_trivia_transactions.created_at::date >= dateadd(day,-3,current_date())
