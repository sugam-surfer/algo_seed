{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

select
    id, app_id, currency, amount, ex_value
from {{ ref('stg_hansolo_trivia_transactions') }}
where decided_at::date = current_date()
    and type = 'withdraw'
    and status in (1,3)
