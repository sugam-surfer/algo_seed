{{
    config(
        materialized    = 'table',
        unique_key      = ['app_id','type','currency','created_date'],
        cluster_by      = 'created_date',
        tags            = ['warehouse'],
    )
}}

{{ incremental_message() }}

select
    app_id,
    type,
    transaction_status,
    currency,
    agent,
    created_at::date as created_date,
    decided_at::date as decided_date,
    count(distinct id) as num_transactions,
    sum(amount) as real_value,
    sum(ex_value) as nominal_value,
    max(inserted_at) as inserted_at
from {{ ref('stg_hansolo_trivia_transactions') }}
{% if is_incremental() -%}
where
    stg_hansolo_trivia_transactions.inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
{%- endif %}
{{ dbt_utils.group_by(7) }}
