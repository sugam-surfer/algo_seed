{{
    config(
        materialized = 'incremental',
        unique_key   = 'user_id',
        tags         = ['withdrawal_automation', 'reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

select
    user_id,
    max(inserted_at) as inserted_at
from {{ ref('stg_hansolo_trivia_transactions') }}
where type = 'withdraw'
    and status in (1,3)
    {% if is_incremental() -%}
    and stg_hansolo_trivia_transactions.inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
{{ dbt_utils.group_by(1) }}
