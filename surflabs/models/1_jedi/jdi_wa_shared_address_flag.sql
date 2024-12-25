{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}


select
    nominated_address,
    app_id,
    ident_value
from {{ ref('stg_hansolo_trivia_transactions') }}
where
    (nominated_address, app_id) in (
        select
            nominated_address,
            app_id
        from {{ ref('stg_hansolo_trivia_transactions') }}
        where type = 'withdraw'
        {{ dbt_utils.group_by(2) }}
        having count(distinct ident_value) > 1
    )
{{ dbt_utils.group_by(3) }}
