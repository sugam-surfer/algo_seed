{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select 
date(created_at) created_date,
identifier,
count(1) user_count
from
{{ ref('stg_attr_user') }}
where identifier <> 'null'
group by 1,2
