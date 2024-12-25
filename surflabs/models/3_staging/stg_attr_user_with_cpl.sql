{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select 
a.*,
b.cpl 
from {{ ref('stg_attr_user') }} a
left join 
{{ ref('stg_attr_central') }} b
on 
date(a.created_at) = b.date and
a.identifier = b.identifier
