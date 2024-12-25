{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select 
a.date,
a.identifier,
a.cost,
case when b.created_date is null then 'dummy' else 'ok' end as user_need,
ifnull(b.user_count,1) adjusted_user_count,
case when b.created_date is null then cost else cost/user_count end as cpl
from
(select date, identifier, cost from {{ ref('raw_attr_unpivot_source_spends') }} where cost <> 0 ) a
left join 
(select created_date, identifier, user_count from {{ ref('stg_attr_unpivot_user_count') }} ) b
on
a.date = b.created_date and
a.identifier = b.identifier
