{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select 
x.identifier,
y.app_name as app_name,
null as ATTR_AF_STATUS,
null as ATTR_MEDIA_SOURCE,
null as ATTR_AF_CHANNEL,
null as ATTR_CAMPAIGN_NAME,
null as ATTR_CAMPAIGN_ID,
null as ATTR_AF_ADSET_ID,
concat('dummy_',ROW_NUMBER() OVER (ORDER BY x.date, x.identifier)) user_id,
x.date,
x.cpl
from
{{ ref('stg_attr_central') }} x
left join 
{{ ref('raw_attr_identifier') }} y
    on x.identifier = y.identifier
where user_need = 'dummy'
