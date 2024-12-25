{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

with 

stg_user_paranthesis as (
select
ifnull(app_name,'[]') as app_name,
ATTR_AF_STATUS,
ATTR_MEDIA_SOURCE,
ATTR_AF_CHANNEL,
ATTR_CAMPAIGN_NAME,
ATTR_CAMPAIGN_ID,
ATTR_AF_ADSET_ID,
id as user_id,
x.created_at
from
{{ ref('stg_user') }} x
left join 
{{ ref('ref_apps') }} y
on x.app_id = y.app_id
)

select 
ifnull(q.identifier,'null') as identifier,
p.* 
from 
stg_user_paranthesis p
left join 
( select * exclude (user_count) from {{ ref('raw_attr_attribution') }} ) q
on 
p.app_name = q.app_name and
p.ATTR_AF_STATUS = q.ATTR_AF_STATUS and
p.ATTR_MEDIA_SOURCE = q.ATTR_MEDIA_SOURCE and
p.ATTR_AF_CHANNEL = q.ATTR_AF_CHANNEL and
p.ATTR_CAMPAIGN_NAME = q.ATTR_CAMPAIGN_NAME and
p.ATTR_CAMPAIGN_ID = q.ATTR_CAMPAIGN_ID and
p.ATTR_AF_ADSET_ID = q.ATTR_AF_ADSET_ID

