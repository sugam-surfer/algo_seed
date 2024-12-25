{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select
identifier,
user_count,
app_name,
ATTR_AF_STATUS,
ATTR_MEDIA_SOURCE,
ATTR_AF_CHANNEL,
ATTR_CAMPAIGN_NAME,
ATTR_CAMPAIGN_ID,
ATTR_AF_ADSET_ID
from
(
select
identifier,
app_name,
ATTR_AF_STATUS,
ATTR_MEDIA_SOURCE,
ATTR_AF_CHANNEL,
ATTR_CAMPAIGN_NAME,
ATTR_CAMPAIGN_ID,
ATTR_AF_ADSET_ID,
count(1) as user_count
from 
{{ ref('stg_attr_user') }}
{{ dbt_utils.group_by(8) }}
order by 2,3,9 desc,1
)
