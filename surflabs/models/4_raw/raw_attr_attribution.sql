{{
    config(
        materialized    = 'table',
        unique_key      = ['APP_NAME','ATTR_AF_STATUS','ATTR_MEDIA_SOURCE','ATTR_AF_CHANNEL','ATTR_CAMPAIGN_NAME','ATTR_CAMPAIGN_ID','ATTR_AF_ADSET_ID'],
        tags            = ['attr', 'warehouse'],
    )
}}

select * from {{ ref('input_gr_attr_attribution') }}
    union all
select * from {{ ref('input_la_attr_attribution') }}
    union all
select * from ahsokatano_4_raw.input_tz_attr_attribution

/*    
with

input_attr_attribution as (
select * from ahsokatano_4_raw.input_gr_attr_attribution
    union all
select * from ahsokatano_4_raw.input_la_attr_attribution
    union all
select * from ahsokatano_4_raw.input_tz_attr_attribution

),

input_hashed AS (
    SELECT
        *,
        HASH(APP_NAME,ATTR_AF_STATUS,ATTR_MEDIA_SOURCE,ATTR_AF_CHANNEL,ATTR_CAMPAIGN_NAME,ATTR_CAMPAIGN_ID,ATTR_AF_ADSET_ID) AS pk_hash, 
        HASH(APP_NAME,ATTR_AF_STATUS,ATTR_MEDIA_SOURCE,ATTR_AF_CHANNEL,ATTR_CAMPAIGN_NAME,ATTR_CAMPAIGN_ID,ATTR_AF_ADSET_ID,identifier) AS row_hash 
    FROM
        input_attr_attribution
),
  
raw_hashed AS (

select 
    a.*,
    b.row_hash
from
    (
    SELECT
        *,
        HASH(APP_NAME,ATTR_AF_STATUS,ATTR_MEDIA_SOURCE,ATTR_AF_CHANNEL,ATTR_CAMPAIGN_NAME,ATTR_CAMPAIGN_ID,ATTR_AF_ADSET_ID) AS pk_hash 
    FROM ahsokatano_4_raw.raw_attr_attribution 
    ) a
    left join
    (
    select 
        *,
        HASH(APP_NAME,ATTR_AF_STATUS,ATTR_MEDIA_SOURCE,ATTR_AF_CHANNEL,ATTR_CAMPAIGN_NAME,ATTR_CAMPAIGN_ID,ATTR_AF_ADSET_ID) AS pk_hash, 
        HASH(APP_NAME,ATTR_AF_STATUS,ATTR_MEDIA_SOURCE,ATTR_AF_CHANNEL,ATTR_CAMPAIGN_NAME,ATTR_CAMPAIGN_ID,ATTR_AF_ADSET_ID,identifier) AS row_hash 
    from (
    SELECT
        * exclude (created_at, updated_at)
    FROM ahsokatano_4_raw.raw_attr_attribution )
    ) b
    on a.pk_hash = b.pk_hash
    
),

flag_table as (
SELECT
    t1.*,
    t2.created_at raw_created_at,
    t2.updated_at raw_updated_at,
    CASE
        WHEN t2.pk_hash IS NOT NULL THEN 1
        ELSE 0
    END AS exist_flag,
    CASE
        WHEN t2.row_hash IS NOT NULL THEN 1
        ELSE 0
    END AS stable_flag
FROM
    input_hashed t1
LEFT JOIN
    raw_hashed t2
ON
    t1.pk_hash = t2.pk_hash and
    t1.row_hash = t2.row_hash
)

select * exclude (pk_hash, row_hash, raw_created_at, raw_updated_at, exist_flag, stable_flag), 
  current_timestamp() created_at, 
  current_timestamp() updated_at 
from flag_table where exist_flag = 0 
union all
select * exclude (pk_hash, row_hash, raw_created_at, raw_updated_at, exist_flag, stable_flag), 
  raw_created_at created_at, 
  current_timestamp() updated_at 
from flag_table where exist_flag = 1 and stable_flag = 0
*/

