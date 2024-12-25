{{
    config(
        materialized    = 'incremental',
        unique_key      = ['user_id','device_token','app_id'],
        cluster_by      = ['user_id','device_token','app_id'],
    	tags    	= ['events', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

input_data_1 as (
  select
    distinct user_id, device_token, app_id, inserted_at
  from {{ ref('stg_ae_events') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),

input_data as (
    select * from input_data_1 where device_token is not null and device_token <> '' and device_token <> 'n/a'
),
    
input_data_agg AS (
  SELECT 
    user_id, device_token, app_id, MAX(inserted_at) AS inserted_at
  FROM input_data
  GROUP BY user_id, device_token, app_id
),

static_data AS (
  SELECT 
    user_id, device_token, app_id, inserted_at
  FROM AHSOKATANO_2_padawan.pdw_ae_user_device_0
)

SELECT 
    COALESCE(a.user_id, b.user_id) AS user_id,
    COALESCE(a.device_token, b.device_token) AS device_token,
    COALESCE(a.app_id, b.app_id) AS app_id,
    COALESCE(a.inserted_at, b.inserted_at) AS inserted_at,
    CASE 
      WHEN a.user_id IS NOT NULL AND b.user_id IS NOT NULL THEN 'OLD'
      WHEN a.user_id IS NOT NULL THEN 'NEW'
      ELSE 'OLD'
    END AS record_type
FROM 
    input_data_agg a
FULL OUTER JOIN
    static_data b
ON 
    a.user_id = b.user_id AND
    a.device_token = b.device_token AND
    a.app_id = b.app_id

