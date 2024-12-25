{{
    config(
        materialized    = 'table',
        unique_key      = 'user_id',
        cluster_by      = 'user_id',
        tags		= 'warehouse',
    )
}}


with table_b as (
  select user_id, first_deposit_ts, inserted_at
  from {{ ref('jdi_lt_user_deposit_txn_prop') }}
),

{% if is_incremental() %}
table_b_updates as (
  select user_id
  from table_b
  where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
),
{% endif %}

table_a as (
  select *
  from {{ ref('stg_attr_all_user_with_cpl') }}
  {% if is_incremental() %}
  where
    inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    or user_id in (select user_id from table_b_updates)
  {% endif %}
),

table_c as (
  select *
  from {{ ref('jdi_lt_user_loc_os') }}
  {% if is_incremental() %}
  where
    inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    or user_id in (select user_id from table_b_updates)
  {% endif %}
),

final as (
  select
    table_a.*,
    table_b.first_deposit_ts,
    table_c.os_first,
    table_c.city_first,
    table_c.country_code_first as country_first,
    table_c.os_last,
    table_c.city_last,
    table_c.country_code_last as country_last
  from
    table_a
    left join table_b 
    on table_a.user_id = table_b.user_id
    left join table_c
    on table_a.user_id = table_c.user_id
)

select
  *
from final
