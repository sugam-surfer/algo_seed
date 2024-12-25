{{
    config(
        materialized	= 'incremental',
        unique_key	= ['base','target','ts_start'],
        cluster_by	= 'created_at::date',
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}


WITH 
min_ts_table as (
select min(ts_start) min_ts from {{ ref('raw_exchange_rate') }}
),

time_intervals AS (
  SELECT 
    min_ts AS start_time,
    DATEADD(MINUTE, 10, min_ts) AS end_time
  FROM 
    min_ts_table
  UNION ALL
  SELECT 
    end_time,
    DATEADD(MINUTE, 10, end_time)
  FROM 
    time_intervals
  WHERE 
    end_time < timestampadd(minute,420,CURRENT_TIMESTAMP())
),

time_range as (
SELECT 
  ROW_NUMBER() OVER (ORDER BY start_time) AS time_slice_index,
  start_time,
  DATEADD(second, -1, end_time) end_time
FROM 
  time_intervals
),

exch_start as (
select base, target, min(ts_start) min_ts from {{ ref('raw_exchange_rate') }} {{ dbt_utils.group_by(2) }}
),

full_run as (
SELECT 
exch_start.base,
exch_start.target,
time_range.time_slice_index,
time_range.start_time
from
exch_start full join time_range 
where exch_start.min_ts <= time_range.start_time
)

/*
,

usd_seeding_exch_start as (
select 'USD' as base, 'USD' as target, min(ts_start) min_ts from {{ ref('raw_exchange_rate') }}
),

usd_seeding_full_run as (
SELECT 
usd_seeding_exch_start.base,
usd_seeding_exch_start.target,
time_range.time_slice_index,
time_range.start_time
from
usd_seeding_exch_start full join time_range 
where usd_seeding_exch_start.min_ts <= time_range.start_time
)
*/

select
	ID,
	BASE,
	TARGET,
	RATE_OPEN,
	RATE_HIGH,
	RATE_LOW,
	RATE_CLOSE,
	TS_START,
	TS_END,
	SCRAPED_AT,
	CREATED_AT,
	INSERTED_AT,
	TIME_SLICE_INDEX,
	IS_FILLED
from
(
(
select 
full_run.time_slice_index,
full_run.base,
full_run.target,
main_table.id,
  COALESCE(rate_open,LAG(rate_open) ignore nulls OVER (ORDER BY full_run.base, full_run.target, full_run.time_slice_index)) AS rate_open,
  COALESCE(rate_high,LAG(rate_high) ignore nulls OVER (ORDER BY full_run.base, full_run.target, full_run.time_slice_index)) AS rate_high,
  COALESCE(rate_low,LAG(rate_low) ignore nulls OVER (ORDER BY full_run.base, full_run.target, full_run.time_slice_index)) AS rate_low,
  COALESCE(rate_close,LAG(rate_close) ignore nulls OVER (ORDER BY full_run.base, full_run.target, full_run.time_slice_index)) AS rate_close,
full_run.start_time ts_start,
main_table.ts_end,
main_table.scraped_at,
main_table.created_at,
  COALESCE(inserted_at,lag(inserted_at) ignore nulls OVER (ORDER BY full_run.base, full_run.target, full_run.time_slice_index)) AS inserted_at,
  main_table.id is null AS is_FILLED
from 
full_run full_run 
left join 
ahsokatano_4_raw.raw_exchange_rate main_table
on 
full_run.base = main_table.base and
full_run.target = main_table.target and
full_run.start_time = main_table.ts_start
order by full_run.target, full_run.time_slice_index
)

/*
union all

(
select 
usd_seeding_full_run.time_slice_index,
usd_seeding_full_run.base,
usd_seeding_full_run.target,
0 as id,
1 AS rate_open,
1 AS rate_high,
1 AS rate_low,
1 AS rate_close,
usd_seeding_full_run.start_time AS ts_start,
DATEADD(second, 10*60-1, usd_seeding_full_run.start_time) AS ts_end,
usd_seeding_full_run.start_time AS scraped_at,
usd_seeding_full_run.start_time AS created_at,
usd_seeding_full_run.start_time AS inserted_at,
true AS is_FILLED
from 
usd_seeding_full_run usd_seeding_full_run 
)
*/

) a
