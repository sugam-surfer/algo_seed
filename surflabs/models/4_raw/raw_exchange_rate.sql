{{
    config(
        materialized    = 'incremental',
        unique_key      = ['base','target','ts_start'],
        cluster_by      = 'created_at::date',
    	post_hook       = ["truncate table ahsokatano_4_raw.old_exchange_rate"],
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

exchange_rate as (
    select
        * exclude (ts_start, ts_end, scraped_at, created_at),
        time_slice(ts_start,10,'minute') as ts_start,
        ts_end, scraped_at, created_at,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ source('padme', 'exchange_rate') }}
    {% if is_incremental() -%}
    where created_at > (select ifnull(max(created_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),

old_exchange_rate as (
    select
        *,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ ref('old_exchange_rate') }}
),

total_exchange_rate as (
select * from exchange_rate
union all
select * from old_exchange_rate
),

output_data_3 as (
    select * from total_exchange_rate where lower(target) not in ('grape')
union all
    select * from total_exchange_rate where lower(target) in ('grape') and date(ts_start) <> '2024-03-12'
union all
    select * from total_exchange_rate where lower(target) in ('grape') and date(ts_start) = '2024-03-12' and rate_close >= 1
union all
    select 
        ID,
        BASE,
        TARGET,
        1/RATE_OPEN RATE_OPEN,
        1/RATE_HIGH RATE_HIGH,
        1/RATE_LOW RATE_LOW,
        1/RATE_CLOSE RATE_CLOSE,
        TS_START,
        TS_END,
        SCRAPED_AT,
        CREATED_AT,
        INSERTED_AT
    from total_exchange_rate where lower(target) in ('grape') and date(ts_start) = '2024-03-12' and rate_close < 1 
),

output_data_2 as (
    select * from output_data_3
minus
    select * from total_exchange_rate
    where id in (178284, 178287, 178354, 178358, 205130, 200356)   
),
    
output_data_1 as (
    select * from output_data_2
union all
    select * exclude (ts_start, ts_end, scraped_at, created_at, INSERTED_AT), 
        dateadd(day,-2,ts_start) ts_start, 
        dateadd(day,-2,ts_end) ts_end,
        dateadd(day,-2,scraped_at) scraped_at, 
        dateadd(day,-2,created_at) created_at,
        INSERTED_AT
    from total_exchange_rate
    where id in (178284, 178287, 178354, 178358, 205130)  
),

output_data as (
    select
        *,
        rank() over(partition by base, target, ts_start order by scraped_at desc) as scrape_rank
    from output_data_1
)

select
    * exclude scrape_rank
from output_data where scrape_rank = 1
