{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= 'id',
	tags		= ['reseed', 'old', 'warehouse'],
    )
}}

{{ incremental_message() }}
	
with

final_date as (
select max(created_at) final_date from ahsokatano_4_raw.raw_exchange_rate   
),

final_data as (
select * exclude (inserted_at) from ahsokatano_4_raw.raw_exchange_rate    
),
    
output_data_2 as (
    select 
    exchange_rate.* exclude (ts_start, ts_end, scraped_at, created_at),
    time_slice(ts_start,10,'minute') as ts_start,
    ts_end, scraped_at, created_at
	from {{ source('padme', 'exchange_rate') }} exchange_rate 
    left join final_date
    where exchange_rate.created_at <= final_date.final_date
),    

output_data_1 as (
    select
        *,
        rank() over(partition by base, target, ts_start order by scraped_at desc) as scrape_rank
    from output_data_2
),

output_data as (
select
    * exclude scrape_rank
from output_data_1 where scrape_rank = 1
)

select * exclude (TS_START, TS_END, SCRAPED_AT, CREATED_AT), TS_START, TS_END, SCRAPED_AT, CREATED_AT from output_data
--	where hour(current_timestamp) in (0,12)
minus
select * from final_data

