{{
    config(
        materialized	= 'incremental',
        unique_key	= 'currency',
        cluster_by	= 'updated_at::date',
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

all_curr as (
select
        entry_fee, 
		id, 
		updated_at
    from {{ source('padme','game_play') }}
    {% if is_incremental() -%}
    where updated_at > (select ifnull(max(updated_at),'1900-01-01 00:00:00') from {{ this }})
	and app_id in {{ var_list('games') }}
    {%- endif %}
),

all_curr_flatten as (
select 
parse_json(entry_fee)[0]:currency::string currency,
id,
updated_at
from all_curr
where parse_json(entry_fee)[0]:currency::string is not null

union

select 
parse_json(entry_fee)[1]:currency::string currency,
id,
updated_at
from all_curr
where parse_json(entry_fee)[1]:currency::string is not null

union

select 
parse_json(entry_fee)[2]:currency::string currency,
id,
updated_at
from all_curr
where parse_json(entry_fee)[2]:currency::string is not null
),

all_curr_processed as (
select *, 
row_number() over (partition by currency order by updated_at) rn
from all_curr_flatten
),

all_curr_first_record as (
select * exclude (rn) from all_curr_processed
where rn=1
),

check_curr as (
select *,
case when currency in {{ var_list('soft_currency_names') }} then 1 else 0 end as check_sc,	
case when currency in {{ var_list('hard_currency_names') }} then 1 else 0 end as check_hc,
case when right(currency,len(currency)-1) in {{ var_list('hard_currency_names') }} then 1 else 0 end as check_bonus_hc	
from all_curr_first_record
)
	
select * exclude (check_sc, check_hc, check_bonus_hc), (check_sc + check_hc + check_bonus_hc) check_curr from check_curr
