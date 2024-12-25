{{
    config(
        materialized    = 'incremental',
        unique_key      = ['user_id','currency_match','id'],
        cluster_by      = ['user_id','currency_match','id','created_at'],
        tags		= ['reseed', 'warehouse'],
    )
}}

with
base as (
select * from {{ ref('stg_user_balance_log') }}
   {% if is_incremental() %}
  where user_id in
         (
        select 
            user_id
        from 
        {{ ref('stg_user_balance_log') }}
        where inserted_at >= (
                            select 
                                dateadd(hour,-6,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )

/*	
	or user_id in 
	(
	select user_id
        from 
        {{ ref('stg_hansolo_trivia_transactions') }}
	where inserted_at >= (
                            select 
                                dateadd(hour,-6,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )	
	)
*/
	
{%- endif %}
),

/*
base_deposit_1 as (
SELECT stg_user_balance_log.*
	,raw_user_ident.ident_value
	,stg_hansolo_trivia_transactions.transaction_status
	,stg_hansolo_trivia_transactions.id AS transaction_id
    ,ifnull(timestampdiff(second,stg_hansolo_trivia_transactions.created_at,stg_user_balance_log.created_at),0) gap
    ,case when gap >= 0 then 0 else -1 end as gap_sign
    ,abs(gap) - 0.1*gap_sign as proximity
    ,rank() over (partition by stg_user_balance_log.id order by proximity) rank_number
    ,rank() over (partition by stg_user_balance_log.user_id, stg_user_balance_log.created_at order by stg_user_balance_log.id) as id_rank_number
    ,rank() over (partition by stg_user_balance_log.user_id, stg_hansolo_trivia_transactions.created_at order by transaction_id) as transaction_id_rank_number
FROM base stg_user_balance_log
LEFT JOIN {{ ref('raw_user_ident') }} raw_user_ident ON stg_user_balance_log.user_id = raw_user_ident.user_id
LEFT JOIN {{ ref('stg_hansolo_trivia_transactions') }} stg_hansolo_trivia_transactions ON stg_hansolo_trivia_transactions.ident_value = raw_user_ident.ident_value
	AND lower(stg_hansolo_trivia_transactions.type) = lower(stg_user_balance_log.reason_category)
	AND stg_hansolo_trivia_transactions.created_at <= timestampadd(second, 600, stg_user_balance_log.created_at)
	AND stg_hansolo_trivia_transactions.created_at >= timestampadd(second, -600, stg_user_balance_log.created_at)
WHERE stg_user_balance_log.reason_category = 'deposit'
),

base_deposit as (
select * exclude ( gap, gap_sign, proximity, rank_number, id_rank_number, transaction_id_rank_number)
	from base_deposit_1
	where rank_number = 1
	and id_rank_number = transaction_id_rank_number
),
	
base_withdraw_1 as (
SELECT stg_user_balance_log.*
	,time_slice(decided_at,10,'minute') as ts_decided
	,raw_user_ident.ident_value
	,stg_hansolo_trivia_transactions.transaction_status
	,stg_hansolo_trivia_transactions.id AS transaction_id
    ,ifnull(timestampdiff(second,stg_hansolo_trivia_transactions.created_at,stg_user_balance_log.created_at),0) gap
    ,case when gap >= 0 then 0 else -1 end as gap_sign
    ,abs(gap) - 0.1*gap_sign as proximity
    ,rank() over (partition by stg_user_balance_log.id order by proximity) rank_number
    ,rank() over (partition by stg_user_balance_log.user_id, stg_user_balance_log.created_at order by stg_user_balance_log.id) as id_rank_number
    ,rank() over (partition by stg_user_balance_log.user_id, stg_hansolo_trivia_transactions.created_at order by transaction_id) as transaction_id_rank_number
FROM base stg_user_balance_log
LEFT JOIN {{ ref('raw_user_ident') }} raw_user_ident ON stg_user_balance_log.user_id = raw_user_ident.user_id
LEFT JOIN {{ ref('stg_hansolo_trivia_transactions') }} stg_hansolo_trivia_transactions ON stg_hansolo_trivia_transactions.ident_value = raw_user_ident.ident_value
	AND lower(stg_hansolo_trivia_transactions.type) = lower(stg_user_balance_log.reason_category)
	AND stg_hansolo_trivia_transactions.created_at <= timestampadd(second, 600, stg_user_balance_log.created_at)
	AND stg_hansolo_trivia_transactions.created_at >= timestampadd(second, -600, stg_user_balance_log.created_at)
WHERE stg_user_balance_log.reason_category = 'withdraw'
),

int_withdraw_1 as (
select * exclude ( gap, gap_sign, proximity, rank_number, id_rank_number, transaction_id_rank_number)
	from base_withdraw_1
	where rank_number = 1
	and id_rank_number = transaction_id_rank_number
),

int_withdraw as (
select
    int_withdraw_1.*,
    stg_exchange_rate.rate_close as new_rate_close,
    round(ifnull(change/stg_exchange_rate.rate_close,change),2) as new_ex_value
from int_withdraw_1
left join {{ ref('stg_exchange_rate') }} stg_exchange_rate on stg_exchange_rate.ts_start = int_withdraw_1.ts_decided
    and stg_exchange_rate.target = int_withdraw_1.currency_match
),
	
base_withdraw as (
select 
	int_withdraw.* exclude ( rate_close, ex_value, ts_decided, ident_value, transaction_status, transaction_id, new_rate_close, new_ex_value),
	case when ts_decided is null then rate_close else new_rate_close end as rate_close,
	case when ts_decided is null then ex_value else new_ex_value end as ex_value,
	ident_value, transaction_status, transaction_id
from int_withdraw
),
*/
	
base_gameplay_and_others as (
SELECT stg_user_balance_log.*
	,null as ident_value
	,null as transaction_status
	,null AS transaction_id
FROM base stg_user_balance_log
WHERE stg_user_balance_log.reason_category not in ('deposit','withdraw')
),

base_all as (

/*
select * from base_deposit
	union all
select * from base_withdraw
	union all
*/

select * from base_gameplay_and_others
)
	
select * from base_all



