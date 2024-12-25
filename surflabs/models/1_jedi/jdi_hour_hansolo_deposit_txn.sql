{{
    config(
        materialized = 'table',
        unique_key   = ['hour_created_at','user_id','currency','reason'],
        cluster_by   = ['hour_created_at','user_id','currency','reason'],
        tags         = ['warehouse'],
    )
}}

WITH latest_inserted_at AS (
    SELECT 
        IFNULL(MAX(inserted_at), '1900-01-01 00:00:00') AS max_inserted_at
    FROM 
        {{ this }}
),

recent_hours AS (
    SELECT 
        DISTINCT DATE_TRUNC('hour', created_at) AS hour_created_at
    FROM 
        {{ ref('stg_hansolo_deposit_txn') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
),

out_put as 
(   
select 
  user_id,
	app_id,
	currency,
	'deposit_hc' as reason,
	'deposit' as reason_category,
	'credit' as reason_txn_type,
	transaction_status,
	case when left(currency,1) = 'B' and right(currency,len(currency)-1) in {{ var_list('hard_currency_names') }} 
	then right(currency,len(currency)-1) else currency end as currency_match,
	date_trunc(hour, created_at) as hour_created_at,
  sum(amount) as change,
	sum(ex_value) as ex_value,
	count(1) as txn_count,
  max(inserted_at) as inserted_at
from
    {{ ref('stg_hansolo_deposit_txn') }}

    {% if is_incremental() %}
        WHERE DATE_TRUNC('hour', created_at) IN (SELECT hour_created_at FROM recent_hours)
    {% endif %}

    {{ dbt_utils.group_by(9) }}
)

select 
    * 
from 
out_put
where 
hour_created_at is not null
