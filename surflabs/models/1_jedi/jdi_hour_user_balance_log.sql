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
        {{ ref('pdw_user_balance_log') }}
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
	reason,
	reason_category,
	reason_txn_type,
	transaction_status,
	currency_match,
	date_trunc(hour, created_at) hour_created_at,
        sum(change) change,
	sum(ex_value) ex_value,
	count(1) txn_count,
        max(inserted_at) inserted_at
    from
    {{ ref('pdw_user_balance_log') }}

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
