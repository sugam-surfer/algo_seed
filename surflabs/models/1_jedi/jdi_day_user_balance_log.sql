{{
    config(
        materialized = 'table',
        unique_key   = ['day_created_at','user_id','currency','reason'],
        cluster_by   = ['day_created_at','user_id','currency','reason'],
        tags         = ['warehouse'],
    )
}}

WITH latest_inserted_at AS (
    SELECT 
        IFNULL(MAX(inserted_at), '1900-01-01 00:00:00') AS max_inserted_at
    FROM 
        {{ this }}
),

recent_days AS (
    SELECT 
        DISTINCT DATE_TRUNC('day', created_at) AS day_created_at
    FROM 
        {{ ref('pdw_user_balance_log') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
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
	date_trunc(day, created_at) day_created_at,
        sum(change) change,
	sum(ex_value) ex_value,
	count(1) txn_count,
        max(inserted_at) inserted_at
    from
    {{ ref('pdw_user_balance_log') }}

    {% if is_incremental() %}
        WHERE DATE_TRUNC('day', created_at) IN (SELECT day_created_at FROM recent_days)
    {% endif %}

    {{ dbt_utils.group_by(9) }}
)

select 
    * 
from 
out_put
where 
day_created_at is not null
