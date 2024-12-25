{{
    config(
        materialized	= 'table',
        unique_key	= 'ident_value',
        tags            = 'warehouse',
    )
}}

SELECT *
	,first_balance_log_ts > ifnull(first_exchange_record_ts, current_timestamp) AS condition
FROM (
	SELECT 
		raw_user_balance_log.currency,
		currency_match,
		min(created_at) first_balance_log_ts
	FROM ahsokatano_4_raw.raw_user_balance_log
	LEFT JOIN ahsokatano_4_raw.ref_currency_match a ON raw_user_balance_log.currency = a.currency
	WHERE raw_user_balance_log.currency NOT IN ('soft')
	{{ dbt_utils.group_by(2) }}
	) a
LEFT JOIN (
	SELECT target exchange_currency
		,min(ts_start) first_exchange_record_ts
	FROM ahsokatano_4_raw.raw_exchange_rate
	GROUP BY 1
	) b ON a.currency_match = b.exchange_currency
