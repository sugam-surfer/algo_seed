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
        {{ ref('pdw_padme_game_play') }}
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
	'NOTIONAL USD' as currency,
	case 
	when is_fake = 'fake' and HOUSE_CONT_PP_EX_VALUE_ALL_HC >= 0 then 'ftue_revenue'  
	when is_fake = 'fake' and HOUSE_CONT_PP_EX_VALUE_ALL_HC < 0 then 'ftue_cost'  
	when is_fake = 'normal' and HOUSE_CONT_PP_EX_VALUE_ALL_HC >= 0 then 'normal_revenue'  
	when is_fake = 'normal' and HOUSE_CONT_PP_EX_VALUE_ALL_HC < 0 then 'normal_cost'  
	else 'unknown' end as reason,
	case 
	when HOUSE_CONT_PP_EX_VALUE_ALL_HC >= 0 then 'revenue'  
	when HOUSE_CONT_PP_EX_VALUE_ALL_HC < 0 then 'revenue'  
	else 'unknown' end as reason_category,
	'neutral' as reason_txn_type,
	game_status as transaction_status,
	'NOTIONAL USD' as currency_match,
	date_trunc(day, created_at) as day_created_at,
	sum(HOUSE_CONT_PP_EX_VALUE_ALL_HC) as change,
	sum(HOUSE_CONT_PP_EX_VALUE_ALL_HC) as ex_value,
	count(1) as txn_count,
	max(inserted_at) as inserted_at
from
    {{ ref('pdw_padme_game_play') }}

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
