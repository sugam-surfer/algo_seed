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

recent_days_user_balance_log AS (
    SELECT 
        DISTINCT day_created_at AS day_created_at
    FROM 
        {{ ref('jdi_day_user_balance_log') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_days_hansolo_withdraw_fee AS (
    SELECT 
        DISTINCT day_created_at AS day_created_at
    FROM 
        {{ ref('jdi_day_hansolo_withdraw_fee') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_days_hansolo_withdraw_txn AS (
    SELECT 
        DISTINCT day_created_at AS day_created_at
    FROM 
        {{ ref('jdi_day_hansolo_withdraw_txn') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_days_hansolo_withdraw_fee AS (
    SELECT 
        DISTINCT day_created_at AS day_created_at
    FROM 
        {{ ref('jdi_day_hansolo_deposit_txn') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_days_padme_game_rev_cost AS (
    SELECT 
        DISTINCT day_created_at AS day_created_at
    FROM 
        {{ ref('jdi_day_padme_game_rev_cost') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(day, -1, max_inserted_at) FROM latest_inserted_at
        )
)

select * from
    {{ ref('jdi_day_user_balance_log') }}
    {% if is_incremental() %}
        WHERE day_created_at IN (SELECT day_created_at FROM recent_days_user_balance_log)
    {% endif %}
union all
select * from
    {{ ref('jdi_day_hansolo_withdraw_fee') }}
    {% if is_incremental() %}
        WHERE day_created_at IN (SELECT day_created_at FROM recent_days_hansolo_withdraw_fee)
    {% endif %}
union all
select * from
    {{ ref('jdi_day_hansolo_withdraw_txn') }}
    {% if is_incremental() %}
        WHERE day_created_at IN (SELECT day_created_at FROM recent_days_hansolo_withdraw_txn)
    {% endif %}
union all
select * from
    {{ ref('jdi_day_hansolo_deposit_txn') }}
    {% if is_incremental() %}
        WHERE day_created_at IN (SELECT day_created_at FROM recent_days_hansolo_deposit_txn)
    {% endif %}
union all
select * from
    {{ ref('jdi_day_padme_game_rev_cost') }}
    {% if is_incremental() %}
        WHERE day_created_at IN (SELECT day_created_at FROM recent_days_padme_game_rev_cost)
    {% endif %}
