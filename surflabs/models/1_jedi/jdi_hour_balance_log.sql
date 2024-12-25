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

recent_hours_user_balance_log AS (
    SELECT 
        DISTINCT hour_created_at AS hour_created_at
    FROM 
        {{ ref('jdi_hour_user_balance_log') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_hours_hansolo_withdraw_fee AS (
    SELECT 
        DISTINCT hour_created_at AS hour_created_at
    FROM 
        {{ ref('jdi_hour_hansolo_withdraw_fee') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_hours_hansolo_withdraw_txn AS (
    SELECT 
        DISTINCT hour_created_at AS hour_created_at
    FROM 
        {{ ref('jdi_hour_hansolo_withdraw_txn') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
),

recent_hours_hansolo_deposit_txn AS (
    SELECT 
        DISTINCT hour_created_at AS hour_created_at
    FROM 
        {{ ref('jdi_hour_hansolo_deposit_txn') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
),
    
recent_hours_padme_game_rev_cost AS (
    SELECT 
        DISTINCT hour_created_at AS hour_created_at
    FROM 
        {{ ref('jdi_hour_padme_game_rev_cost') }}
    WHERE 
        inserted_at >= (
            SELECT DATEADD(hour, -1, max_inserted_at) FROM latest_inserted_at
        )
)

select * from
    {{ ref('jdi_hour_user_balance_log') }}
    {% if is_incremental() %}
        WHERE hour_created_at IN (SELECT hour_created_at FROM recent_hours_user_balance_log)
    {% endif %}
union all
select * from
    {{ ref('jdi_hour_hansolo_withdraw_fee') }}
    {% if is_incremental() %}
        WHERE hour_created_at IN (SELECT hour_created_at FROM recent_hours_hansolo_withdraw_fee)
    {% endif %}
union all
select * from
    {{ ref('jdi_hour_hansolo_withdraw_txn') }}
    {% if is_incremental() %}
        WHERE hour_created_at IN (SELECT hour_created_at FROM recent_hours_hansolo_withdraw_txn)
    {% endif %}
union all
select * from
    {{ ref('jdi_hour_hansolo_deposit_txn') }}
    {% if is_incremental() %}
        WHERE hour_created_at IN (SELECT hour_created_at FROM recent_hours_hansolo_deposit_txn)
    {% endif %}
union all
select * from
    {{ ref('jdi_hour_padme_game_rev_cost') }}
    {% if is_incremental() %}
        WHERE hour_created_at IN (SELECT hour_created_at FROM recent_hours_padme_game_rev_cost)
    {% endif %}
