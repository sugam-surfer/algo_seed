{{ config(
    materialized='incremental',
    unique_key='ID',
) }}

-- Step 0: Data Limitation
with limited_data as (
    select 
        raw_user_balance_log.*,
        ref_transaction_reason.REASON_CATEGORY,
        ref_transaction_reason.REASON_TXN_TYPE
    from {{ ref('raw_user_balance_log') }} raw_user_balance_log
    left join 
    ahsokatano_4_raw.ref_transaction_reason ref_transaction_reason
    ON raw_user_balance_log.REASON = ref_transaction_reason.REASON
    where user_id in (
'2blsKwzIKLaoiYtIelUG4ZcirQG',
'2h8oVjD6B6Yd076F8zzukUp8hho',
'2h0qTHxXrKt3xBIFepJ8lgd0t8N',
'2hsuCXJVKGeCdxxrLRJ4wK8BWOH',
'2hvfSdu8VzRV83h62cPDdet9ZxM',
'2hvfSsjA5MWhUKJrM5Lfgrtx2BO',
'2hvfSNG3FAVXe3u7Whx9DizPKDs',
'2gyF01S1Mxdc6feV4o5RsATzmWH',
'2hsaHDu6VIv3q38HOyCwF7S9Vrq'
)
),

new_transactions AS (
    SELECT 
        ID,
        USER_ID,
        APP_ID,
        CURRENCY,
        TXN_ID,
        CHANGE,
        REASON,
        reason_category,
        REASON_TXN_TYPE,
        CREATED_AT,
        INSERTED_AT,
        ref_id,
        CASE 
            WHEN LOWER(REASON_TXN_TYPE) = 'credit' THEN CHANGE
            ELSE 0
        END AS remaining_credit,
        CASE 
            WHEN LOWER(REASON_TXN_TYPE) = 'debit' THEN CHANGE
            ELSE 0
        END AS remaining_debit
    FROM limited_data
    WHERE INSERTED_AT > (
        SELECT IFNULL(MAX(INSERTED_AT), '1970-01-01')
        FROM {{ this }}
    )
),

existing_credits AS (
    SELECT 
        USER_ID,
        APP_ID,
        CURRENCY,
        ID AS credit_txn_id,
        remaining_credit
    FROM {{ this }}
    WHERE remaining_credit > 0
),

existing_debits AS (
    SELECT 
        USER_ID,
        APP_ID,
        CURRENCY,
        ID AS debit_txn_id,
        remaining_debit
    FROM {{ this }}
    WHERE remaining_debit < 0
),

fifo_matching AS (
    SELECT 
        ec.USER_ID,
        ec.APP_ID,
        ec.CURRENCY,
        ec.credit_txn_id,
        ed.debit_txn_id,
        LEAST(ec.remaining_credit, -ed.remaining_debit) AS consumption_amount
    FROM existing_credits ec
    JOIN existing_debits ed
        ON ec.USER_ID = ed.USER_ID
        AND ec.APP_ID = ed.APP_ID
        AND ec.CURRENCY = ed.CURRENCY
    WHERE ec.remaining_credit > 0
      AND ed.remaining_debit < 0
    ORDER BY ec.credit_txn_id, ed.debit_txn_id
),

credit_consumption_update AS (
    SELECT 
        fm.credit_txn_id,
        SUM(fm.consumption_amount) AS total_consumed
    FROM fifo_matching fm
    GROUP BY fm.credit_txn_id
),

final_output AS (
    SELECT 
        nt.ID,
        nt.USER_ID,
        nt.APP_ID,
        nt.CURRENCY,
        nt.TXN_ID,
        nt.CHANGE,
        nt.REASON,
        nt.reason_category,
        nt.REASON_TXN_TYPE,
        nt.CREATED_AT,
        nt.INSERTED_AT,
        nt.ref_id,
        CASE 
            WHEN LOWER(nt.REASON_TXN_TYPE) = 'credit' THEN 
                GREATEST(nt.remaining_credit - COALESCE(ccu.total_consumed, 0), 0)
            ELSE nt.remaining_credit
        END AS remaining_credit,
        nt.remaining_debit,
        CASE 
            WHEN LOWER(nt.REASON_TXN_TYPE) = 'credit' AND nt.remaining_credit - COALESCE(ccu.total_consumed, 0) = 0 THEN 'Full'
            WHEN LOWER(nt.REASON_TXN_TYPE) = 'credit' AND nt.remaining_credit - COALESCE(ccu.total_consumed, 0) > 0 THEN 'Partial'
            ELSE 'No'
        END AS consumption_status
    FROM new_transactions nt
    LEFT JOIN credit_consumption_update ccu ON nt.ID = ccu.credit_txn_id
)

SELECT * FROM final_output
