{{ config(
    materialized='incremental',
    unique_key='ID, is_real',
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

source_data as (
    -- Fetch only new or updated transactions since the last run
    select
        ID,
        USER_ID,
        CURRENCY,
        CASE WHEN LEFT(CURRENCY,1) = 'B' AND right(currency, len(currency)-1) in {{ var_list('hard_currency_names') }} 
        then right(currency, len(currency)-1) else currency end as currency_match,
        case when REASON_TXN_TYPE = 'credit' then abs(CHANGE) 
        when REASON_TXN_TYPE = 'debit' then -1*abs(CHANGE) else 0 end as change ,
        REASON,
        TXN_ID,
        CREATED_AT,
        REF_ID,
        APP_ID,
        current_timestamp as INSERTED_AT,
        REASON_CATEGORY,
        REASON_TXN_TYPE
    from limited_data -- your source table
    {% if is_incremental() %}
    where INSERTED_AT > (select max(INSERTED_AT) from {{ this }}) -- Only process new or updated records
    {% endif %}
),

-- Calculate ledger balances based on new transactions
source_running_ledger_base as (
    select
        sd.ID,
        sd.USER_ID,
        sd.CURRENCY,
        sd.CURRENCY_MATCH,
        sd.CHANGE,
        sd.REASON,
        sd.TXN_ID,
        sd.CREATED_AT,
        sd.REF_ID,
        sd.APP_ID,
        sd.INSERTED_AT,
        sd.REASON_CATEGORY,
        sd.REASON_TXN_TYPE,
        -- Compute ledger_balance based on previous records and new transactions
        sum(sd.CHANGE) over (
            partition by sd.USER_ID, sd.APP_ID, sd.CURRENCY_MATCH
            order by sd.CREATED_AT, sd.ID -- Order by CREATED_AT and ID (for tie-breaking)
            rows between unbounded preceding and current row
        ) as ledger_balance,
        null as is_latest,
        1 as is_real
    from source_data sd
),

source_impute as (
    select
        ID,
        USER_ID,
        CURRENCY,
        CURRENCY_MATCH,
        case when ledger_balance > CHANGE then -1*ledger_balance else -1*change end as change,
        'imputed' as REASON,
        null as TXN_ID,
        CREATED_AT,
        null as REF_ID,
        APP_ID,
        INSERTED_AT,
        'imputed' as REASON_CATEGORY,
        'credit' as REASON_TXN_TYPE,
        0 as ledger_balance,
        false as is_latest, 
        0 as is_real 
    from source_running_ledger_base where ledger_balance < 0
),

source_combined as (
    select * from source_running_ledger_base
        union all
    select * from source_impute
),

source_running_ledger as (
    select
        sd.ID,
        sd.USER_ID,
        sd.CURRENCY,
        sd.CURRENCY_MATCH,
        sd.CHANGE,
        sd.REASON,
        sd.TXN_ID,
        sd.CREATED_AT,
        sd.REF_ID,
        sd.APP_ID,
        sd.INSERTED_AT,
        sd.REASON_CATEGORY,
        sd.REASON_TXN_TYPE,
        -- Compute ledger_balance based on previous records and new transactions
        sum(sd.CHANGE) over (
            partition by sd.USER_ID, sd.APP_ID, sd.CURRENCY_MATCH
            order by sd.CREATED_AT, sd.ID, sd.is_real -- Order by CREATED_AT and ID (for tie-breaking)
            rows between unbounded preceding and current row
        ) as ledger_balance,
        sd.is_latest,
        sd.is_real
    from source_combined sd
),

-- Determine the latest transactions for each tuple
source_latest_transaction as (
    select
        USER_ID,
        APP_ID,
        CURRENCY_MATCH,
        max(CREATED_AT) as max_created_at,
        max(ID) as max_ID
    from source_data
    group by USER_ID, APP_ID, CURRENCY_MATCH
),

previous_latest_data as (
    select
        * 
    from {{ this }}
    where is_latest = true
),

-- Mark the latest records and update `is_latest` for the current run and add balances, if exists, from the previous records
source_latest as (
    select
        srl.ID,
        srl.USER_ID,
        srl.CURRENCY,
        srl.CURRENCY_MATCH,
        srl.CHANGE,
        srl.REASON,
        srl.TXN_ID,
        srl.CREATED_AT,
        srl.REF_ID,
        srl.APP_ID,
        srl.INSERTED_AT,
        srl.REASON_CATEGORY,
        srl.REASON_TXN_TYPE,
        srl.ledger_balance + coalesce(pld.ledger_balance,0) as ledger_balance,
        -- Flag to identify the latest record
        case
            when srl.CREATED_AT = slt.max_created_at and srl.ID = slt.max_ID and srl.is_real = 1 then true
            else false
        end as is_latest,
        srl.is_real
    from source_running_ledger srl
    left join source_latest_transaction slt
        on srl.USER_ID = slt.USER_ID
        and srl.APP_ID = slt.APP_ID
        and srl.CURRENCY_MATCH = slt.CURRENCY_MATCH
    left join previous_latest_data pld
        on srl.USER_ID = pld.USER_ID
        and srl.APP_ID = pld.APP_ID
        and srl.CURRENCY_MATCH = pld.CURRENCY_MATCH
),

-- Mark previous records as not latest
previous_change_records as (
    select
        * exclude (is_latest, is_real),
        false as is_latest,
        is_real
    from {{ this }}
    where is_latest = true
    and (USER_ID, APP_ID, CURRENCY_MATCH) in (
        select
            USER_ID,
            APP_ID,
            CURRENCY_MATCH
        from source_latest_transaction
    )
),

-- Combine new and updated existing records
final_data as (
    select * from source_latest
    union all
    select * from previous_change_records
)

select * from final_data
