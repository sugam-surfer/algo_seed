{{
    config(
        materialized    = 'table',
        unique_key      = 'ident_value',
        tags            = 'warehouse',
    )
}}

select distinct ident_value from hansolo.trivia_transactions
minus select distinct ident_value from hansolo.web3auth_wallets

