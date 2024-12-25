{{
    config(
        materialized    = 'table',
        unique_key      = 'reason',
        tags            = 'warehouse',
    )
}}

select reason,
case reason 
when 'hc_withdraw' then FALSE
when 'level_up' then FALSE
when 'hc_withdraw_refund' then FALSE
when 'hc_deposit_bonus' then FALSE
when 'web3_hc' then FALSE
when 'entry_fee' then FALSE
when 'd_reward' then FALSE
when 'leaderboard' then FALSE
when 'hc_deposit' then FALSE
when 'win' then FALSE
when 'social_hc' then FALSE
else TRUE 
end as is_new_reason
from (    
select distinct reason from 
(
    select 
    user_balance_log.reason,
    ifnull(user_balance_log.app_id, app.id) as app_id
    from {{ source('padme','user_balance_log') }} user_balance_log 
    left join {{ source('padme', 'user') }} user on user.id = user_balance_log.user_id
    left join {{ source('hansolo', 'app') }} app on app.company_id = user.company_id
)
where app_id in {{ var_list('games') }}
minus
select distinct reason from ahsokatano_4_raw.ref_transaction_reason
)
