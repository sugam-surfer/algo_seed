{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

with

required_apps as (
    select app_id
    from (
        values
        {% for app_id in var('games_withdrawal_automation') %}
            ('{{ app_id }}'){% if not loop.last %},{% endif %}
        {% endfor %}
    ) AS t(app_id)
),

approved_withdrawals_today as (
    select
        app_id,
        sum(ex_value) as total_ex_value
    from {{ ref('jdi_wa_approved_withdrawals_today') }}
    {{ dbt_utils.group_by(1) }}
),

remaining_daily_budget as (
    select
        r.app_id,
        coalesce(total_ex_value, 0) as total_ex_value,
        b.budget,
        b.budget - coalesce(total_ex_value, 0) as budget_left
    from required_apps r
    left join approved_withdrawals_today a on r.app_id = a.app_id
    left join (select app_id, {{ withdrawal_daily_budget('app_id') }} as budget from required_apps) b on r.app_id = b.app_id
)

select
    jdi_wa_decision_table.id,
    jdi_wa_decision_table.ident_type,
    jdi_wa_decision_table.ident_value,
    jdi_wa_decision_table.user_id,
    jdi_wa_decision_table.nominated_address,
    jdi_wa_decision_table.app_id,
    currency,
    amount,
    ex_value,
    created_at,
    win_rate,
    prio_score,
    approval_ranking,
    sum(ex_value) over (partition by jdi_wa_decision_table.app_id order by approval_ranking) as cum_sum,
    budget_left,
    case
        when prio_score = 1 and cum_sum < budget_left then 'approve'
        when cum_sum < budget_left then 'approve'
        else 'waitlist'
    end as status,
    case
        when prio_score = 1 and cum_sum < budget_left then 'Depositor with high deposit to withdraw ratio'
        when cum_sum < budget_left then 'First time withdrawer'
        else 'Daily budget is exhausted, the request is in the waitlist'
    end as reason
from {{ ref('jdi_wa_decision_table') }}
left join remaining_daily_budget on remaining_daily_budget.app_id = jdi_wa_decision_table.app_id
where status = 'approve'
