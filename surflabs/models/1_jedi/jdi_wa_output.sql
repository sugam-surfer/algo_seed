{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

select
    jdi_wa_approvals.id,
    jdi_wa_approvals.ident_type,
    jdi_wa_approvals.ident_value,
    jdi_wa_approvals.user_id,
    friendly_name,
    nominated_address,
    app_id,
    currency,
    amount,
    jdi_wa_approvals.created_at,
    status,
    reason
from {{ ref('jdi_wa_approvals') }}
left join {{ ref('raw_user_ident') }} on raw_user_ident.user_id = jdi_wa_approvals.user_id
where status = 'approve'

union all

select
    jdi_wa_decision_table.id,
    jdi_wa_decision_table.ident_type,
    jdi_wa_decision_table.ident_value,
    jdi_wa_decision_table.user_id,
    friendly_name,
    nominated_address,
    app_id,
    currency,
    amount,
    jdi_wa_decision_table.created_at,
    status,
    reason
from {{ ref('jdi_wa_decision_table') }}
left join {{ ref('raw_user_ident') }} on raw_user_ident.user_id = jdi_wa_decision_table.user_id
where status in ('confiscate', 'reject')
