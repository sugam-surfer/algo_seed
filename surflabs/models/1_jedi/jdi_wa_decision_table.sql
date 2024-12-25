{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}

with

user_win_rates as (
    select *
    from {{ ref('jdi_wa_user_win_rates') }}
    where jdi_wa_user_win_rates.is_fake = 'normal'
        and jdi_wa_user_win_rates.game_type = 'hard'
),

game_win_rates as (
    select *
    from {{ ref('jdi_wa_game_win_rates') }}
    where jdi_wa_game_win_rates.game_type = 'hard'
        and jdi_wa_game_win_rates.is_fake = 'normal'
),

shared_address_flag as (
    select
        ident_value,
        app_id,
        true as shared_address
    from {{ ref('jdi_wa_shared_address_flag') }}
    {{ dbt_utils.group_by(2) }}
),

abnormal_score_diff_flag as (
    select
        user_id,
        count(*) as abn_matches,
        true as abnormal_score_diff
    from {{ ref('jdi_wa_user_score_diff') }}
    where game_type = 'hard'
        and or_entry_fee_amount > {{ var('lowest_entry_fee_for_abnormal_score_diff') }}
    {{ dbt_utils.group_by(1) }}
    having abn_matches > {{ var('min_games_per_user_for_abnormal_score_diff') }}
)

select
    jdi_wa_pending_withdrawals.*,
    case
        when
            dayname(created_at) in ('Sat', 'Sun')
                then
                    timediff(hour, dateadd(day, (8 - dayofweek(created_at)) % 7, cast(created_at as date)), convert_timezone('America/Los_Angeles', 'UTC',current_timestamp))
                     - (datediff(week, created_at, convert_timezone('America/Los_Angeles', 'UTC',current_timestamp)) - 1) * 48
        else
            datediff(hour, created_at, convert_timezone('America/Los_Angeles', 'UTC',current_timestamp))
            - (
                datediff(week, created_at, convert_timezone('America/Los_Angeles', 'UTC',current_timestamp)) * 48
            )
    end as working_hours_since_request,
    case
        when ifnull(deposits_ex_value,0) > 0 then true
        else false
    end as is_depositor,
    case
        when jdi_wa_prev_withdrawers.user_id is not null then true
        else false
    end as prev_withdrawer,
    deposits_ex_value,
    withdrawals_ex_value,
    deposits_ex_value / (withdrawals_ex_value + jdi_wa_pending_withdrawals.ex_value) as deposit_withdrawal_ratio,
    user_win_rates.total_games,
    jdi_wa_avg_usergames.games_per_day,
    win_rate,
    ci_low,
    ci_high,
    case
        when jdi_wa_ident_value.override_reason is not null then jdi_wa_ident_value.override_reason
        else 'none'
    end as override_reason,
    case
        when jdi_wa_mult_user_device.user_id is not null then jdi_wa_mult_user_device.num_other_user_ids
        else 0
    end as num_other_users,
    coalesce(shared_address_flag.shared_address, false) as shared_address,
    abn_matches,
    coalesce(abnormal_score_diff_flag.abnormal_score_diff, false) as abnormal_score_diff,
    case
        when override_reason != 'none' then override_status
--        when is_depositor and abnormal_score_diff then 'confiscate'
--        when is_depositor and shared_address then 'confiscate'
--        when num_other_users >= {{ var('min_num_other_user_ids') }} then 'confiscate'
        when not is_depositor then 'reject'
        when num_other_users < {{ var('min_num_other_user_ids') }} and deposit_withdrawal_ratio > {{ var('deposit_withdrawal_ratio_for_manual_review') }} then 'approve'
        when is_depositor and working_hours_since_request > {{ var('approve_after_working_hours_since_request') }} then 'approve'
        when is_depositor then 'review'
--        else 'hold'
        else 'approve'
    end as status,
    case
        when override_reason != 'none' then override_prio_score
--        when is_depositor and abnormal_score_diff then '99999999'
--        when is_depositor and shared_address then '99999999'
--        when num_other_users >= {{ var('min_num_other_user_ids') }} then '99999999'
        when not is_depositor then '99999999'
        when num_other_users < {{ var('min_num_other_user_ids') }} and deposit_withdrawal_ratio > {{ var('deposit_withdrawal_ratio_for_manual_review') }} then '1'
        when is_depositor and working_hours_since_request > {{ var('approve_after_working_hours_since_request') }} then '2'
        when is_depositor then '99999999'
--        else '99999999'
        else '2'
    end as prio_score,
    case
        when override_reason != 'none' then override_reason
--        when is_depositor and abnormal_score_diff then 'Abnormal score differences'
--        when is_depositor and shared_address then 'Depositor sharing nominated address with others'
--        when num_other_users >= {{ var('min_num_other_user_ids') }} then 'Multiple users on the same device'
        when not is_depositor then 'Non-depositor'
        when num_other_users < {{ var('min_num_other_user_ids') }} and deposit_withdrawal_ratio > {{ var('deposit_withdrawal_ratio_for_manual_review') }} then 'Depositor to approve'
        when is_depositor and working_hours_since_request > {{ var('approve_after_working_hours_since_request') }} then 'Approve after 48 hours of investigation'
        when is_depositor and num_other_user_ids >= {{ var('min_num_other_user_ids') }} then 'Depositor with multiple users on the same device'
--        when deposit_withdrawal_ratio <= {{ var('deposit_withdrawal_ratio_for_manual_review') }} then 'Depositor with low deposit to withdraw ratio'
        else 'no issues found'
    end as reason,
    case
        when status = 'approve' then rank() over (partition by jdi_wa_pending_withdrawals.app_id, prio_score order by win_rate)
        else '99999999'
    end as approval_ranking
from {{ ref('jdi_wa_pending_withdrawals') }}
left join ahsokatano_1_jedi.jdi_wa_ident_value on jdi_wa_ident_value.ident_value = jdi_wa_pending_withdrawals.ident_value
left join {{ ref('jdi_wa_depositors') }} on jdi_wa_depositors.ident_value = jdi_wa_pending_withdrawals.ident_value
    and jdi_wa_depositors.app_id = jdi_wa_pending_withdrawals.app_id
left join {{ ref('jdi_wa_prev_withdrawers') }} on jdi_wa_prev_withdrawers.user_id = jdi_wa_pending_withdrawals.user_id
left join user_win_rates on user_win_rates.user_id = jdi_wa_pending_withdrawals.user_id
left join game_win_rates on game_win_rates.app_id = jdi_wa_pending_withdrawals.app_id
left join {{ ref('jdi_wa_mult_user_device') }} on jdi_wa_mult_user_device.user_id = jdi_wa_pending_withdrawals.user_id
left join {{ ref('jdi_wa_avg_usergames') }} on jdi_wa_avg_usergames.user_id = jdi_wa_pending_withdrawals.user_id
left join shared_address_flag on shared_address_flag.ident_value = jdi_wa_pending_withdrawals.ident_value
    and shared_address_flag.app_id = jdi_wa_pending_withdrawals.app_id
left join abnormal_score_diff_flag on abnormal_score_diff_flag.user_id = jdi_wa_pending_withdrawals.user_id
