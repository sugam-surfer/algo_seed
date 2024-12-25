{{
    config(
        materialized = 'table',
        tags         = ['withdrawal_automation', 'warehouse'],
    )
}}


with

score_differences as (
    select
        app_id,
        game_type,
        room_id,
        or_entry_fee_amount,
        match_id,
        abs(max(score) - min(score)) as score_difference
    from {{ ref('stg_padme_game_play') }}
    where game_status like '04%'
        and is_fake = 'normal'
    {{ dbt_utils.group_by(5) }}
),

filtered_matches as (
    select
        score_differences.game_type,
        score_differences.or_entry_fee_amount,
        match_id,
        score_difference
    from score_differences
    left join {{ ref('jdi_wa_game_score_diff') }} on jdi_wa_game_score_diff.app_id = score_differences.app_id
        and jdi_wa_game_score_diff.room_id = score_differences.room_id
    where score_difference > jdi_wa_game_score_diff.upper_bound
)

select
    p.app_id,
    f.game_type,
    f.or_entry_fee_amount,
    f.match_id,
    p.user_id,
    p.score,
    f.score_difference
from filtered_matches f
join {{ ref('stg_padme_game_play') }} p on f.match_id = p.match_id
