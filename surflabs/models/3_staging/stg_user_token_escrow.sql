{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
        tags            = ['sch_4hrs', 'sch_daily', 'crypto_unicorns', 'reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

select
    raw_user_token_escrow.*,
    ref_apps.app_name,
    case
        when created_at >= '2023-06-06 16:00:00.000' then true
        else false
    end as bp_launch
from {{ ref('raw_user_token_escrow') }}
left join {{ ref('ref_apps') }} ref_apps
    on raw_user_token_escrow.app_id = ref_apps.app_id
{% if is_incremental() -%}
where updated_at > (select max(updated_at) from {{ this }})
{%- endif %}
