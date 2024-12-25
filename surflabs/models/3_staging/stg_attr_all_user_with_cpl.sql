{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select 
    a.*,
    b.* exclude (id, created_at, ATTR_AF_STATUS, ATTR_MEDIA_SOURCE, ATTR_AF_CHANNEL, ATTR_CAMPAIGN_NAME, ATTR_CAMPAIGN_ID, ATTR_AF_ADSET_ID, app_id, inserted_at, attr_install_time), 
    ifnull(b.app_id,ref_apps.app_id) as app_id, b.inserted_at, b.attr_install_time  
from 
(
select * from {{ ref('stg_attr_user_with_cpl')}}
union all
select * from {{ ref('stg_attr_dummy_user_with_cpl')}}
) a
left join 
{{ ref('stg_user')}} b
on a.user_id = b.id
left join 
{{ ref('ref_apps') }}
on a.app_name = ref_apps.app_name


