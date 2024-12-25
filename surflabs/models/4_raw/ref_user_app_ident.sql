{{
    config(
        materialized    = 'incremental',
        unique_key      = 'user_id',
        tags            = ['reseed', 'ref', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

raw_user_inc as (
    select * 
    from {{ ref('raw_user') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),

raw_user_ident_inc as (
    select * 
    from {{ ref('raw_user_ident') }} 
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
),  

table_1 as (
select raw_user.id as user_id, raw_user.app_id, raw_user_ident_inc.ident_value, raw_user.inserted_at 
from
{{ ref('raw_user') }} raw_user
left outer join
raw_user_ident_inc
on raw_user.id = raw_user_ident_inc.user_id
    where raw_user_ident_inc.user_id is not null
),

table_2 as (    
select raw_user_inc.id as user_id, raw_user_inc.app_id, raw_user_ident.ident_value, raw_user_inc.inserted_at 
from
raw_user_inc
left outer join
{{ ref('raw_user_ident') }} raw_user_ident
on raw_user_inc.id = raw_user_ident.user_id
),

table_final as (
select * from table_1 
    union all 
select * from table_2
)    

select distinct * from table_final
