{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

select * exclude (cpl) from {{ ref('stg_attr_dummy_user_with_cpl') }} 
