{{
    config(
        materialized    = 'incremental',
        unique_key      = 'user_id',
        cluster_by      = 'created_at::date',
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

user_app_data as (
    select
        raw_user_app_data.* exclude data,
        parse_json(data):n::string as user_name,
        parse_json(data):p::string as profile_picture,
        parse_json(data):d::string as data,
    
parse_json(parse_json(data):d::string):laguna_leaderboard::string laguna_leaderboard,
parse_json(parse_json(data):d::string):laguna_leaderboard:is_connected::boolean is_connected,
parse_json(parse_json(data):d::string):laguna_leaderboard:lb_wallet[0]:address::string lb_wallet_address,
parse_json(parse_json(data):d::string):laguna_leaderboard:lb_wallet[0]:chain::string lb_wallet_chain,
parse_json(parse_json(data):d::string):laguna_leaderboard:lb_wallet[0]:is_active::boolean lb_wallet_is_active,

case
left(parse_json(parse_json(data):d::string):laguna_leaderboard:lb_wallet[0]:time::string,10)
when '25-05-2024' then '2024-05-25'
when '28-05-2024' then '2024-05-28'
when '25-5-2024 ' then '2024-05-25'
when '28.05.2024' then '2024-05-28'
when '5/24/2024 ' then '2024-05-24'
when '5/25/2024 ' then '2024-05-25'
when '5/27/2024 ' then '2024-05-27'
when '5/28/2024 ' then '2024-05-28'
when '5/30/2024 ' then '2024-05-30'
else 
to_timestamp_ntz(
left(parse_json(parse_json(data):d::string):laguna_leaderboard:lb_wallet[0]:time::string,10)
, 'DD/MM/YYYY') 
end as lb_wallet_date

    from {{ ref('raw_user_app_data') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)

select
    *
from user_app_data
