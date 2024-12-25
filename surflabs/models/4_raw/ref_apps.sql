{{
    config(
        materialized    = 'incremental',
        unique_key      = 'app_id',
        cluster_by      = 'app_created_at::date',
        tags            = ['reseed', 'ref', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

app as (
    select
        *
    from {{ ref('raw_apps') }}
    where app_id in {{ var_list('games') }}
),

company as (
    select
        *
    from {{ ref('raw_companies') }}
)

select
    app.app_id,
case app.app_id 

when '2WNr2yXBKfirIsZ1fZVjzKGk6oC' then 'Unicorn Bingo Cash'
when '2cHCxmLoPnWJjo2uFmWjMBFmpRE' then 'Grapes Bingo Cash'
when '2cHDj3KTMzcNvE6YYP5T0eOeYNw' then 'Unicorn Solitaire Cash'
when '2iox4YrMnxg1tm353csAeRe4FOY' then 'Unicorn Solitaire Free'
when '2cHDTN1GaEecLirrbTfelbugXkS' then 'Grapes Solitaire Cash'
when '2YcC9BKWmVV9s3ECpz9ZXxdh7kK' then 'Unicorn Trivia Cash'
when '2FLSyMNfgv8GMAmSgMj3oEnWObB' then 'Unicorn Bumper Corns Cash'
when '2FLUn422UdoHSsnn7O8tWx7bqLy' then 'Unicorn Mob Run Cash'
when '2KUv09iNJLtzSyD8KakbJeCYbDj' then 'Tezos Trivia Cash'

else app.app_name 

end as app_name,
    app.app_description,
    app.company_id,
    company.company_name,
    app.app_ios_id,
    app.app_android_id,
    app.app_sdk_api_key,
    app.app_created_at,
    app.app_updated_at,
    app.app_hard_currencies,
    app.app_flags,
    company.company_dev_api_key,
    company.company_created_at,
    company.company_updated_at,
    {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at,

case app_id 

when '2FLSyMNfgv8GMAmSgMj3oEnWObB' then 'laguna'
when '2FLUn422UdoHSsnn7O8tWx7bqLy' then 'laguna'
when '2KUv09iNJLtzSyD8KakbJeCYbDj' then 'tezos'
when '2WNr2yXBKfirIsZ1fZVjzKGk6oC' then 'laguna'
when '2YcC9BKWmVV9s3ECpz9ZXxdh7kK' then 'laguna'
when '2cHDj3KTMzcNvE6YYP5T0eOeYNw' then 'laguna'
when '2cHCxmLoPnWJjo2uFmWjMBFmpRE' then 'grapes'
when '2cHDTN1GaEecLirrbTfelbugXkS' then 'grapes'
when '2dj3bVyQPnukw9ypC0ktXPZz6mL' then 'laguna'
when '2fXaLwr2Am6USRiuSrtIzvQG3Fn' then 'grapes'
when '2cHDj3KTMzcNvE6YYP5T0eOeYNw' then 'laguna'
when '2ioxDEF5RrlbGJi4eQA6HfHBMRs' then 'grapes'
when '2iox4YrMnxg1tm353csAeRe4FOY' then 'laguna'
else 'zzzzzz' 

end as ts_rls,
    app.app_name as app_db_name
    from app
inner join company on app.company_id = company.company_id
    {% if is_incremental() -%}
    where app_updated_at > (select ifnull(max(app_updated_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
