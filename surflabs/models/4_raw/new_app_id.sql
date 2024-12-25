{{
    config(
        materialized    = 'table',
        unique_key      = 'app_id',
        tags            = 'warehouse',

    )
}}


select distinct app_id from {{ source('padme','game_play') }}
where app_id not in {{ var_list('games') }} or app_id not in
(
'2FLSyMNfgv8GMAmSgMj3oEnWObB',
'2FLUn422UdoHSsnn7O8tWx7bqLy',
'2KUv09iNJLtzSyD8KakbJeCYbDj',
'2WNr2yXBKfirIsZ1fZVjzKGk6oC',
'2YcC9BKWmVV9s3ECpz9ZXxdh7kK',
'2cHDj3KTMzcNvE6YYP5T0eOeYNw',
'2dj3bVyQPnukw9ypC0ktXPZz6mL',
'2iox4YrMnxg1tm353csAeRe4FOY'
)
