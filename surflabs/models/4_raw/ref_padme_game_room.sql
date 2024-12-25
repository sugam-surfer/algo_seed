{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        tags            = ['reseed', 'ref', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

game_room as (
select
        *
    from {{ ref('raw_padme_game_room') }}
    {% if is_incremental() -%}

        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
    {%- endif %}
),

tall_game_room as (
select 
  *,
  parse_json(entry_fee):currency::string entry_fee_currency, 
  parse_json(entry_fee):amount::float entry_fee_amount, 
  parse_json(prize_pool):currency::string prize_pool_currency, 
  (
    ifnull(parse_json(prize_pool):rewards[0]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[1]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[2]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[3]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[4]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[5]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[6]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[7]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[8]::float,0) + 
    ifnull(parse_json(prize_pool):rewards[9]::float,0)
  ) prize_pool_amount
    from game_room
)

select
    * ,
  concat(max_players,'P - [E:',entry_fee_currency,':',entry_fee_amount,'] [P:',prize_pool_currency,':',prize_pool_amount,']') table_desc,
    (floor(floor(flags / 2) / 2) % 2) is_fake_allowed_table, 
    (floor(flags / 2) % 2) is_non_paying_user_table, 
    (flags % 2) is_deleted_table
from tall_game_room
