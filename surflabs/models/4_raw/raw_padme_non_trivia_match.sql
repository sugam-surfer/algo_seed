{{
    config(
        materialized	= 'incremental',
        unique_key	= 'match_id',
        cluster_by	= 'inserted_at::date',
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}


with

game_play as (
select
    match_id,
    case
    	when bitand(flags,1)!=0 then '01. started'
       	when bitand(flags,2)!=0 then '02. cancelled'
	when bitand(flags,4)!=0 then '03. pending'
	when bitand(flags,8)!=0 then '04. finished'
	when bitand(flags,16)!=0 then '05. expired'
	else '99. unknown'
    end as game_status,
    case when bitand(flags,32)!=0 then 'fake' else 'normal' end is_fake,
    ended_at,
    INSERTED_AT 
from {{ ref('raw_padme_game_play') }} 
{% if is_incremental() -%}
where match_id in 
    (
        select match_id from {{ ref('raw_padme_game_play') }}
        where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    )
and app_id not in {{ var_list('trivia_games') }}
{%- endif %}
),

DIST_GAME_PLAY AS (
select distinct match_id, game_status, is_fake, ended_at 
from game_play 
),

game_status_cnt as (
select match_id, is_fake, count(DISTINCT GAME_STATUS) as game_status_cnt, max(ended_at) as match_ended_at
from dist_game_play {{ dbt_utils.group_by(2) }}
),

joiner as (
select 
dist_game_play.match_id,
dist_game_play.game_status,
dist_game_play.is_fake,
game_status_cnt.game_status_cnt,
game_status_cnt.match_ended_at
from
dist_game_play
left join 
game_status_cnt
on dist_game_play.match_id = game_status_cnt.match_id
and dist_game_play.is_fake = game_status_cnt.is_fake
)

select 
game_play.match_id,
game_play.game_status game_state,
game_play.is_fake,
joiner.game_status_cnt,
joiner.match_ended_at,
max(game_play.inserted_at) inserted_at
from 
game_play
left join 
joiner
on 
game_play.match_id = joiner.match_id and 
game_play.game_status = joiner.game_status and
game_play.is_fake = joiner.is_fake
{{ dbt_utils.group_by(5) }}




