{{
    config(
        materialized	= 'incremental',
        unique_key	= 'id',
        cluster_by	= ['user_id', 'match_id', 'created_at'],
        tags            = ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

game_play as (
select
        * exclude (GAME_ROOM_NAME, GAME_ROOM_FLAGS, GAME_ROOM_ORDER, GAME_ROOM_TYP)
    from {{ ref('raw_padme_game_play') }}
    {% if is_incremental() -%}

        where id in 
        (
        select 
            id
        from 
        {{ ref('raw_padme_game_play') }}
        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )

	or 

	match_id in 
        (
        select 
            match_id
        from 
        {{ ref('raw_padme_non_trivia_match')}}
        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )

	or 

	match_id in 
        (
        select 
            match_id
        from 
        {{ ref('raw_trivia_match')}}
        where inserted_at > (
                            select 
                                dateadd(hour,-2,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
        )

    {%- endif %}
),

tall_game_play as (
select
    game_play.* exclude (match_id, entry_fee, original_entry_fee),
	match_id,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_sc,
    case
        when left(parse_json(entry_fee)[0]:currency::string,1) = 'B' and right(parse_json(entry_fee)[0]:currency::string,len(parse_json(entry_fee)[0]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when left(parse_json(entry_fee)[1]:currency::string,1) = 'B' and right(parse_json(entry_fee)[1]:currency::string,len(parse_json(entry_fee)[1]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when left(parse_json(entry_fee)[2]:currency::string,1) = 'B' and right(parse_json(entry_fee)[2]:currency::string,len(parse_json(entry_fee)[2]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_bonus_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:amount::float
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:amount::float
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:amount::float
        else null
    end as entry_fee_amount_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('soft_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_sc,
    case
        when left(parse_json(entry_fee)[0]:currency::string,1) = 'B' and right(parse_json(entry_fee)[0]:currency::string,len(parse_json(entry_fee)[0]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when left(parse_json(entry_fee)[1]:currency::string,1) = 'B' and right(parse_json(entry_fee)[1]:currency::string,len(parse_json(entry_fee)[1]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when left(parse_json(entry_fee)[2]:currency::string,1) = 'B' and right(parse_json(entry_fee)[2]:currency::string,len(parse_json(entry_fee)[2]:currency::string)-1) in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_bonus_hc,
    case
        when parse_json(entry_fee)[0]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[0]:currency::string
        when parse_json(entry_fee)[1]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[1]:currency::string
        when parse_json(entry_fee)[2]:currency::string in {{ var_list('hard_currency_names') }} then parse_json(entry_fee)[2]:currency::string
        else null
    end as entry_fee_curr_hc,
    parse_json(original_entry_fee):currency::string as or_entry_fee_curr,
    parse_json(original_entry_fee):amount::float as or_entry_fee_amount,
    case
        when (entry_fee_curr_sc is not null) then 'soft'
        when ((entry_fee_curr_hc is not null) or (entry_fee_curr_bonus_hc is not null)) then 'hard'
        when ((entry_fee_curr_sc is not null) and ((entry_fee_curr_hc is not null) or (entry_fee_curr_bonus_hc is not null))) then 'both'
        else 'unknown'
    end as game_type,
    case
        when bitand(flags,1)!=0 then '01. started'
        when bitand(flags,2)!=0 then '02. cancelled'
        when bitand(flags,4)!=0 then '03. pending'
        when bitand(flags,8)!=0 then '04. finished'
        when bitand(flags,16)!=0 then '05. expired'
        else '99. unknown'
    end as game_status,
	case when bitand(flags,32)!=0 then 'fake' else 'normal' end is_fake
    from game_play
),

non_trivia_match as (
  select distinct match_id, game_state, is_fake, game_status_cnt from {{ ref('raw_padme_non_trivia_match')}}  
),

non_trivia_extd_game_play as (
  select tall_game_play.* exclude (position),
    case
        when game_status_cnt = 1 and left(game_status,2) in ('03','04','05') then timestampdiff(seconds, created_at, ended_at) else null
    end as playtime_seconds,
    case
        when game_status_cnt = 1 and left(game_status,2) in ('04') then 
	case when date(created_at) >= '2024-07-16' and app_id in ('2cHCxmLoPnWJjo2uFmWjMBFmpRE','2cHDTN1GaEecLirrbTfelbugXkS','2fXaLwr2Am6USRiuSrtIzvQG3Fn','2ioxDEF5RrlbGJi4eQA6HfHBMRs') and position = 0 and won = 0 then 1 else position end 
--	position 
	else null
    end as position_clean,
    case   
        when game_status_cnt > 1 then 'multiple_game_status_in_progress' 
        when (game_status_cnt = 1 or game_status_cnt is null) and left(game_status,2) = '01' then 'game_in_progress'
        when (game_status_cnt = 1 or game_status_cnt is null) and left(game_status,2) = '02' then 'refund'
        when game_status_cnt = 1 and left(game_status,2) = '03' then 'match_in_progress'
        when game_status_cnt = 1 and left(game_status,2) = '04' then 'win'
        when game_status_cnt = 1 and left(game_status,2) = '05' then 'refund'
    else 'unknown'
    end
    as payout_status
    from tall_game_play
    left join non_trivia_match
    on tall_game_play.match_id = non_trivia_match.match_id 
    and tall_game_play.game_status = non_trivia_match.game_state
    and tall_game_play.is_fake = non_trivia_match.is_fake
	where app_id not in {{ var_list('trivia_games') }}
),

non_trivia_non_finished_match_processed as (
select 
    * ,
    null as prize_position_curr,
--    null as prize_position_json_index,
    null as prize_position_amount
    from 
non_trivia_extd_game_play where payout_status not in ('win')
),

non_trivia_finished_match as (
select 
    *
    from
non_trivia_extd_game_play where payout_status in ('win')
),

non_trivia_finished_match_flatten as (
select 
    non_trivia_finished_match.*, 
    parse_json(prize_pool):currency::string as prize_position_curr,
    prize_position_json.index as prize_position_json_index,
    prize_position_json.value::float as prize_position_amount,
    ROW_NUMBER() OVER (PARTITION BY id, position_clean ORDER BY CASE WHEN prize_position_json.index = position_clean THEN 0 ELSE 1 END, prize_position_json.index) AS rn
    FROM 
    non_trivia_finished_match, lateral flatten(INPUT => parse_json(prize_pool):rewards, outer => true) prize_position_json
),   
    
non_trivia_finished_match_processed as (
select * exclude (payout_status, prize_position_curr, prize_position_json_index, prize_position_amount, rn),
    CASE payout_status 
	when 'win' then
		case WHEN prize_position_json_index = position_clean then 'win' else 'no_win' end 
	end as payout_status, 
    CASE WHEN prize_position_json_index = position_clean then prize_position_curr else null end as prize_position_cur, 
    CASE WHEN prize_position_json_index = position_clean then prize_position_amount else null end as prize_position_amount
    from
    non_trivia_finished_match_flatten where rn = 1
),

trivia_match as (
  select distinct match_id, 
    match_state,
	case
   		when match_state is null then 'no_match_found'
   		when match_state = -1 then 'multiple_game_status_in_progress'
		when match_state = 1 then 'Open (joinable)'
    		when match_state = 2 then 'In progress'
        	when match_state = 3 then 'Finished'
		when match_state = 4 then 'Cancelled timeout'
		when match_state = 5 then 'Cancelled tie'
    end as match_state_desc,
	match_status_cnt	
	from 
	(
	select distinct a.match_id, 
	case 
		when match_status_cnt > 1 then -1 
		when match_status_cnt = 1 then status 
		else null 
	end as match_state,
	match_status_cnt
	from {{ ref('raw_trivia_match')}} a
	left join 
	(
	select match_id, count(distinct status) match_status_cnt from 
	( select distinct match_id, status from {{ ref('raw_trivia_match')}} ) group by 1
	) b
	on a.match_id = b.match_id
	)
),

trivia_extd_game_play as (
  select tall_game_play.* exclude (position),
    case
        when match_state = 3 then timestampdiff(seconds, created_at, ended_at) else null
    end as playtime_seconds,
    case
        when match_state = 3 then position else null
    end as position_clean,
    case   
        when match_state is null then 'no_match_found' 
        when match_state = -1 then 'multiple_game_status_in_progress' 
        when match_state = 1 then 'game_in_progress'
        when match_state = 2 then 'match_in_progress'
        when match_state = 3 then 'win'
        when match_state = 4 then 'refund'
        when match_state = 5 then 'refund'
    else 'unknown'
    end
    as payout_status
    from tall_game_play
    left join trivia_match
    on tall_game_play.match_id = trivia_match.match_id 
	where app_id in {{ var_list('trivia_games') }}
),

trivia_non_finished_match_processed as (
select 
    * ,
    null as prize_position_curr,
--    null as prize_position_json_index,
    null as prize_position_amount
    from 
trivia_extd_game_play where payout_status <> 'win'
),

trivia_finished_match as (
select 
    *
    from
trivia_extd_game_play where payout_status = 'win'
),

trivia_finished_match_flatten as (
select 
    trivia_finished_match.*, 
    parse_json(prize_pool):currency::string as prize_position_curr,
    prize_position_json.index as prize_position_json_index,
    prize_position_json.value::float as prize_position_amount,
    ROW_NUMBER() OVER (PARTITION BY id, position_clean ORDER BY CASE WHEN prize_position_json.index = position_clean THEN 0 ELSE 1 END, prize_position_json.index) AS rn
    FROM 
    trivia_finished_match, lateral flatten(INPUT => parse_json(prize_pool):rewards, outer => true) prize_position_json
),   
    
trivia_finished_match_processed as (
select * exclude (payout_status, prize_position_curr, prize_position_json_index, prize_position_amount, rn),
    CASE WHEN prize_position_json_index = position_clean then 'win' else 'no_win' end as payout_status, 
    CASE WHEN prize_position_json_index = position_clean then prize_position_curr else null end as prize_position_cur, 
    CASE WHEN prize_position_json_index = position_clean then prize_position_amount else null end as prize_position_amount
    from
    trivia_finished_match_flatten where rn = 1
)






select * from non_trivia_non_finished_match_processed
    union all
select * from non_trivia_finished_match_processed
	UNION ALL
select * from trivia_non_finished_match_processed
    union all
select * from trivia_finished_match_processed
