{{
    config(
        materialized	= 'incremental',
        unique_key	= ['id','currency_type','currency','transaction_type'],
        cluster_by	= 'created_at::date',
        tags		= ['reseed', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

stg_game_play as (
    select
        * exclude (claimed, house_cut, score, prize_pool, validated, extra),
    from {{ ref('stg_padme_game_play') }}
    {% if is_incremental() -%}
    where inserted_at > (select ifnull(max(inserted_at),'1900-01-01 00:00:00') from {{ this }})
    {%- endif %}
)


select
    * exclude (entry_fee_curr_sc, entry_fee_amount_sc, entry_fee_curr_hc, entry_fee_amount_hc, entry_fee_curr_bonus_hc, entry_fee_amount_bonus_hc, PRIZE_POSITION_CURR, PRIZE_POSITION_AMOUNT),
    case 
	when entry_fee_curr_sc is not null then 'sc' 
	when entry_fee_curr_hc is not null then 'hc' 
	when entry_fee_curr_bonus_hc is not null then 'bonus_hc' 
    end as currency_type,
    case 
	when entry_fee_curr_sc is not null then entry_fee_curr_sc 
	when entry_fee_curr_hc is not null then entry_fee_curr_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_curr_bonus_hc 
    end as currency,
    case 
	when entry_fee_curr_sc is not null then entry_fee_amount_sc 
	when entry_fee_curr_hc is not null then entry_fee_amount_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_amount_bonus_hc 
    end as amount,
	'entry_fee' transaction_type,
	created_at transaction_ts
from stg_game_play
where (entry_fee_curr_sc is not null or entry_fee_curr_hc is not null or entry_fee_curr_bonus_hc is not null)

union all

select
    * exclude (entry_fee_curr_sc, entry_fee_amount_sc, entry_fee_curr_hc, entry_fee_amount_hc, entry_fee_curr_bonus_hc, entry_fee_amount_bonus_hc, PRIZE_POSITION_CURR, PRIZE_POSITION_AMOUNT),
    case 
	when entry_fee_curr_sc is not null then 'sc' 
	when entry_fee_curr_hc is not null then 'hc' 
	when entry_fee_curr_bonus_hc is not null then 'bonus_hc' 
    end as currency_type,
    case 
	when entry_fee_curr_sc is not null then entry_fee_curr_sc 
	when entry_fee_curr_hc is not null then entry_fee_curr_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_curr_bonus_hc 
    end as currency,
    case 
	when entry_fee_curr_sc is not null then entry_fee_amount_sc 
	when entry_fee_curr_hc is not null then entry_fee_amount_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_amount_bonus_hc 
    end as amount,
	'refund' transaction_type,
	updated_at transaction_ts
from stg_game_play
where (entry_fee_curr_sc is not null or entry_fee_curr_hc is not null or entry_fee_curr_bonus_hc is not null)
and payout_status = 'refund'

union all

select
    * exclude (entry_fee_curr_sc, entry_fee_amount_sc, entry_fee_curr_hc, entry_fee_amount_hc, entry_fee_curr_bonus_hc, entry_fee_amount_bonus_hc, PRIZE_POSITION_CURR, PRIZE_POSITION_AMOUNT),
    case 
	when entry_fee_curr_sc is not null then 'sc' 
	when entry_fee_curr_hc is not null then 'hc' 
	when entry_fee_curr_bonus_hc is not null then 'bonus_hc' 
    end as currency_type,
    case 
	when entry_fee_curr_sc is not null then entry_fee_curr_sc 
	when entry_fee_curr_hc is not null then entry_fee_curr_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_curr_bonus_hc 
    end as currency,
    case 
	when entry_fee_curr_sc is not null then entry_fee_amount_sc 
	when entry_fee_curr_hc is not null then entry_fee_amount_hc 
	when entry_fee_curr_bonus_hc is not null then entry_fee_amount_bonus_hc 
    end as amount,
	'manual_refund' transaction_type,
	updated_at transaction_ts
from stg_game_play
where (entry_fee_curr_sc is not null or entry_fee_curr_hc is not null or entry_fee_curr_bonus_hc is not null)
and payout_status = 'manual_refund'

union all

select
    * exclude (entry_fee_curr_sc, entry_fee_amount_sc, entry_fee_curr_hc, entry_fee_amount_hc, entry_fee_curr_bonus_hc, entry_fee_amount_bonus_hc, PRIZE_POSITION_CURR, PRIZE_POSITION_AMOUNT),
    case 
        when PRIZE_POSITION_CURR in {{ var_list('soft_currency_names') }} then 'sc'
	when left(PRIZE_POSITION_CURR,1) = 'B' and right(PRIZE_POSITION_CURR,len(PRIZE_POSITION_CURR)-1) in {{ var_list('hard_currency_names') }} then 'bonus_hc'
        when PRIZE_POSITION_CURR in {{ var_list('hard_currency_names') }} then 'hc'
	else null 
	end as currency_type,
	PRIZE_POSITION_CURR currency,
	PRIZE_POSITION_AMOUNT amount,
	'win' transaction_type,
	updated_at transaction_ts
from stg_game_play
where PRIZE_POSITION_CURR is not null
and payout_status in ('win')


