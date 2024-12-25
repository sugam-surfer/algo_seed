{{
    config(
        materialized	= 'incremental',
        unique_key      = ['user_id','txn_number'],
        cluster_by      = ['user_id','txn_number'],
        tags		= ['reseed', 'warehouse'],
    )
}}

with
base as (
select * from {{ ref('stg_user_balance_log') }} where reason_category = 'deposit'
),
	
inc_user_id as
(
	select 
            distinct user_id
        from 
	base
	where inserted_at >= (
                            select 
                                dateadd(day,-1,ifnull(max(inserted_at),'1900-01-01 00:00:00')) 
                            from 
                            {{ this }}
                            )
),

out_put as 
(   
    select 
	*,
	rank() over(partition by user_id order by created_at) as txn_number
    from
    base
	
    {% if is_incremental() %}
        where user_id in 
        (
        select 
            inc_user_id.user_id
        from 
        inc_user_id 
	left join 
	( select user_id from
	( 
	select user_id, max(txn_number) mtn from {{ this }}
	where reason_category = 'deposit'
	{{ dbt_utils.group_by(1) }}
	) x where mtn >= 5 ) a 
	on inc_user_id.user_id = a.user_id and a.user_id is null
        )
    {% endif %}

)
	
select * from out_put where txn_number <= 5
