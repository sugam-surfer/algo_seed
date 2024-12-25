{{
    config(
        materialized    = 'incremental',
        unique_key      = ['user_id','game_number'],
        cluster_by      = 'user_id',
        tags		= ['reseed', 'warehouse'],
    )
}}

with

new_users as
(
	select 
            user_id cug
        from 
        {{ ref('stg_padme_game_play') }}
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
	rank() over(partition by user_id order by created_at desc) as game_number
    from
    {{ ref('stg_padme_game_play') }}
	
    {% if is_incremental() %}
        where user_id in 
        (
        select 
            new_users.cug
        from 
        new_users 
	)
    {% endif %}
)
	
select * from out_put where game_number = 1
