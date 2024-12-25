{{
    config(
        materialized    = 'incremental',
        unique_key      = ['user_id','game_type','game_number'],
        cluster_by      = 'user_id',
        tags		= ['reseed', 'warehouse'],
    )
}}

with

new_users_game_type as
(
	select 
            concat(user_id, game_type) cug
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
	rank() over(partition by user_id, game_type order by created_at desc) as game_number
    from
    {{ ref('stg_padme_game_play') }}
	
    {% if is_incremental() %}
        where concat(user_id, game_type) in 
        (
        select 
            new_users_game_type.cug
        from 
        new_users_game_type 
	)
    {% endif %}
)
	
select * from out_put where game_number = 1
