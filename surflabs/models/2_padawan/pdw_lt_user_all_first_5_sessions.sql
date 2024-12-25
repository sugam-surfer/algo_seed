{{
    config(
        materialized    = 'incremental',
        unique_key      = ['user_id','session_number'],
        cluster_by      = 'user_id',
        tags		= ['events', 'reseed', 'warehouse'],
    )
}}

with

new_users as
(
	select 
            user_id cug
        from 
        {{ ref('pdw_ae_sessions') }}
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
	rank() over(partition by user_id order by SESSION_START_TIMESTAMP) as session_number
    from
    {{ ref('pdw_ae_sessions') }}
	
    {% if is_incremental() %}
        where user_id in 
        (
        select 
            new_users.cug
        from 
        new_users 
	left join 
	( select cug from
	( 
	select user_id cug, max(session_number) mgn from {{ this }}
	{{ dbt_utils.group_by(1) }}
	) x where mgn >= 5 ) a 
	on new_users.cug = a.cug and a.cug is null
        )
    {% endif %}
)
	
select * from out_put where session_number <= 5
