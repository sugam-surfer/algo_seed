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
	rank() over(partition by user_id order by SESSION_START_TIMESTAMP desc) as session_number
    from
    {{ ref('pdw_ae_sessions') }}
	
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
	
select * from out_put where session_number = 1
