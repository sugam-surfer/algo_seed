{{
    config(
        materialized    = 'incremental',
        unique_key      = 'sdk_replay_id',
        cluster_by      = ['ts::date', 'sdk_replay_id'],
	post_hook    	= ["delete from ae.events where app_id not in ('2cHCxmLoPnWJjo2uFmWjMBFmpRE','2cHDTN1GaEecLirrbTfelbugXkS','2fXaLwr2Am6USRiuSrtIzvQG3Fn','2ioxDEF5RrlbGJi4eQA6HfHBMRs')",
			   "delete from ae.event_ids where app_id not in ('2cHCxmLoPnWJjo2uFmWjMBFmpRE','2cHDTN1GaEecLirrbTfelbugXkS','2fXaLwr2Am6USRiuSrtIzvQG3Fn','2ioxDEF5RrlbGJi4eQA6HfHBMRs')",
			   "delete from ae.event_replays where app_id not in ('2cHCxmLoPnWJjo2uFmWjMBFmpRE','2cHDTN1GaEecLirrbTfelbugXkS','2fXaLwr2Am6USRiuSrtIzvQG3Fn','2ioxDEF5RrlbGJi4eQA6HfHBMRs')"],
        tags            = ['events', 'warehouse'],
    )
}}

{{ incremental_message() }}

with

events as (
    select
        * exclude _tsdate,
        {{ dbt_date.convert_timezone(current_timestamp(), source_tz="UTC") }} as inserted_at
    from {{ source('ae', 'events') }}
     where 
        {% if is_incremental() -%}
        _ts > (
                select 
                    dateadd(minute,-150,ifnull(max(_ts),'1900-01-01 00:00:00')) 
                from {{ this }}
        ) and
        data:_replay_id::string not in (select sdk_replay_id from {{ this }})
        {%- endif %}
--        data:_replay_id::string not in (select sdk_replay_id from {{ this }})
--        and _ts in ('2024-04-24','2024-04-25','2024-04-26','2024-05-08','2024-05-09','2024-05-16','2024-05-22','2024-05-23','2024-06-11','2024-06-17','2024-06-22','2024-06-24')
        and app_id in {{ var_list('games') }}
        and data:format in {{ var_list('data_formats') }}
)

select
    *,
    data:_replay_id::string as sdk_replay_id
from events
