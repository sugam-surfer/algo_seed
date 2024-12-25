{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
      	post_hook       = ["delete from ahsokatano_4_raw.check_hansolo_trivia_transactions where dest_table_type = 'dest'"],
        tags            = ['on_chain', 'trivia', 'reseed', 'warehouse'],
    )
}}

with

app_id_dummy_user as 
(
    select app_id, concat('dummy_',mdun) as user_id from 
    (
        select app_id, min(dummy_user_number) mdun from 
        (
            select app_id, right(user_id,len(user_id)-6) dummy_user_number, user_id 
            from {{ ref('stg_attr_all_user_with_cpl') }} 
            where left(user_id,5) = 'dummy' 
        )
        group by 1 
    )
),

base as 
(
    select a.* exclude (issue_type, dest_table_type, user_id, inserted_at), 
    ifnull(ifnull(ref_user_app_ident.user_id, app_id_dummy_user.user_id), 'NA') as user_id,
    a.inserted_at
    from {{ ref('check_hansolo_trivia_transactions') }} a
    left join
    {{ ref('ref_user_app_ident') }} ref_user_app_ident
    on a.ident_value = ref_user_app_ident.ident_value and a.app_id = ref_user_app_ident.app_id
    left join
    app_id_dummy_user
    on a.app_id = app_id_dummy_user.app_id
    where a.dest_table_type = 'dest'
),

id_cnt as (
select id, count(1) cnt from base group by 1 order by 2 desc,1 desc
),

bal_log as (
SELECT distinct user_id, app_id, reason_category
from {{ ref('raw_user_balance_log') }} raw_user_balance_log
left join ahsokatano_4_raw.ref_transaction_reason
on raw_user_balance_log.reason = ref_transaction_reason.reason
where reason_category in ('deposit','withdraw')
),

id_equal_1 as (
select base.* 
from base 
where id in (select id from id_cnt where cnt = 1)
),

id_more_1_bal_log as (
select base.*, 
bal_log.user_id as bal_log_user_id, 
bal_log.app_id as bal_log_app_id, 
bal_log.reason_category as bal_log_reason_category 
from base 
LEFT JOIN 
bal_log

on 

base.user_id = bal_log.user_id and
base.app_id = bal_log.app_id and 
base.type = bal_log.reason_category

where id in (select id from id_cnt where cnt > 1) 
--and bal_log.user_id is not null
),

id_more_1_cnt as (
select id, count(1) cnt from id_more_1_bal_log 
where bal_log_user_id is not null
group by 1 order by 2 desc,1 desc
),

id_more_1_bal_log_equal_1 as (
select id_more_1_bal_log.* exclude (bal_log_user_id, bal_log_app_id, bal_log_reason_category)
from id_more_1_bal_log 
where bal_log_user_id is not null
and id in (select id from id_more_1_cnt where cnt = 1)
),

id_more_1_bal_log_not_equal_1 as (
select id_more_1_bal_log.* exclude (user_id, bal_log_user_id, bal_log_app_id, bal_log_reason_category),
max(user_id) user_id
from id_more_1_bal_log 
where bal_log_user_id is not null
and id in (select id from id_more_1_cnt where cnt <> 1)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

id_more_1 as (
select * exclude (user_id, inserted_at), user_id, inserted_at from id_more_1_bal_log_equal_1
union all
select * exclude (user_id, inserted_at), user_id, inserted_at from id_more_1_bal_log_not_equal_1
),

id_final as (
select * from id_more_1
union all
select * from id_equal_1
)

select distinct * from id_final
