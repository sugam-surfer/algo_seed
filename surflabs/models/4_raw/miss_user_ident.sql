{{
    config(
        materialized    = 'table',
        unique_key      = 'ident_value',
        tags            = 'warehouse',
    )
}}

select *,
'1900-01-01 00:00:00' as created_at,
'1900-01-01 00:00:00' as updated_at,
concat('dummy_user_',row_number() over(order by ident_value),'@gmail.com') as friendly_name 
from (
select distinct user_id, ident_type, ident_value
from (
select a.*,b.* from
(
select id a_txn_id, * 
from 
hansolo.trivia_transactions
where ident_value in
(select distinct ident_value from hansolo.trivia_transactions
minus
select distinct ident_value from padme.user_ident) 
) a
left join
(
select id b_id, * from padme.user_balance_log
where reason like '%with%'
) b
on 
a.created_at = b.created_at
where amount + change = 0
order by a_txn_id
))

