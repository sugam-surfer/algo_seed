{{
    config(
        materialized    = 'table',
        tags		    = 'warehouse',
    )
}}

select
  user_id, balance, currency, last_login, app_id
from
  (
    select
      a.user_id, a.balance, a.currency, b.last_login, b.app_id,
      case when left(currency, 1) = 'B'
      and right(currency, len(currency)-1) in ('Hard', 'dummy_vouchers', 'hard', 'USD', 'XTZ', 'RBW', 'GRAPE', 'APE', 'BABYDOGE', 'CUXAI', 'CUARB', 'XAIXAI', 'XAIARB', 'ETH', 'USDTARB')
      then right(currency, len(currency)-1)
      else currency end as currency_match
    from
      padme.user_hard_balance a
      left join (
        select
          b1.id as user_id,
          b2.id as app_id,
          b1.last_login
        from
          padme.user b1
          left join hansolo.app b2 on b1.company_id = b2.company_id
          where b1.id not in (select user_id from ahsokatano_4_raw.raw_hansolo_trivia_transactions)
      ) b on a.user_id = b.user_id
    where
      b.app_id in ('2WNr2yXBKfirIsZ1fZVjzKGk6oC','2dj3bVyQPnukw9ypC0ktXPZz6mL')
      and timestampdiff(hour, b.last_login, current_timestamp) > 7 * 24 + 1
      and currency <> currency_match
      and balance > 0
  )
