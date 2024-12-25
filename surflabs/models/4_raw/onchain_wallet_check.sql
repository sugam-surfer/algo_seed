{{
    config(
        materialized    = 'table',
	    tags            = ['onchain', 'warehouse'],
    )
}}

select 
  ident_type, 
  ident_value
from {{ source('padme','user_ident') }}
where ident_value not in 
  (
    select 
        distinct ident_value 
    from {{ source('hansolo','web3auth_wallets') }}
  )
and created_at::date >= '2024-05-27' 
--and created_at < timeadd(hour,-2,current_timestamp())
and ident_type like 'web3auth%'
