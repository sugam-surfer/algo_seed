{{
    config(
        materialized    = 'table',
    	tags    		= ['onchain', 'warehouse'],
    )
}}

select 
  ident_type, 
  ident_value
from {{ source('hansolo','web3auth_wallets') }}
where init = 0
--  and created_at < timeadd(hour,-2,current_timestamp())
