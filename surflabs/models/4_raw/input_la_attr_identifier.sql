{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
)
}}

select  
  source,
  app_name,
  identifier
from 
google_sheets.input_la_attr_identifier
