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
google_sheets.input_gr_attr_identifier
