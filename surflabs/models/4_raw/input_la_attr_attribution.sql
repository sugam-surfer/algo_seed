{{
    config(
        materialized	= 'table',
        tags            = ['attr', 'warehouse'],
)
}}

select  
	APP_NAME,
	ATTR_AF_STATUS,
	ATTR_MEDIA_SOURCE,
	ATTR_AF_CHANNEL,
	ATTR_CAMPAIGN_NAME,
	ATTR_CAMPAIGN_ID,
	ATTR_AF_ADSET_ID,
	IDENTIFIER,
	USER_COUNT
from 
google_sheets.input_la_attr_manual_attribution
