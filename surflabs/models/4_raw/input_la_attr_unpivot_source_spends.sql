{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
    )
}}

SELECT 
    Temp_Unpivoted.Date, 
    Temp_Unpivoted.Source, 
    id.identifier,
    Temp_Unpivoted.Cost
FROM (
SELECT Date, 
Pixoble_freeapp, 
ValueLeaf_freeapp, 
FacebookInternal_freeapp, 
TheFluencer_freeapp, 
XAI_freeapp, 
F2C_Banner_Migration_freeApp,
Stan_freeapp, 
GoogleAdsInternal_freeapp, 
Organic_freeapp, 
Pixoble_Cashapp, 
ValueLeaf_Cashapp, 
FacebookInternal_Cashapp, 
TheFluencer_Cashapp, 
XAI_Cashapp, 
F2C_Banner_Migration_CashApp,
Stan_Cashapp, 
GoogleAdsInternal_Cashapp, 
Organic_Cashapp,
UNICORN_TRIVIA_FACEBOOK_LAGUNA_AD_ACCOUNT,
MIGRATION_SOLITAIRE_CASHAPP,
ORGANIC_SOLITAIRE_FREEAPP,
MIGRATION_SOLITAIRE_FREEAPP,
ORGANIC_SOLITAIRE_CASHAPP,
UNICORN_MOB_RUN_FACEBOOK_LAGUNA_AD_ACCOUNT,
UNICORN_BUMPER_CORNS_FACEBOOK_LAGUNA_AD_ACCOUNT
    FROM {{ ref('input_la_attr_source_spends') }}
) AS t
UNPIVOT (
    Cost FOR Source IN 
(
Pixoble_freeapp, 
ValueLeaf_freeapp, 
FacebookInternal_freeapp, 
TheFluencer_freeapp, 
XAI_freeapp, 
F2C_Banner_Migration_freeApp,
Stan_freeapp, 
GoogleAdsInternal_freeapp, 
Organic_freeapp, 
Pixoble_Cashapp, 
ValueLeaf_Cashapp, 
FacebookInternal_Cashapp, 
TheFluencer_Cashapp, 
XAI_Cashapp, 
F2C_Banner_Migration_CashApp,
Stan_Cashapp, 
GoogleAdsInternal_Cashapp, 
Organic_Cashapp,
UNICORN_TRIVIA_FACEBOOK_LAGUNA_AD_ACCOUNT,
MIGRATION_SOLITAIRE_CASHAPP,
ORGANIC_SOLITAIRE_FREEAPP,
MIGRATION_SOLITAIRE_FREEAPP,
ORGANIC_SOLITAIRE_CASHAPP,
UNICORN_MOB_RUN_FACEBOOK_LAGUNA_AD_ACCOUNT,
UNICORN_BUMPER_CORNS_FACEBOOK_LAGUNA_AD_ACCOUNT
)
) AS Temp_Unpivoted
left join
ahsokatano_4_raw.input_la_attr_identifier id
on upper(Temp_Unpivoted.source) = upper(id.source)
