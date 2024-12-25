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
Announcement, 
Organic, 
Pixoble_freeapp, 
ValueLeaf_freeapp, 
FacebookInternal_freeapp, 
TheFluencer_freeapp, 
OnCLicka_freeapp, 
Adsterra_freeapp, 
Stan_freeapp, 
GoogleAdsInternal_freeapp, 
Organic_freeapp, 
Pixoble_Cashapp, 
ValueLeaf_Cashapp, 
FacebookInternal_Cashapp, 
TheFluencer_Cashapp, 
OnCLicka_Cashapp, 
Adsterra_Cashapp, 
Stan_Cashapp, 
GoogleAdsInternal_Cashapp, 
Organic_Cashapp,
Migration_Cashapp
    FROM {{ ref('input_gr_attr_source_spends') }}
) AS t
UNPIVOT (
    Cost FOR Source IN 
(
Announcement, 
Organic, 
Pixoble_freeapp, 
ValueLeaf_freeapp, 
FacebookInternal_freeapp, 
TheFluencer_freeapp, 
OnCLicka_freeapp, 
Adsterra_freeapp, 
Stan_freeapp, 
GoogleAdsInternal_freeapp, 
Organic_freeapp, 
Pixoble_Cashapp, 
ValueLeaf_Cashapp, 
FacebookInternal_Cashapp, 
TheFluencer_Cashapp, 
OnCLicka_Cashapp, 
Adsterra_Cashapp, 
Stan_Cashapp, 
GoogleAdsInternal_Cashapp, 
Organic_Cashapp,
Migration_Cashapp
)
) AS Temp_Unpivoted
left join
ahsokatano_4_raw.input_gr_attr_identifier id
on upper(Temp_Unpivoted.source) = upper(id.source)
