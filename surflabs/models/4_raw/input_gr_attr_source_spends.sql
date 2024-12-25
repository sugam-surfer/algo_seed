{{
    config(
        materialized    = 'table',
        tags            = ['attr', 'warehouse'],
)
}}

select  
TO_DATE(DATE, 'DD/MM/YYYY') DATE,
TO_DATE(MONTH_START, 'DD/MM/YYYY') MONTH_START,
TOTAL,
ANNOUNCEMENT,
ORGANIC,
PIXOBLE_FREEAPP,
VALUE_LEAF_FREEAPP as ValueLeaf_freeapp, 
FACEBOOK_INTERNAL_FREEAPP as FacebookInternal_freeapp, 
THE_FLUENCER_FREEAPP as TheFluencer_freeapp, 
ON_CLICKA_FREEAPP as OnCLicka_freeapp,
ADSTERRA_FREEAPP,
STAN_FREEAPP,
GOOGLE_ADS_INTERNAL_FREEAPP as GoogleAdsInternal_freeapp, 
ORGANIC_FREEAPP,
PIXOBLE_CASHAPP,
VALUE_LEAF_CASHAPP as ValueLeaf_Cashapp, 
FACEBOOK_INTERNAL_CASHAPP as FacebookInternal_Cashapp, 
THE_FLUENCER_CASHAPP as TheFluencer_Cashapp, 
ON_CLICKA_CASHAPP as OnCLicka_Cashapp, 
ADSTERRA_CASHAPP,
STAN_CASHAPP,
GOOGLE_ADS_INTERNAL_CASHAPP as GoogleAdsInternal_Cashapp, 
ORGANIC_CASHAPP,
MIGRATION_CASHAPP
from 
google_sheets.input_gr_attr_source_spends
