with 



----------------------------------------------------------
-- Add dynamic values you would like to customize later -- 
----------------------------------------------------------

future_proof 




as



(

select 

3 as ward_value_lookback_window 

)


,


--**********************************-- 
--** Volume Driver Table Creation **--
--**********************************-- 



-------------------------------------------------------------------------------------------------------
-- Remove duplicates and filter to only the relevant data --
-- There are multiple rows in some instances for godson bonuses with different created_at timestamps --
-- Need to check with Tanguy why there are godfathers without any ward amount 
-------------------------------------------------------------------------------------------------------

referrals_data_prep

as 


(


select 
a.customer_id
,min(b.godfather_id) as godfather_id
,max(a.amount_ward) as godson_ward
,max(b.amount_ward) as godfather_ward
,max(a.amount_ward) + max(coalesce(b.amount_ward,0)) as referral_ward
,min(a.created_at) as ref_created_at 
,min(date(a.created_at)) as ref_date 
from weward-1548152103232.silver.sponsorship_transaction as a

left join weward-1548152103232.silver.sponsorship_transaction as b
on a.godfather_id = b.customer_id 
and b.sponsorship_type = 'godfather'
and date(b.created_at) < current_date
and date(b.created_at) >= date('2024-01-01')

WHERE date(a.created_at) >= date('2024-01-01')
and a.sponsorship_type  = 'godson'
and date(a.created_at) < current_date
group by 1




)





,


---------------------------------------------------------------
-- Pull the customer country, platform, and sponsorship data -- 
---------------------------------------------------------------


customer_data 


as 


(

select 
a.customer_id 
,a.adjust_id
,CASE country
    WHEN 'FR' THEN 'France'
    WHEN 'IT' THEN 'Italy'
    WHEN 'ES' THEN 'Spain'
    WHEN 'BE' THEN 'Belgium'
    WHEN 'DE' THEN 'Germany'
    WHEN 'GB' THEN 'United Kingdom'
    WHEN 'US' THEN 'United States'
    WHEN 'JP' THEN 'Japan'
    WHEN 'NL' THEN 'Netherlands'
    WHEN 'MX' THEN 'Mexico'
    WHEN 'BR' THEN 'Brazil'
    WHEN 'CL' THEN 'Chile'
    WHEN 'PT' THEN 'Portugal'
    WHEN 'ZA' THEN 'South Africa'
    WHEN 'SE' THEN 'Sweden'
    WHEN 'CA' THEN 'Canada'
    WHEN 'AU' THEN 'Australia'
    WHEN 'AT' THEN 'Austria'
    WHEN 'FI' THEN 'Finland'
    WHEN 'CH' THEN 'Switzerland'
    WHEN 'IE' THEN 'Ireland'
    WHEN 'NZ' THEN 'New Zealand'
    WHEN 'AE' THEN 'United Arab Emirates'
    WHEN 'NO' THEN 'Norway'
    WHEN 'DK' THEN 'Denmark'
    WHEN 'SG' THEN 'Singapore'
    WHEN 'IL' THEN 'Israel'
    WHEN 'SA' THEN 'Saudi Arabia'
    WHEN 'MY' THEN 'Malaysia'
    ELSE 'Other'
END AS cust_country
,a.last_timezone as cust_last_timezone
,date(a.signup_at) as sign_up_date
,CASE 
  when a.platform like '%android%' then 'Android'
  when a.platform like '%ios%' then 'iOS'
  else 'Other'
  END
  as cust_platform

, CASE
    when b.customer_id is not null then 'Referral'
	else 'Other'
	end as cust_channel 

,coalesce(b.referral_ward,0) as ref_bonus_wards  
,b.godfather_id

FROM weward-1548152103232.silver.customer_data as a 

left join referrals_data_prep as b
on a.customer_id = b.customer_id

WHERE date(a.signup_at) >= date('2024-01-01')
and date(a.signup_at) < current_date
and a.customer_id not in (26744561) -- excluded users due to suspected cheating



)





,







------------------------------------------------
-- Pull all aggregated install data from SKAN -- 
------------------------------------------------

user_level_installs 

as 

(


SELECT
  DATE(installed_at) AS date
,CASE
    -- Explicit campaign mappings
    WHEN campaign = 'Weward_US_iOS_SKAN_Install_Test Advantage+ Campaign' THEN 'United States'
    WHEN campaign = 'Weward_LATAM_BR_iOS_SKAN_Install' THEN 'Brazil'
    WHEN campaign = 'Weward_UK_iOS_SKAN_Install_AAA' THEN 'United Kingdom'
    WHEN campaign = 'Weward_FR_iOS_Install-SKAN_Adgroups' THEN 'France'
    WHEN campaign = 'Weward_CA_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Canada'
    WHEN campaign = 'Weward_LATAM_MX_iOS_SKAN_Install' THEN 'Mexico'
    WHEN campaign = 'Weward_DE_iOS_Install-SKAN' THEN 'Germany'
    WHEN campaign = 'Weward_ZA_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'South Africa'
    WHEN campaign = 'Weward_FR_iOS_SKAN_Install_Test Advantage+ Campaign Campaign' THEN 'France'
    WHEN campaign = 'Weward_MY_iOS_SKAN_Install_Advantage+ Campaign' THEN 'Malaysia'
    WHEN campaign = 'WeWard_US_iOS_Opti-Install_SKAN' THEN 'United States'
    WHEN campaign = 'Weward_AUS_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Australia'
    WHEN campaign = 'Weward_LATAM_CL_iOS_SKAN_Install' THEN 'Chile'
    WHEN campaign = 'Weward_AT_iOS_AAA_Install-SKAN - test advantage+ Campaign' THEN 'Austria'
    WHEN campaign = 'Weward_UK_iOS_SKAN_Install_Adgroups' THEN 'United Kingdom'
    WHEN campaign = 'Weward_DE_iOS_AAA_Install-SKAN - test advantage+ Campaign' THEN 'Germany'
    WHEN campaign = 'Weward_US_iOS_SKAN_Test opti Wecard V2 Campaign' THEN 'United States'
    WHEN campaign = 'Weward_PT_iOS_SKAN_Install' THEN 'Portugal'
    WHEN campaign = 'Weward_IL_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Israel'
    WHEN campaign = 'Weward_CH_iOS_AAA_Install-SKAN - test advantage+ Campaign' THEN 'Switzerland'
    WHEN campaign = 'Weward_US_iOS_SKAN_Install_Test opti Wecard Campaign' THEN 'United States'
    WHEN campaign = 'Weward_US_iOS_SKAN_Install_Test EM Campaign' THEN 'United States'
    WHEN campaign = 'Weward_US_iOS_SKAN_5 wards won_Test Campaign' THEN 'United States'
    WHEN campaign = 'Weward_US_iOS_SKAN_Install_Test NBC vs Craftman Campaign' THEN 'United States'
    WHEN campaign = 'Weward_Nordics_SW_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Sweden'
    WHEN campaign = 'Weward_Nordics_NO_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Norway'
    WHEN campaign = 'Weward_US_iOS_SKAN_Install_Test LAL' THEN 'United States'
    WHEN campaign = 'Weward_Nordics_FI_iOS_SKAN_Install_Advantage+ Campaign Campaign' THEN 'Finland'
    WHEN campaign = 'Weward_UK_iOS_SKAN_Install_AAA - test opti 1€' THEN 'United Kingdom'
    WHEN campaign = 'Weward_DE_AND_Activated User_Adgroups' THEN 'Germany'
    WHEN campaign = 'Weward_FR_AND_ROAS' THEN 'France'
    WHEN campaign = 'Weward_UK_AND_ROAS' THEN 'United Kingdom'
    WHEN campaign = 'Weward_US_AND_ROAS' THEN 'United States'
    WHEN campaign = 'WeWard_US_iOS_Opti-ROAS' THEN 'United States'
    WHEN campaign = 'Weward_UK_iOS_SKAN_AEO_2Ksteps Campaign' THEN 'United Kingdom'
    WHEN campaign = 'Weward_US_iOS_SKAN_AEO_2Ksteps Campaign' THEN 'United States'
    WHEN campaign = 'Weward_US_AND_CPmorethan4Ksteps' THEN 'United States'
    WHEN campaign = 'Weward_UK_iOS_SKAN_AEO_Greenstar Campaign' THEN 'United Kingdom'
    WHEN campaign = 'WeWard_Creative-testing_US_AND_Installs' THEN 'United States'
    WHEN campaign = 'Weward_US_AND_CPmorethan4Ksteps - V2' THEN 'United States'
    WHEN campaign = 'Weward_DE_AND_AEO_2K Steps' THEN 'Germany'
    WHEN campaign = 'Weward_NZ_iOS_SKAN_Install_Advantage+' THEN 'New Zealand'
    WHEN campaign = 'Weward_FR_iOS_Install-AEM' THEN 'France'
    WHEN campaign = 'WeWard_Creative-testing_FR_AND_Installs' THEN 'France'
    WHEN campaign = 'Weward_DE_AND_ROAS' THEN 'Germany'
    WHEN campaign = 'Weward_AT-CH_iOS_Install-AEM' THEN 'Austria'
    WHEN campaign = 'Weward_ES_iOS_SKAN_Install_Test' THEN 'Spain'
    WHEN campaign = 'Weward_AT-CH_AND_ROAS' THEN 'Austria'
    WHEN campaign = 'Weward_UK_iOS_SKAN_Install' THEN 'United Kingdom'
    WHEN campaign = 'Weward_IT_iOS_SKAN_Install_Test' THEN 'Italy'
    WHEN campaign = 'Weward_AUS-NZ_AND_ROAS' THEN 'Australia'
    WHEN campaign = 'crea test ios' THEN 'France'

    -- Catch-all: match any "_XX_" country code and map to country name
    WHEN REGEXP_CONTAINS(campaign, r'_US_') THEN 'United States'
    WHEN REGEXP_CONTAINS(campaign, r'_BR_') THEN 'Brazil'
    WHEN REGEXP_CONTAINS(campaign, r'_UK_') THEN 'United Kingdom'
    WHEN REGEXP_CONTAINS(campaign, r'_FR_') THEN 'France'
    WHEN REGEXP_CONTAINS(campaign, r'_CA_') THEN 'Canada'
    WHEN REGEXP_CONTAINS(campaign, r'_MX_') THEN 'Mexico'
    WHEN REGEXP_CONTAINS(campaign, r'_DE_') THEN 'Germany'
    WHEN REGEXP_CONTAINS(campaign, r'_ZA_') THEN 'South Africa'
    WHEN REGEXP_CONTAINS(campaign, r'_MY_') THEN 'Malaysia'
    WHEN REGEXP_CONTAINS(campaign, r'_AUS_') THEN 'Australia'
    WHEN REGEXP_CONTAINS(campaign, r'_CL_') THEN 'Chile'
    WHEN REGEXP_CONTAINS(campaign, r'_AT_') THEN 'Austria'
    WHEN REGEXP_CONTAINS(campaign, r'_PT_') THEN 'Portugal'
    WHEN REGEXP_CONTAINS(campaign, r'_IL_') THEN 'Israel'
    WHEN REGEXP_CONTAINS(campaign, r'_CH_') THEN 'Switzerland'
    WHEN REGEXP_CONTAINS(campaign, r'_SW_') THEN 'Sweden'
    WHEN REGEXP_CONTAINS(campaign, r'_NO_') THEN 'Norway'
    WHEN REGEXP_CONTAINS(campaign, r'_FI_') THEN 'Finland'
    WHEN REGEXP_CONTAINS(campaign, r'_NZ_') THEN 'New Zealand'
    WHEN REGEXP_CONTAINS(campaign, r'_ES_') THEN 'Spain'
    WHEN REGEXP_CONTAINS(campaign, r'_IT_') THEN 'Italy'

    ELSE 'Other'
END AS campaign_country
,case 
    WHEN REGEXP_CONTAINS(adjust_country, r'US') THEN 'United States'
    WHEN REGEXP_CONTAINS(adjust_country, r'BR') THEN 'Brazil'
    WHEN REGEXP_CONTAINS(adjust_country, r'UK') THEN 'United Kingdom'
    WHEN REGEXP_CONTAINS(adjust_country, r'FR') THEN 'France'
    WHEN REGEXP_CONTAINS(adjust_country, r'CA') THEN 'Canada'
    WHEN REGEXP_CONTAINS(adjust_country, r'MX') THEN 'Mexico'
    WHEN REGEXP_CONTAINS(adjust_country, r'DE') THEN 'Germany'
    WHEN REGEXP_CONTAINS(adjust_country, r'ZA') THEN 'South Africa'
    WHEN REGEXP_CONTAINS(adjust_country, r'MY') THEN 'Malaysia'
    WHEN REGEXP_CONTAINS(adjust_country, r'AU_') THEN 'Australia'
    WHEN REGEXP_CONTAINS(adjust_country, r'CL') THEN 'Chile'
    WHEN REGEXP_CONTAINS(adjust_country, r'AT') THEN 'Austria'
    WHEN REGEXP_CONTAINS(adjust_country, r'PT') THEN 'Portugal'
    WHEN REGEXP_CONTAINS(adjust_country, r'IL') THEN 'Israel'
    WHEN REGEXP_CONTAINS(adjust_country, r'CH') THEN 'Switzerland'
    WHEN REGEXP_CONTAINS(adjust_country, r'SW') THEN 'Sweden'
    WHEN REGEXP_CONTAINS(adjust_country, r'NO') THEN 'Norway'
    WHEN REGEXP_CONTAINS(adjust_country, r'FI') THEN 'Finland'
    WHEN REGEXP_CONTAINS(adjust_country, r'NZ') THEN 'New Zealand'
    WHEN REGEXP_CONTAINS(adjust_country, r'ES') THEN 'Spain'
    WHEN REGEXP_CONTAINS(adjust_country, r'IT') THEN 'Italy'

    ELSE 'Other'
end as adjust_country 
, adjust_country as og
  ,CASE 
  when adjust_platform like '%android%' then 'Android'
  when adjust_platform like '%ios%' then 'iOS'
  else 'Other'
  END
  as platform
  -- ,adjust_campaign as campaign 
  -- ,adjust_adgroup as ad_group
  
  ,CASE

    /* ===== Major self-attributing networks (SANs) ===== */
    -- Meta family (FB/IG/MAN)
    WHEN lower(network) IN ('facebook installs','instagram installs','off-facebook installs','unattributed') or lower(network) like '%facebook%' THEN 'Meta'
    -- TikTok in all flavors (SAN, influence, installs)
    WHEN lower(network) LIKE 'tiktok%' THEN 'TikTok'
    -- Apple Search Ads
    WHEN lower(network) = 'apple search ads' THEN 'Apple'
    -- Google Ads / App Campaigns (ACI, etc.)
    WHEN lower(network) LIKE 'google ads%' THEN 'Google Ads'

    /* ===== Big ad networks / DSPs ===== */
    -- AppLovin (incl. CTV)
    WHEN lower(network) LIKE 'applovin%' THEN 'AppLovin'
    -- Unity
    WHEN lower(network) LIKE 'unity ads%' THEN 'Unity'
    -- ironSource
    WHEN lower(network) LIKE 'ironsource%' THEN 'ironSource'
    -- Tapjoy (offerwall but often tracked standalone)
    WHEN lower(network) LIKE 'tapjoy%' THEN 'Tapjoy'

    /* ===== Offerwalls ===== */
    -- Adjoe and other offerwalls
    WHEN lower(network) LIKE 'adjoe%' THEN 'Adjoe'

    /* ===== Affiliates / Partnerships ===== */
    -- Impact is an affiliate network
    WHEN lower(network) = 'impact' THEN 'Affiliate'
    -- Known partners / agencies / brand collabs (adjust as you learn more)
    WHEN lower(network) IN ('casamedia_de','zedgev2','nl_tgtg','venus','test deezer','test deezer ') THEN 'Partnerships'
    -- Cactus vendor / tournament campaigns
    WHEN lower(network) LIKE 'cactus%' THEN 'Cactus'

    /* ===== Organic & referral ===== */
    WHEN lower(network) IN ('organic','google organic search') THEN 'Organic'
    WHEN lower(network) IN ('referral','app_share_performance') THEN 'Referral'
    -- Owned/earned social buckets
    WHEN lower(network) IN ('social media','communities') THEN 'Organic Social'

    /* ===== Preloads / Carriers ===== */
    WHEN lower(network) LIKE 'preload%' OR lower(network) IN ('preload - aura','bouygues') THEN 'Preloads'

    /* ===== Miscellaneous labels ===== */
    -- Snapchat (you only listed installs)
    WHEN lower(network) LIKE 'snapchat%' THEN 'Snapchat'
    -- Influencers (direct + via agency)
    WHEN lower(network) IN ('br_influencer','blu market') THEN 'Influencer'
    -- Hygiene / QA buckets you may want to exclude from performance rollups
    WHEN lower(network) = 'untrusted devices' THEN 'Invalid'

    /* ===== Fallback ===== */
    ELSE 'Other'
  END AS channel
  ,adjust_id 
  ,row_number() over (partition by adjust_id order by installed_at asc) as rank_install 
FROM weward-1548152103232.silver.install_adjust as a 
WHERE 
DATE(installed_at) >= date('2024-01-01')
and date(installed_at) < current_date


)


-- check for duplicates -- 


-- select 
-- count(adjust_id) 
-- ,count(distinct adjust_id) 
-- from user_level_installs  


,





------------------------------------
-- Aggregate the User Level Data  -- 
------------------------------------


aggregate_customer_and_adjust_prep 


as 


(

select 

-- Customer features -- 
a.customer_id 
,a.adjust_id 
,a.cust_country as sign_up_country
,a.cust_last_timezone
,a.sign_up_date
,a.cust_platform
,a.cust_channel 
,a.ref_bonus_wards  


-- Install Features --- 

,b.date as install_date
,b.adjust_country as install_ip_country 
,b.campaign_country as install_campaign_country 
,b.adjust_id as install_id
,b.channel as install_channel
,b.platform as install_platform



-- Combined Features -- 
,case
when a.cust_channel = 'Referral' then a.cust_channel 
when b.channel is not null then b.channel
else 'No Attribution' end as channel 

,coalesce(a.cust_country,b.adjust_country) as country

,coalesce(a.cust_platform,b.platform) as platform 

,coalesce(a.sign_up_date,b.date) as date

  ,CASE
     WHEN a.adjust_id IS NOT NULL AND b.adjust_id IS NOT NULL THEN 'both'
     WHEN a.adjust_id IS NOT NULL AND b.adjust_id IS NULL     THEN 'customer_only'
     WHEN a.adjust_id IS NULL     AND b.adjust_id IS NOT NULL THEN 'install_only'
     ELSE 'Other'
   END AS row_origin

,row_number() over (partition by COALESCE(CAST(a.customer_id AS STRING), b.adjust_id) order by coalesce(b.date,a.sign_up_date)) as rank_user

,COALESCE(CAST(a.customer_id AS STRING), b.adjust_id) as user_id

from customer_data  as a
full outer join user_level_installs as b 
on a.adjust_id = b.adjust_id 
and b.rank_install = 1
-- and a.sign_up_date >= b.date


)



,suspicious_users AS 

(
  SELECT customer_id
  FROM weward-1548152103232.user_activity.cheaters

  UNION all 

  SELECT customer_id
  FROM weward-1548152103232.silver.customer_revenue
  WHERE date >= date('2024-01-01')
    AND date < CURRENT_DATE
	and campaign_category = 'offerwall'
	and source <> 'adjoe'

 union all 

 select customer_id  FROM `weward-1548152103232.user_activity.cheaters_revenue`


 union all 

 select customer_id
  FROM `weward-1548152103232.study.transaction_ow_survey_09_2025_to_02_2026`
  WHERE survey_institute = 'tapjoy_offerwall'

    )



,aggregate_customer_and_adjust as 

(

select 
* 
from aggregate_customer_and_adjust_prep
where rank_user = 1
and sign_up_country = 'United States' 
-- and customer_id not in (select customer_id from suspicious_users)

)


-- select 
-- date
-- ,count(customer_id) as cust
-- ,count(distinct customer_id) dist_cust
-- ,count(adjust_id) as matched_installs
-- ,count(distinct adjust_id) dist_matched_installs
-- ,count(install_id) as installs
-- ,count(distinct install_id) as dist_installs
-- from aggregate_customer_and_adjust 
-- group by 1 
-- order by 1 desc 

-- select 
-- count(customer_id) as cust
-- ,count(distinct customer_id) dist_cust
-- ,count(adjust_id) as matched_installs
-- ,count(distinct adjust_id) dist_matched_installs
-- ,count(install_id) as installs
-- ,count(distinct install_id) as dist_installs
-- from aggregate_customer_and_adjust 


-- 1,826,496
-- 1,826,496
-- 1,801,388
-- 1,693,735
-- 2,408,914
-- 2,342,762



-- 1,826,496
-- 1,826,496
-- 1,801,388
-- 1,693,735
-- 2,412,957
-- 2,342,762

----------------------------------------------------------
-- Aggregate the volume metrics to be joined at the end -- 
----------------------------------------------------------

, 


volume as (
  select 
    DATE_TRUNC(a.date, WEEK(MONDAY)) as date 
  , platform 
  -- , country 
  -- , channel
  , count(distinct a.customer_id) as accounts
  , count(distinct a.install_id)  as app_installs
  , count(distinct a.date) as volume_days_mature 
  from aggregate_customer_and_adjust as a
  WHERE rank_user = 1
  group by 1
  ,2
  -- ,3
  -- ,4
)



,





--*****************************-- 
--** Revenue Cohort Creation **--
--*****************************-- 

--------------------------------------------------------------
-- Build cohots of revenue data joined to the volume driver -- 
--------------------------------------------------------------

revenue_data_prep as (
  select 
    DATE_TRUNC(a.date, WEEK(MONDAY)) as date 
  , platform 
  -- , country 
  -- , channel
  , DATE_DIFF(DATE_TRUNC(b.date, WEEK(MONDAY)), DATE_TRUNC(a.date, WEEK(MONDAY)), WEEK) AS period
  , count(distinct a.customer_id) as accounts
  , count(distinct a.install_id)  as app_installs
  , sum(revenue_eur)                                  as total_rev
  , SUM(CASE WHEN b.campaign_category = 'paidoffer'    THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS paidoffer_rev
  , SUM(CASE WHEN b.campaign_category = 'ads'          THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS ads_rev
  , SUM(CASE WHEN b.campaign_category = 'affiliation'  THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS affiliation_rev
  , SUM(CASE WHEN b.campaign_category = 'freeoffer'    THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS freeoffer_rev
  , SUM(CASE WHEN b.campaign_category = 'survey'       THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS survey_rev
  , SUM(CASE WHEN b.campaign_category = 'iap'          THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS iap_rev
  , SUM(CASE WHEN b.campaign_category = 'offerwall'    THEN COALESCE(revenue_eur, 0) ELSE 0 END) AS offerwall_rev
  from aggregate_customer_and_adjust as a
  left join weward-1548152103232.silver.customer_revenue as b

-- left join (

-- select * from weward-1548152103232.silver.customer_revenue 
-- where not 
-- (
-- lower(coalesce(source,'')) = 'adjoe' 
-- and date >= date('2025-01-01') 
-- )

-- union all 

-- select * from silver.customer_revenue_adjoe_temp where lower(source) = 'adjoe' and date >= date('2025-01-01') 

-- ) as b
    on a.customer_id = b.customer_id 
   and a.sign_up_date <= b.date
   and b.date >= date('2024-01-01')
  and b.date < current_date  
  -- AND b.date < DATE_TRUNC(current_date, WEEK(MONDAY))
  -- and b.date < current_date
   -- and DATE_TRUNC(b.date, WEEK(MONDAY)) < current_date 
  group by 1
  ,2
  ,3
  -- ,4
  -- ,5
)

 -- 5,951,190.52
-- select 
-- sum(total_rev) from revenue_data_prep


-- SELECT COUNT(DISTINCT CUSTOMER_ID), COUNT(CUSTOMER_ID) FROM aggregate_customer_and_adjust 


,

-------------------------------------------------------------------------------------------------
-- Create the final revenue data cohorts including cumulative functions over the last X period -- 
-------------------------------------------------------------------------------------------------

revenue_cohorts


as 


(


select
  date
, platform
-- , country
-- , channel
, period as period 

-- point-in-time values
, total_rev as per_total_rev
, paidoffer_rev as per_paidoffer_rev
, ads_rev as per_ads_rev
, affiliation_rev as per_affiliation_rev
, freeoffer_rev as per_freeoffer_rev
, survey_rev as per_survey_rev
, iap_rev as per_iap_rev
, offerwall_rev as per_offerwall_rev

-- cumulative across age within each cohort
, SUM(total_rev)       OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_total_rev
, SUM(paidoffer_rev)   OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_paidoffer_rev
, SUM(ads_rev)         OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_ads_rev
, SUM(affiliation_rev) OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_affiliation_rev
, SUM(freeoffer_rev)   OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_freeoffer_rev
, SUM(survey_rev)      OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_survey_rev
, SUM(iap_rev)         OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_iap_rev
, SUM(offerwall_rev)   OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_offerwall_rev


from revenue_data_prep
order by platform
-- , country
-- , channel
, date
, period

)



,



--******************************************************-- 
--** Contra Revenue (cost of rewards) Cohort Creation **--
--******************************************************-- 

---------------------------------------------------------------------------
-- Pull the actual cost of wards for use to estimtae the cost of rewards -- 
---------------------------------------------------------------------------


ward_value_data_prep 


as 


(


SELECT 
-- CASE 
--   when b.platform like '%android%' then 'Android'
--   when b.platform like '%ios%' then 'iOS'
--   else 'Other'
--   END
--   as platform
-- ,
-- CASE b.country
--     WHEN 'FR' THEN 'France'
--     WHEN 'IT' THEN 'Italy'
--     WHEN 'ES' THEN 'Spain'
--     WHEN 'BE' THEN 'Belgium'
--     WHEN 'DE' THEN 'Germany'
--     WHEN 'GB' THEN 'United Kingdom'
--     WHEN 'US' THEN 'United States'
--     WHEN 'JP' THEN 'Japan'
--     WHEN 'NL' THEN 'Netherlands'
--     WHEN 'MX' THEN 'Mexico'
--     WHEN 'BR' THEN 'Brazil'
--     WHEN 'CL' THEN 'Chile'
--     WHEN 'PT' THEN 'Portugal'
--     WHEN 'ZA' THEN 'South Africa'
--     WHEN 'SE' THEN 'Sweden'
--     WHEN 'CA' THEN 'Canada'
--     WHEN 'AU' THEN 'Australia'
--     WHEN 'AT' THEN 'Austria'
--     WHEN 'FI' THEN 'Finland'
--     WHEN 'CH' THEN 'Switzerland'
--     WHEN 'IE' THEN 'Ireland'
--     WHEN 'NZ' THEN 'New Zealand'
--     WHEN 'AE' THEN 'United Arab Emirates'
--     WHEN 'NO' THEN 'Norway'
--     WHEN 'DK' THEN 'Denmark'
--     WHEN 'SG' THEN 'Singapore'
--     WHEN 'IL' THEN 'Israel'
--     WHEN 'SA' THEN 'Saudi Arabia'
--     WHEN 'MY' THEN 'Malaysia'
--     ELSE 'Other'
-- END AS country
-- ,  
SUM(a.cost_eur) / NULLIF(SUM(a.amount), 0) AS ward_value
  
FROM `696845466639.costs.fct_cashout` as a

INNER JOIN `696845466639.raw_data.auth` as b 
  ON a.customer_id = b.customer_id
  
WHERE a.created_at >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL (select ward_value_lookback_window from future_proof) MONTH)
and a.created_at < DATE_TRUNC(current_date , MONTH)
-- group by 1
-- ,2
order by 1
-- ,2

)






--------------------------------------------------------------------------------
-- Aggregate the total ward amount and the previously calculate cost of wards -- 
--------------------------------------------------------------------------------

,cost_data_prep AS (
  SELECT
    DATE_TRUNC(a.date, WEEK(MONDAY))                                                   AS date
  ,a.platform
  ,DATE_DIFF(DATE_TRUNC(b.date, WEEK(MONDAY)), DATE_TRUNC(a.date, WEEK(MONDAY)), WEEK) AS period
  ,SUM(b.amount_ward)                                                                   AS contra_wards
  ,MAX(c.ward_value)                                                                    AS contra_ward_value
  ,SUM(b.amount_ward) * MAX(c.ward_value) * 0.40                                       AS contra_rev_40p
  ,SUM(b.amount_ward) * MAX(c.ward_value)                                               AS contra_rev_100p
  FROM aggregate_customer_and_adjust AS a
  LEFT JOIN weward-1548152103232.silver.customer_cost AS b
    ON a.customer_id = b.customer_id
   AND a.sign_up_date <= b.date
   AND b.date >= date('2024-01-01')
   AND b.date < CURRENT_DATE
   AND b.amount_ward > 0
  LEFT JOIN ward_value_data_prep AS c
    ON 1=1
  GROUP BY 1, 2, 3
)


-- select * from cost_data_prep  where contra_wards < 0

,


----------------------------------------------------------------------------------------------
-- Create the final cost data cohorts including cumulative functions over the last X period -- 
----------------------------------------------------------------------------------------------


cost_cohorts 


as 


(

  select 
    date 
  , platform 
  -- , country 
  -- , channel
  , period 
  , contra_wards as per_contra_wards
  , contra_ward_value as per_contra_ward_value
  -- , contra_rev as per_contra_rev 
  , contra_rev_40p as per_contra_rev_40p
  , contra_rev_100p as per_contra_rev_100p
  , SUM(contra_wards) OVER (PARTITION BY platform, date ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_contra_wards
  -- , SUM(contra_rev) OVER  (PARTITION BY platform, date ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_contra_rev  
  , SUM(contra_rev_40p) OVER  (PARTITION BY platform, date ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_contra_rev_40p
  , SUM(contra_rev_100p) OVER  (PARTITION BY platform, date ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_contra_rev_100p
  from cost_data_prep 


)


-- select count(distinct a.customer_id) as accounts, count( a.customer_id) as accounts2
-- from aggregate_customer_and_adjust as a

,



--***************************************--
--** Redemption Cohort Calculations **--
--***************************************--

--------------------------------------------------------------
-- Build cohorts of redemption data joined to the volume driver --
--------------------------------------------------------------

redemption_data_prep as (
  select
    DATE_TRUNC(a.date, WEEK(MONDAY)) as date
  , a.platform
  -- , a.country
  -- , a.channel
  , DATE_DIFF(DATE_TRUNC(DATE(b.created_at), WEEK(MONDAY)), DATE_TRUNC(a.date, WEEK(MONDAY)), WEEK) AS period
  , sum(-1 * b.cost_eur)                              as redemption
  , sum(-1 * b.amount)                               as redemption_wards
  from aggregate_customer_and_adjust as a
  left join `696845466639.costs.fct_cashout` as b
    on a.customer_id = b.customer_id
   and a.sign_up_date <= DATE(b.created_at)
   and DATE(b.created_at) >= date('2024-01-01')
   and DATE(b.created_at) < current_date
  group by 1
  ,2
  ,3
  -- ,4
  -- ,5
)

,


redemption_cohorts


as


(

select
  date
, platform
-- , country
-- , channel
, period

-- point-in-time values
, redemption as per_redemption
, redemption_wards as per_redemption_wards

-- cumulative across age within each cohort
, SUM(redemption) OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_redemption
, SUM(redemption_wards) OVER (PARTITION BY platform, date ORDER BY period
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_redemption_wards

from redemption_data_prep
order by platform
-- , country
-- , channel
, date
, period

)


,


--***********************************--
--** Retention Cohort Calculations **--
--***********************************-- 

--------------------------------------------------------------
-- Build cohots of revenue data joined to the volume driver -- 
--------------------------------------------------------------




retention_data_prep as (
  select 
    DATE_TRUNC(a.date, WEEK(MONDAY)) as date 
  , a.platform 
  -- , a.country 
  -- , a.channel
  , DATE_DIFF(DATE_TRUNC(b.date, WEEK(MONDAY)), DATE_TRUNC(a.date, WEEK(MONDAY)), WEEK) AS period
  , count(distinct a.customer_id) as accounts
  , count(distinct a.install_id)  as app_installs
  , count(distinct b.customer_id)                             as retained 
  from aggregate_customer_and_adjust as a
  left join weward-1548152103232.silver.customer_activity as b
    on a.customer_id = b.customer_id 
   and a.sign_up_date <= b.date
   and b.date >= date('2024-01-01')
   -- AND b.date < DATE_TRUNC(current_date, WEEK(MONDAY))
   and b.date < current_date 
  group by 1
  ,2
  ,3
  -- ,4
  -- ,5
)

,


retention_cohorts 

as 


(


  select 
    date 
  , platform 
  -- , country 
  -- , channel
  , period 
  , retained as per_retained
  -- , SUM(retained) OVER (PARTITION BY platform, date ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_retained
  from retention_data_prep 


)


,



--**********************************-- 
--** Final Data Aggregation Stage **--
--**********************************-- 

----------------------------------------------------------------------------------------
-- Build a cross product in order to provide every cohort wiht every possible period -- 
----------------------------------------------------------------------------------------


-------------------------------------------------
-- Create a data frame of all possible periods -- 
-------------------------------------------------


possible_periods 

AS 

(

  SELECT distinct period 
  FROM retention_cohorts
  WHERE period IS NOT NULL 
  AND period >= 0

  union distinct 


  SELECT distinct period 
  FROM revenue_cohorts
  WHERE period IS NOT NULL 
  AND period >= 0
)


-- select count(distinct period) from possible_periods 

,

------------------------------------------------------
-- Cross Product these periods onto the volume data -- 
------------------------------------------------------


volume_cohorts AS 


(
  SELECT
    a.date
  , a.platform
  -- , a.country
  -- , a.channel
  , a.accounts
  , a.app_installs
  , b.period
  , a.volume_days_mature
  FROM volume a
  CROSS JOIN possible_periods  b
)



-----------------------------------------
-- Pull the final data joined together -- 
-----------------------------------------

,


final_data


as 



(

select 
a.date 
,a.platform
-- ,a.country
-- ,a.channel
,a.period
,a.accounts
,a.app_installs
, b.per_total_rev
, b.per_paidoffer_rev
, b.per_ads_rev
, b.per_affiliation_rev
, b.per_freeoffer_rev
, b.per_survey_rev
, b.per_iap_rev
, b.per_offerwall_rev
, b.cumu_total_rev
, b.cumu_paidoffer_rev
, b.cumu_ads_rev
, b.cumu_affiliation_rev
, b.cumu_freeoffer_rev
, b.cumu_survey_rev
, b.cumu_iap_rev
, b.cumu_offerwall_rev

, c.per_contra_wards
, c.per_contra_ward_value
-- , c.per_contra_rev
, c.per_contra_rev_40p
, c.per_contra_rev_100p
, c.cumu_contra_wards
-- , c.cumu_contra_rev
, c.cumu_contra_rev_40p
, c.cumu_contra_rev_100p


-- , b.per_total_rev - c.per_contra_rev as per_net_rev
-- , b.cumu_total_rev - c.cumu_contra_rev as cumu_net_rev
, b.per_total_rev - c.per_contra_rev_40p as per_net_rev_40p
, b.cumu_total_rev - c.cumu_contra_rev_100p as cumu_net_rev_100p


,coalesce(d.per_retained ,0) as per_retained
-- ,coalesce(d.cumu_retained,0) as cumu_retained

, e.per_redemption
, e.per_redemption_wards
, e.cumu_redemption
, e.cumu_redemption_wards
, SAFE_DIVIDE(e.per_redemption, e.per_redemption_wards)   AS per_ward_value
, SAFE_DIVIDE(e.cumu_redemption, e.cumu_redemption_wards) AS cumu_ward_value

from volume_cohorts as a

left join revenue_cohorts as b
on a.date = b.date 
and a.platform = b.platform 
-- and a.country = b.country 
-- and a.channel = b.channel 
and a.period = b.period
 -- AND DATE_ADD(b.date, INTERVAL (b.period + 1 + 2) WEEK) <= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))



left join cost_cohorts as c 
on a.date = c.date 
and a.platform = c.platform 
-- and a.country = c.country 
-- and a.channel = c.channel 
and a.period = c.period 
 -- AND DATE_ADD(c.date, INTERVAL (c.period + 1 + 2) WEEK) <= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))


left join retention_cohorts as d
on a.date = d.date
and a.platform = d.platform
-- and a.country = d.country
-- and a.channel = d.channel
and a.period = d.period
 -- AND DATE_ADD(d.date, INTERVAL (d.period + 1 + 2) WEEK) <= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))

left join redemption_cohorts as e
on a.date = e.date
and a.platform = e.platform
-- and a.country = e.country
-- and a.channel = e.channel
and a.period = e.period

where a.platform <> 'Other'
and a.volume_days_mature >= 7
AND DATE_ADD(
      DATE_ADD(a.date, INTERVAL a.period WEEK),
      INTERVAL (1 + 2) WEEK
    )
    <= DATE_TRUNC(current_date, WEEK(MONDAY))

	

AND (
     b.period IS NOT NULL
  OR c.period IS NOT NULL
  OR d.period IS NOT NULL
  OR e.period IS NOT NULL
)	

-- a.accounts < a.app_installs

order by platform
-- , country
-- , channel
, date
, period asc 

-- limit 5000 

)

select
*
from final_data
