i-- Instructions:
-- 1: Change dates parameters below
-- 2: Change table creation locations (find + replace): sandbox_bmorais.competitive_intelligence
-- database + schema

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- This code still uses warehouse.prod.imv_logs
-- Chris Paoletti will update this to use in analytics.inventory.inventory_listings_imv in January 2026
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Performance Health Sent Email date
set performance_sent_dt = '2025-12-02'; 

-- NBDR (DDI) Set email sent start/end datetimes
-- PRE/POST TWO WEEKS associated with performance_sent_dt
set nbdr_sent_start_dt = '2025-11-18'; 
set nbdr_sent_end_dt = '2025-12-17'; 

-- Set IMV start/end datetimes
set imv_start_dt = '2025-01-01'; -- fixed
set imv_end_dt = '2025-12-31'; 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Get List of dealers who were sent + opened the Performance Health Report Email 
CREATE OR REPLACE TABLE sandbox_bmorais.competitive_intelligence.PERFORMANCE_HEALTH_DEALER_LIST AS
select 
    spid_mapping.context_value_int as service_provider_id
    , spid_mapping._region_
    , count(case when se.sent_time is not null then spid_mapping.context_value_int else false end) as sends 
    , count(case when se.open_time is not null then spid_mapping.context_value_int else false end) as opens 
from warehouse.site.sent_email se
inner join warehouse.site.dealer_email_sparkpost_track_events ste
    on se.id = ste.sent_email_id
    and se._region_ = case
                        when ste.i18n_region = 'XA' then 'NA'
                        else ste.i18n_region end
inner join warehouse.site.sent_email_context as spid_mapping
    on se.id = spid_mapping.sent_email_id
    and se._region_ = spid_mapping._region_
    and spid_mapping.context_key = 'sp_id'
where
     se.email_type = 'PERFORMANCE_HEALTH'
    and ste.environment_name = 'PROD'
    and se.email not like '%cargurus%'
    and sent_time::date = $performance_sent_dt 
group by all
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------
-- deal rating and price are not stored in the email data
-- only listing id and service provider id are surfaced in the email data
-- we'll approximate deal rating and price from the IMV logs via email send date information
-- create table with all NBDR email sent date + listings and with all info surfaced to dealer in those emails
-- original price, original deal rating, price at next deal rating all pulled from IMV LOGS on the date of NBDR email send

CREATE OR REPLACE TABLE
  sandbox_bmorais.competitive_intelligence.NBDR_LISTING_PRICE_AT_SEND_BY_COUNTRY_DEC_12_02 AS

WITH
    cte_emails_and_listings AS (
        SELECT DISTINCT
            c_sp_id.context_value_int AS service_provider_id
          , c_sp_id._region_
          , c_l_id.context_value_int AS inventory_listing_id
          , s.sent_time::DATE AS sent_date
          , s.open_time::DATE AS open_date
          , s.click_time::DATE AS click_date
          , s.unsubscribe_time::DATE AS unsubscribe_date
        FROM
            warehouse.site.sent_email s
            JOIN warehouse.site.sent_email_context c_l_id ON c_l_id.sent_email_id = s.id
            AND s.email_type IN ('NEXT_BEST_DEAL_RATING', 'NEXT_BEST_DEAL_RATING_CA', 'NEXT_BEST_DEAL_RATING_UK')
            AND c_l_id.context_key = 'l_id'
            JOIN warehouse.site.sent_email_context c_sp_id ON c_sp_id.sent_email_id = s.id
            AND s.email_type IN ('NEXT_BEST_DEAL_RATING', 'NEXT_BEST_DEAL_RATING_CA', 'NEXT_BEST_DEAL_RATING_UK')
            AND c_sp_id.context_key = 'sp_id'
            JOIN sandbox_bmorais.competitive_intelligence.PERFORMANCE_HEALTH_DEALER_LIST as performance_health_dealers
            on c_sp_id.context_value_int = performance_health_dealers.service_provider_id and c_sp_id._region_ = performance_health_dealers._region_
        WHERE
            s.email_type IN ('NEXT_BEST_DEAL_RATING', 'NEXT_BEST_DEAL_RATING_CA', 'NEXT_BEST_DEAL_RATING_UK')
            AND s.sent_time::DATE >= $nbdr_sent_start_dt
            AND s.sent_time::DATE < $nbdr_sent_end_dt
            AND s.id NOT LIKE '%cargurus.com%'
            AND performance_health_dealers.opens is not null -- only filter to dealers who opened performance health report 
    )
  , cte_emails_and_listings_dedup AS (
        SELECT
            service_provider_id
          , _region_
          , inventory_listing_id
          , sent_date
          , COUNT(
                DISTINCT CASE
                    WHEN open_date IS NOT NULL THEN open_date
                    ELSE NULL
                END
            ) AS open_count
          , COUNT(
                DISTINCT CASE
                    WHEN open_date IS NOT NULL THEN click_date
                    ELSE NULL
                END
            ) AS click_count
          , COUNT(
                DISTINCT CASE
                    WHEN unsubscribe_date IS NOT NULL THEN unsubscribe_date
                    ELSE NULL
                END
            ) AS unsubscribe_count
        FROM
            cte_emails_and_listings
        GROUP BY
            1
          , 2
          , 3
          , 4
    )
  , cte_original_deal_rating_price AS (
        -- find deal rating and price associated with the listing we sent in the NBDR email
        -- anchored to the date/time of the NBDR email send
        -- appears to be 3 duplicates on 6/5/2023 in the IMV logs, no idea why...
        -- we'll handle these duplicates as completely separate instances
        SELECT DISTINCT
            nbdr_emails.*
          , imv_prod.process_start_time::DATE AS imv_date
        , CASE 
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'FAIR_DEAL' THEN 'FAIR_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'HIGH_PRICE' THEN 'POOR_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'LOW_OUTLIER' THEN 'OUTLIER'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'GOOD_DEAL' THEN 'GOOD_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'GREAT_DEAL' THEN 'GREAT_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'HIGH_OUTLIER' THEN 'OUTLIER'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'OVERPRICED' THEN 'OVERPRICED'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'NPA' THEN 'NA'
                ELSE imv_prod.deal_rating
                END as original_deal_rating
          , imv_prod.price AS original_price
            -- calculate price at next best deal rating

          , (
                (imv_prod."PRICE") - (
                    ROUND(
                        CASE
                            WHEN (
                                TO_CHAR(
                                    TO_DATE(imv_prod."PROCESS_START_TIME")
                                  , 'YYYY-MM-DD'
                                )
                            ) >= '2021-11-01' -- New IMV score cutoffs finalized after 11/1/21
                            AND SQRT((imv_prod."REGRESSION_VARIANCE")) != 0 -- regression variance from can't be zero
                            AND (imv_prod."SERVICE_PROVIDER_ID") IS NOT NULL -- remove P2P
                            AND (imv_prod."PRICE") IS NOT NULL -- price can't be null
                            AND (imv_prod."IMV_CONTEXT_NEUTRAL_SCORE") IS NOT NULL -- score can't be null
                            AND (imv_prod."IS_NEW") = FALSE -- must not be new
                            AND (
                                CASE
                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                    ELSE INITCAP(
                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                    )
                                END
                            ) != 'Great Price' -- must not already be great price; can't improve rating
                            THEN
                            -- below: IMV ratings cutoffs at which each listing's "next" deal rating starts (valid from Nov. 2021 onwards)
                            (
                                (
                                    CASE
                                        WHEN (imv_prod."COUNTRY_CODE") = 'US' THEN CASE
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") < 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN -2.99999999
                                            WHEN (imv_prod."IMV_FINAL_SCORE") > -3.0
                                            AND (imv_prod."IMV_FINAL_SCORE") < -1.54 THEN -1.54
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -1.54
                                            AND (imv_prod."IMV_FINAL_SCORE") < -0.72 THEN -0.72
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -0.72
                                            AND (imv_prod."IMV_FINAL_SCORE") < 0.23 THEN 0.23
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 0.23
                                            AND (imv_prod."IMV_FINAL_SCORE") < 1.02 THEN 1.02
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 1.02
                                            AND (imv_prod."IMV_FINAL_SCORE") < 3 THEN NULL
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") > 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN 2.9999999999
                                        END
                                        WHEN (imv_prod."COUNTRY_CODE") = 'CA' THEN CASE
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") < 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN -2.99999999
                                            WHEN (imv_prod."IMV_FINAL_SCORE") > -3.0
                                            AND (imv_prod."IMV_FINAL_SCORE") < -1.57 THEN -1.57
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -1.57
                                            AND (imv_prod."IMV_FINAL_SCORE") < -0.71 THEN -0.71
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -0.71
                                            AND (imv_prod."IMV_FINAL_SCORE") < 0.25 THEN 0.25
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 0.25
                                            AND (imv_prod."IMV_FINAL_SCORE") < 1.11 THEN 1.11
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 1.11
                                            AND (imv_prod."IMV_FINAL_SCORE") < 3 THEN NULL
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") > 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN 2.9999999999
                                        END
                                        WHEN (imv_prod."COUNTRY_CODE") = 'GB' THEN CASE
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") < 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN -2.99999999
                                            WHEN (imv_prod."IMV_FINAL_SCORE") > -3.0
                                            AND (imv_prod."IMV_FINAL_SCORE") < -1.75 THEN -1.75
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -1.75
                                            AND (imv_prod."IMV_FINAL_SCORE") < -0.84 THEN -0.84
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= -0.84
                                            AND (imv_prod."IMV_FINAL_SCORE") < 0.39 THEN 0.39
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 0.39
                                            AND (imv_prod."IMV_FINAL_SCORE") < 1.33 THEN 1.33
                                            WHEN (imv_prod."IMV_FINAL_SCORE") >= 1.33
                                            AND (imv_prod."IMV_FINAL_SCORE") < 3 THEN NULL
                                            WHEN (
                                                CASE
                                                    WHEN imv_prod."IMV_FINAL_RATING" IN ('FAIR_PRICE', 'OK_PRICE') THEN 'Fair Price'
                                                    WHEN imv_prod."IMV_FINAL_RATING" = 'POOR_PRICE' THEN 'High Price'
                                                    ELSE INITCAP(
                                                        REPLACE(LOWER(imv_prod."IMV_FINAL_RATING"), '_', ' ')
                                                    )
                                                END
                                            ) = 'Outlier'
                                            AND (imv_prod."IMV_NO_DEALER_SCORE") > 0
                                            AND (imv_prod."IMV_FINAL_SCORE") IS NULL THEN 2.9999999999
                                        END
                                    END
                                ) - (imv_prod."IMV_FINAL_SCORE")
                            ) * SQRT((imv_prod."REGRESSION_VARIANCE"))
                            ELSE NULL
                        END
                      , 0
                    )
                )
            ) AS price_at_next_deal_rating
        FROM
            cte_emails_and_listings_dedup AS nbdr_emails
            LEFT JOIN warehouse.logs.imv_prod AS imv_prod ON nbdr_emails.inventory_listing_id = imv_prod.inventory_listing_id
            AND nbdr_emails.service_provider_id = imv_prod.service_provider_id
            AND nbdr_emails._region_ = imv_prod._region_
            AND nbdr_emails.sent_date = imv_prod.process_start_time::DATE -- IMV price + deal rating info on same day of NBDR email send 
            AND imv_prod.process_start_time::DATE >= $imv_start_dt
            AND imv_prod.process_start_time::DATE <= $imv_end_dt
            AND imv_prod.is_new = FALSE -- deal rating only applicable to USED listings
            LEFT JOIN "ANALYTICS"."IMV2"."DENORMALIZED_PREDICTIONS" AS denormalized_predictions 
            on denormalized_predictions.inventory_listing_id = imv_prod.inventory_listing_id
            and denormalized_predictions.creation_date = imv_prod.process_start_time::DATE
            and denormalized_predictions.country = 'GB' -- IMV2 only applicable for UK 
            AND denormalized_predictions.country = imv_prod.country_code
    )
SELECT
    *
  , COALESCE(
        LEAD(sent_date) OVER (
            PARTITION BY
                service_provider_id
              , _region_
              , inventory_listing_id
            ORDER BY
                sent_date asc
        )
      , CURRENT_DATE
    ) AS next_sent_date
FROM
    cte_original_deal_rating_price
;
--------------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------------
-- Next, get a time series of all the listings AFTER the NBDR sent date and up to next sent date
-- flag as instances where the NBDR email price was lowered and the deal rating was changed as a result
 CREATE OR REPLACE TABLE
sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_DEC_12_02 AS
WITH
    cte_dats AS (
        SELECT
            imv_prod.inventory_listing_id
          , imv_prod.service_provider_id
          , imv_prod._region_
          , imv_prod.listing_area_id
          , o.maker_name
          , o.model_name
          , o.year AS model_year
          , imv_prod.process_start_time::DATE AS DATE
          , price
          , CASE 
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'FAIR_DEAL' THEN 'FAIR_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'HIGH_PRICE' THEN 'POOR_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'LOW_OUTLIER' THEN 'OUTLIER'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'GOOD_DEAL' THEN 'GOOD_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'GREAT_DEAL' THEN 'GREAT_PRICE'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'HIGH_OUTLIER' THEN 'OUTLIER'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'OVERPRICED' THEN 'OVERPRICED'
                WHEN imv_prod.country_code = 'GB' AND denormalized_predictions.imv2_base_deal_rating = 'NPA' THEN 'NA'
                ELSE imv_prod.deal_rating
                END as deal_rating
          , dts.sent_date
          , dts.next_sent_date
          , dts.open_count
          , dts.click_count
          , dts.unsubscribe_count
          , dts.original_price
          , dts.original_deal_rating
          , dts.price_at_next_deal_rating
          , COUNT(*) AS row_ctr
        FROM
            warehouse.logs.imv_prod AS imv_prod
            LEFT JOIN ANALYTICS.INVENTORY.VEHICLE_ONTOLOGY o ON imv_prod.listing_entity_id = o.entity_id
            AND imv_prod._region_ = o.region
            LEFT JOIN "ANALYTICS"."IMV2"."DENORMALIZED_PREDICTIONS" AS denormalized_predictions 
            on denormalized_predictions.inventory_listing_id = imv_prod.inventory_listing_id
            and denormalized_predictions.creation_date = imv_prod.process_start_time::DATE
            and denormalized_predictions.country = 'GB' -- IMV2 only applicable for UK 
            AND denormalized_predictions.country = imv_prod.country_code
            JOIN sandbox_bmorais.competitive_intelligence.NBDR_LISTING_PRICE_AT_SEND_BY_COUNTRY_DEC_12_02 AS dts ON dts.service_provider_id = imv_prod.service_provider_id
            and dts._region_ = imv_prod._region_
            AND dts.inventory_listing_id = imv_prod.inventory_listing_id
            AND imv_prod.process_start_time::DATE >= dts.imv_date
            AND imv_prod.process_start_time::DATE < dts.next_sent_date -- filter + expand by IMV from NBDR email send date to next NBDR email send date 
        WHERE
            imv_prod.process_start_time::DATE >= $imv_start_dt
            AND imv_prod.process_start_time::DATE <= $imv_end_dt
            AND imv_prod.is_new = FALSE -- deal rating only applicable to USED listings
        GROUP BY
            1
          , 2
          , 3
          , 4
          , 5
          , 6
          , 7
          , 8
          , 9
          , 10
          , 11
          , 12
          , 13
          , 14
          , 15
          , 16
          , 17
          , 18
        ORDER BY
            1
          , 2
          , 3
          , 4
          , 5
          , 6
          , 7
          , 8
          , 9
          , 10
          , 11
          , 12
          , 13
          , 14
          , 15
          , 16
          , 17
          , 18
    )
SELECT
    inventory_listing_id
  , service_provider_id
  , _region_
  , listing_area_id
  , sent_date
  , next_sent_date
  , open_count
  , DATE
  , original_price
  , price
  , IFNULL(price - original_price, 0) AS price_delta
    -- flag changes price

  , CASE
        WHEN price < original_price THEN 'yes'
        ELSE 'no'
    END AS price_lowered_flag
    -- flag changes in deal rating

  , original_deal_rating
  , deal_rating
  , CASE
        WHEN original_deal_rating <> deal_rating THEN 'yes'
        ELSE 'no'
    END AS deal_rating_change_flag

  -- carry next day deal rating to indicate persistence change
  , LEAD(deal_rating) OVER (PARTITION BY service_provider_id, inventory_listing_id ORDER BY date asc) as next_day_deal_rating

    -- flag price lowered + deal rating change as a result of NBDR emails

  , CASE
        WHEN price_lowered_flag = 'yes'
        AND deal_rating_change_flag = 'yes' THEN 'yes'
         AND open_count > 0 
        ELSE 'no'
    END AS nbdr_change_flag
FROM
    cte_dats AS dats
WHERE sent_date <> date -- price + deal rating in NBDR email will be same on sent_date = date, let's ignore those records as dealers can't have their prices persist during the same date  
;
----------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------
-- Ensure NBDR price changes stick (persist) more than 1 day
CREATE OR REPLACE TABLE sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_W_PERSISTENCE_DEC_12_02 AS 
WITH
    cte_filtered AS (
        SELECT
            service_provider_id
          , _region_
          , inventory_listing_id
          , listing_area_id
          , original_deal_rating
          , original_price
          , COUNT(DISTINCT DATE) AS count_nbdr_changes
          , MIN(DATE) AS min_nbdr_change_date
        FROM
            sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_DEC_12_02
        WHERE
            nbdr_change_flag = 'yes'
        GROUP BY
            1
          , 2
          , 3
          , 4
          , 5
          , 6
    )
, cte_deal_rating_at_min_nbdr_change_date as (
SELECT
    dt1.service_provider_id
    , dt1._region_
          , dt1.inventory_listing_id
          , dt1.listing_area_id
          , dt2.min_nbdr_change_date
   , dt1.next_day_deal_rating as deal_rating_at_min_nbdr_change_date_for_send
   , dt1.price as price_at_min_nbdr_change_date_for_send
   , count(*) as row_ctr
FROM
    sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_DEC_12_02 AS dt1
    JOIN cte_filtered AS dt2 ON dt2.service_provider_id = dt1.service_provider_id
    AND dt2._region_ = dt1._region_
    AND dt2.inventory_listing_id = dt1.inventory_listing_id
    AND dt2.listing_area_id = dt1.listing_area_id
    AND dt2.original_deal_rating = dt1.original_deal_rating
    AND dt2.original_price = dt1.original_price
where COALESCE(dt2.count_nbdr_changes, 0) > 0 -- filter to only where the a dealer lowered price + changed deal rating during the timeframe between NBDR emails  
    AND min_nbdr_change_date = date
group by 1, 2, 3, 4, 5, 6, 7
order by dt2.min_nbdr_change_date asc
)
, cte_merged as (
SELECT
    dt1.*
    , dt2.min_nbdr_change_date
    , dt3.deal_rating_at_min_nbdr_change_date_for_send
    , dt3.price_at_min_nbdr_change_date_for_send
FROM
    sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_DEC_12_02 AS dt1
    LEFT JOIN cte_filtered AS dt2 ON dt2.service_provider_id = dt1.service_provider_id
    AND dt2._region_ = dt1._region_
    AND dt2.inventory_listing_id = dt1.inventory_listing_id
    AND dt2.listing_area_id = dt1.listing_area_id
    -- this was changed...
  --  AND dt2.original_deal_rating = dt1.original_deal_rating
  --  AND dt2.original_price = dt1.original_price

    LEFT JOIN cte_deal_rating_at_min_nbdr_change_date AS dt3 ON dt3.service_provider_id = dt1.service_provider_id
    AND dt3._region_ = dt1._region_
    AND dt3.inventory_listing_id = dt1.inventory_listing_id
    AND dt3.listing_area_id = dt1.listing_area_id
    AND dt3.min_nbdr_change_date = dt2.min_nbdr_change_date    
WHERE
   COALESCE(dt2.count_nbdr_changes, 0) > 0 -- filter to only where the a dealer lowered price + changed deal rating during the timeframe between NBDR emails  
)
, cte_persistence_summary as (
select service_provider_id
     , _region_
     , inventory_listing_id
     , listing_area_id
     , min_nbdr_change_date
     , deal_rating_at_min_nbdr_change_date_for_send
     , count(distinct case when deal_rating = deal_rating_at_min_nbdr_change_date_for_send then date else null end) as num_days_persistence
from cte_merged
where date > min_nbdr_change_date -- only count days AFTER the NBDR DEAL RATING CHANGE OCCURRED
and min_nbdr_change_date < $imv_end_dt -- filter out changes that occurred on the last possible date of when the data is pull
group by 1, 2, 3, 4, 5, 6
having num_days_persistence > 0
)
select *
from cte_persistence_summary
----------------------------------------------------------------------------------------------------------------------




----------------------------------------------------------------------------------------------------------------------
-- Change final CTE group by to get associated summaries
select  
/* 
-- include fields if you want SPID level summary
        dats.service_provider_id,
        dats._region_,
*/
        account_category,
     case when min_nbdr_change_date < $performance_sent_dt THEN 'PRE' ELSE 'POST' end as time_period
     , count(distinct concat(dats.service_provider_id, dats._region_, inventory_listing_id, listing_area_id, min_nbdr_change_date)) as listing_deal_change_count
      , count(distinct concat(dats.service_provider_id, dats._region_)) as dealer_deal_change_count,
      listing_deal_change_count / dealer_deal_change_count as listings_updated_per_dealer
      -- , dealer_deal_change_count / dealer_count as pct_dealers_changing
from sandbox_bmorais.competitive_intelligence.NBDR_CHANGES_FLAGGED_BY_COUNTRY_W_PERSISTENCE_DEC_12_02 as dats
left join warehouse.utils.datedimension as dd on dd.calendardate = dats.min_nbdr_change_date
LEFT JOIN WAREHOUSE.SITE.SERVICE_PROVIDERS AS service_providers ON service_providers.location_id = dats.service_provider_id
and service_providers._region_ = dats._region_
LEFT JOIN analytics.dealers.service_providers AS cargurus_service_providers ON service_providers.LOCATION_ID = (cargurus_service_providers."SERVICE_PROVIDER_ID")
      and service_providers._region_ = (cargurus_service_providers."REGION")
LEFT JOIN analytics.dealers.dealers  AS dealers ON (cargurus_service_providers."SF_ACCOUNT_ID") = (dealers."SF_ACCOUNT_ID")
AND service_providers._region_ = dats._region_
group by ALL
--order by 1, 2 desc
----------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------
-- dealer counts for each account_category associated with dealers who were sent or opened Performance Health email
select account_category
    , count(distinct dats.service_provider_id) as dealer_count
from sandbox_bmorais.competitive_intelligence.PERFORMANCE_HEALTH_DEALER_LIST as dats
LEFT JOIN WAREHOUSE.SITE.SERVICE_PROVIDERS AS service_providers ON service_providers.location_id = dats.service_provider_id
and service_providers._region_ = dats._region_
LEFT JOIN analytics.dealers.service_providers AS cargurus_service_providers ON service_providers.LOCATION_ID = (cargurus_service_providers."SERVICE_PROVIDER_ID")
      and service_providers._region_ = (cargurus_service_providers."REGION")
LEFT JOIN analytics.dealers.dealers  AS dealers ON (cargurus_service_providers."SF_ACCOUNT_ID") = (dealers."SF_ACCOUNT_ID")
AND service_providers._region_ = dats._region_
group by 1
----------------------------------------------------------------------------------------------------------------------
