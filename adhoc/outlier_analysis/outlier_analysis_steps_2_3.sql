-- Step 2: Lower outlier rate by segment, per reference month
-- A dealer is a lower outlier only if it is BOTH below p05 AND below
-- the Tukey lower fence, so lower_outlier_rate_pct can never exceed
-- below_p05_rate_pct (~5%). The gap between the two shows how much
-- the fence narrows the p05 criterion in each segment.
-- Units: *_rate_pct are percents of dealers. outlier_base already stores mom_pct,
-- p05, lower_fence and distance_to_lower_fence in percent, so
-- avg_mom_change_of_outliers_pct and avg_distance_below_fence_pp are NOT multiplied by 100 here.

select
    metric_name
    , inventory_date                                                                    as reference_month
    , inventory_size_bucket
    , current_account_category
    , count(*)                                                                          as dealers
    , count_if(mom_pct < p05)                                                           as below_p05
    , count_if(is_lower_outlier)                                                        as lower_outliers
    , round(100 * below_p05 / dealers, 2)                                               as below_p05_rate_pct
    , round(100 * lower_outliers / dealers, 2)                                          as lower_outlier_rate_pct
    , round(avg(case when is_lower_outlier then mom_pct end), 2)                        as avg_mom_change_of_outliers_pct
    , round(avg(case when is_lower_outlier then distance_to_lower_fence end), 2)        as avg_distance_below_fence_pp
from sandbox_bmorais.outlier_analysis.outlier_base
where
    metric_name like 'LEADS_PER_UNIT%'
group by all
order by metric_name, reference_month, inventory_size_bucket, current_account_category
;

-- Step 3: Outliers vs. non-outliers on photos and pricing, per reference month
-- Compares each dealer's reference month with the month before it:
--   % of used inventory without photos, % high-price or overpriced.
-- Units: % of used inventory; changes in percentage points (pp).
-- "worsened" = more than 5pp in the bad direction.
-- Cells with fewer than 10 dealers are dropped.

with dealer_months as (
    select
        b.metric_name
        , b.inventory_size_bucket
        , b.current_account_category
        , b.inventory_date
        , b.service_provider_id
        , b.is_lower_outlier
        , 100 * (1 - pri.total_used_inventory_dates_with_photos
                     / nullif(pri.total_used_inventory_dates, 0))                   as pct_without_photos_prior
        , 100 * (1 - cur.total_used_inventory_dates_with_photos
                     / nullif(cur.total_used_inventory_dates, 0))                   as pct_without_photos_current
        , pct_without_photos_current - pct_without_photos_prior                     as photos_change_pp
        , 100 * (pri.total_high_price_used_inventory_dates + pri.total_overpriced_used_inventory_dates)
              / nullif(pri.total_used_inventory_dates, 0)                           as pct_badly_priced_prior
        , 100 * (cur.total_high_price_used_inventory_dates + cur.total_overpriced_used_inventory_dates)
              / nullif(cur.total_used_inventory_dates, 0)                           as pct_badly_priced_current
        , pct_badly_priced_current - pct_badly_priced_prior                         as pricing_change_pp
    from sandbox_bmorais.outlier_analysis.outlier_base b
    inner join analytics.unified_dealer_data_mart.performance_health_metric_comparison_monthly cur
        on b.service_provider_id = cur.service_provider_id
        and b.region = cur.region
        and cur.country_code = 'US'
        and b.inventory_date = cur.inventory_date
    inner join analytics.unified_dealer_data_mart.performance_health_metric_comparison_monthly pri
        on b.service_provider_id = pri.service_provider_id
        and b.region = pri.region
        and pri.country_code = 'US'
        and pri.inventory_date = dateadd('month', -1, b.inventory_date)
)

select
    metric_name
    , inventory_date                                                               as reference_month
    , inventory_size_bucket
    , current_account_category
    , iff(is_lower_outlier, '🔴 lower outlier', '🟢 non-outlier')                        as dealer_group
    , count(*)                                                                     as dealers
    , round(avg(pct_without_photos_prior), 2)                                      as without_photos_avg_prior_pct
    , round(avg(pct_without_photos_current), 2)                                    as without_photos_avg_current_pct
    , round(avg(photos_change_pp), 2)                                              as without_photos_avg_change_pp
    , round(100 * count_if(photos_change_pp > 5) / count(photos_change_pp), 2)     as without_photos_pct_worsened_5pp
    , round(avg(pct_badly_priced_prior), 2)                                        as pricing_avg_prior_pct
    , round(avg(pct_badly_priced_current), 2)                                      as pricing_avg_current_pct
    , round(avg(pricing_change_pp), 2)                                             as pricing_avg_change_pp
    , round(100 * count_if(pricing_change_pp > 5) / count(pricing_change_pp), 2)   as pricing_pct_worsened_5pp
from dealer_months
group by all
having dealers >= 10
order by metric_name, reference_month, inventory_size_bucket, current_account_category, dealer_group
;
