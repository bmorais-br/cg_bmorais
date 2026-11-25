-- Outlier base, MoM-change method (Tukey lower fence), segmented distributions
--
-- Stats (percentiles, fences) are computed per metric × month ×
-- inventory_size_bucket × current_account_category so each dealer
-- is flagged relative to their own peer group, not the full population.
--
-- Segments with fewer than 30 dealers are excluded from outlier
-- flagging — percentile estimates are unreliable at small n.

create schema if not exists sandbox_bmorais.outlier_analysis;
create or replace transient table sandbox_bmorais.outlier_analysis.outlier_base as
with unpivoted as (
    select
        service_provider_id
        , inventory_date
        , region
        , country_code
        , current_account_category
        , avg_daily_used_inventory
        , case
            when avg_daily_used_inventory < 50   then '< 50'
            when avg_daily_used_inventory <= 150  then '50-150'
            else '> 150'
          end                                                         as inventory_size_bucket
        , metric_name
        , mom_pct
    from analytics.unified_dealer_data_mart.performance_health_metric_monthly_changes
    unpivot (mom_pct for metric_name in (
        leads_per_unit_used_inventory_mom_pct,
        vdp_views_per_unit_used_inventory_mom_pct,
        connections_per_unit_used_inventory_mom_pct,
        srp_impressions_per_unit_used_inventory_mom_pct
    ))
    where
        mom_pct is not null
        and country_code = 'US'
        and region = 'NA'
        and inventory_date between '2026-05-01' and '2026-10-01'   -- adjust window here
),

stats as (
    select
        metric_name
        , inventory_date
        , inventory_size_bucket
        , current_account_category
        , count(*)                                                   as segment_dealer_count
        , percentile_cont(0.05) within group (order by mom_pct)      as p05
        , percentile_cont(0.25) within group (order by mom_pct)      as q1
        , percentile_cont(0.75) within group (order by mom_pct)      as q3
        , q3 - q1                                                    as iqr
        , q1 - 1.5 * iqr                                             as lower_fence
    from unpivoted
    group by
        metric_name
        , inventory_date
        , inventory_size_bucket
        , current_account_category
    having count(*) >= 30                                            -- exclude unreliable small segments
)

select
    u.service_provider_id
    , u.inventory_date
    , u.region
    , u.current_account_category
    , u.avg_daily_used_inventory
    , u.inventory_size_bucket
    , u.metric_name
    , round(100*u.mom_pct, 4)                                            as mom_pct
    , s.segment_dealer_count
    , round(100*s.p05, 4)                                                as p05
    , round(100*s.lower_fence, 4)                                        as lower_fence
    , (u.mom_pct < s.p05 and u.mom_pct < s.lower_fence)             as is_lower_outlier
    , round(100*u.mom_pct - 100*s.lower_fence, 4)                           as distance_to_lower_fence
from unpivoted u
inner join stats s
    on u.metric_name = s.metric_name
    and u.inventory_date = s.inventory_date
    and u.inventory_size_bucket = s.inventory_size_bucket
    and u.current_account_category = s.current_account_category
order by u.inventory_date, u.metric_name, u.mom_pct

;

select
    metric_name
    , inventory_size_bucket
    , current_account_category
    , count(distinct case when lower_fence <= -100 then inventory_date else null end)   as unreachable_fence_months
    , count(distinct inventory_date)                                                as total_months
from sandbox_bmorais.outlier_analysis.outlier_base
group by all
order by 1, 2, 3
--where metric_name = 'CONNECTIONS_PER_UNIT_USED_INVENTORY_MOM_PCT' and inventory_date = '2026-10-01' and current_account_category = 'Franchise - Large'
--and is_lower_outlier
--order by inventory_size_bucket, current_account_category;
