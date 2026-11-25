-- Step 1b: Outlier base, percentile-position method (alternative to the MoM-change method in Step 1)
--
-- Instead of taking the MoM % change of each metric and finding outliers in that distribution:
--   1. Rank every dealer on the metric itself (per-unit rate) within its peer group and month,
--      giving a percentile rank (0-100).
--   2. The dealer's change = percentile rank this month - percentile rank last month, in
--      percentile positions (e.g. p65 -> p40 = -25).
--   3. The distribution of those position changes, per metric x month x peer group, gives the
--      Tukey lower fence (Q1 - 1.5 * IQR). A dealer that dropped further than the fence is flagged.
--
-- Metrics (per-unit rates from performance_health_metric_monthly_changes):
--   leads_per_unit_used_inventory       (LPU)
--   connections_per_unit_used_inventory (CPU)
--
-- Peer group = inventory_size_bucket x current_account_category (same as Step 1), measured in each
-- month. Ranks are computed within each month's peer group, so a dealer that changes size bucket
-- between months is compared against a different peer group each month.
-- Segments with fewer than 30 dealers are dropped, both for ranking and for the fence.
--
-- Output matches outlier_base (Step 1), except:
--   mom_pct                -> percentile_change   (percentile positions, not a % change)
--   + metric_value, percentile_rank, prior_percentile_rank at the end
-- Units: percentile_change, p05, lower_fence and distance_to_lower_fence are in percentile
-- positions (0-100 scale). Downstream steps join on service_provider_id, inventory_date and
-- metric_name; metric_name starts with LEADS_PER_UNIT / CONNECTIONS_PER_UNIT so existing
-- "like 'LEADS_PER_UNIT%'" filters still work.
--
-- Reference months are 2026-05-01 to 2026-10-01; April is pulled only for the prior-month rank.

create schema if not exists sandbox_bmorais.outlier_analysis;
create or replace transient table sandbox_bmorais.outlier_analysis.outlier_base_pct_changes_method as
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
        , metric_value
    from analytics.unified_dealer_data_mart.performance_health_metric_monthly_changes
    unpivot (metric_value for metric_name in (
        leads_per_unit_used_inventory,
        connections_per_unit_used_inventory
    ))
    where
        metric_value is not null
        and country_code = 'US'
        and region = 'NA'
        and inventory_date between '2026-04-01' and '2026-10-01'   -- adjust window here (start = first reference month - 1)
),

ranked as (
    select
        *
        , count(*) over (
            partition by metric_name, inventory_date, inventory_size_bucket, current_account_category
          )                                                                                as rank_segment_count
        , 100 * percent_rank() over (
            partition by metric_name, inventory_date, inventory_size_bucket, current_account_category
            order by metric_value
          )                                                                                as percentile_rank
    from unpivoted
),

with_prior as (
    select
        *
        , lag(inventory_date) over (
            partition by service_provider_id, metric_name order by inventory_date)         as prior_inventory_date
        , lag(percentile_rank) over (
            partition by service_provider_id, metric_name order by inventory_date)         as prior_percentile_rank
    from ranked
    where rank_segment_count >= 30                                   -- exclude unreliable small segments
),

changes as (
    select
        *
        , percentile_rank - prior_percentile_rank                                          as percentile_change
    from with_prior
    where
        inventory_date >= '2026-05-01'                                                     -- first reference month
        and prior_inventory_date = dateadd('month', -1, inventory_date)                    -- prior row must be the adjacent month
),

stats as (
    select
        metric_name
        , inventory_date
        , inventory_size_bucket
        , current_account_category
        , count(*)                                                   as segment_dealer_count
        , percentile_cont(0.05) within group (order by percentile_change)  as p05
        , percentile_cont(0.25) within group (order by percentile_change)  as q1
        , percentile_cont(0.75) within group (order by percentile_change)  as q3
        , q3 - q1                                                    as iqr
        , q1 - 1.5 * iqr                                             as lower_fence
    from changes
    group by
        metric_name
        , inventory_date
        , inventory_size_bucket
        , current_account_category
    having count(*) >= 30                                            -- exclude unreliable small segments
)

select
    c.service_provider_id
    , c.inventory_date
    , c.region
    , c.current_account_category
    , c.avg_daily_used_inventory
    , c.inventory_size_bucket
    , c.metric_name
    , round(c.percentile_change, 4)                                  as percentile_change
    , s.segment_dealer_count
    , round(s.p05, 4)                                                as p05
    , round(s.lower_fence, 4)                                        as lower_fence
    , (c.percentile_change < s.lower_fence)                          as is_lower_outlier
    , round(c.percentile_change - s.lower_fence, 4)                  as distance_to_lower_fence
    , c.metric_value
    , round(c.percentile_rank, 4)                                    as percentile_rank
    , round(c.prior_percentile_rank, 4)                              as prior_percentile_rank
from changes c
inner join stats s
    on c.metric_name = s.metric_name
    and c.inventory_date = s.inventory_date
    and c.inventory_size_bucket = s.inventory_size_bucket
    and c.current_account_category = s.current_account_category
order by c.inventory_date, c.metric_name, c.percentile_change

;

-- Sanity check: how many dealers are flagged, and can the fence be reached at all?
-- A fence at or below -100 can never flag anyone (a percentile rank can only drop 100 positions).
select
    metric_name
    , inventory_size_bucket
    , current_account_category
    , count(distinct case when lower_fence <= -100 then inventory_date else null end)   as unreachable_fence_months
    , count(distinct inventory_date)                                                    as total_months
    , count_if(is_lower_outlier)                                                        as flagged_dealer_months
    , count(*)                                                                          as dealer_months
from sandbox_bmorais.outlier_analysis.outlier_base_pct_changes_method
group by all
order by 1, 2, 3
;
