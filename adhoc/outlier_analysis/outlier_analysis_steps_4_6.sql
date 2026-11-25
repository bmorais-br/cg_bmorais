-- Step 3 fix: in dealer_months, replace b.current_account_category_simplified with
-- b.current_account_category (outlier_base only carries current_account_category), and
-- do the same in the final select / group by / order by.

-- =====================================================================
-- Step 4: Dealer-month signals (churn / inventory blackout / deal-rating mix)
-- One row per dealer x month for every dealer in outlier_base, so outliers
-- can be compared with non-outliers. Metric-independent.
--
-- Churn / downgrade (analytics.mrr.daily_paying_subscriptions, daily grain), tier-based:
--   Tier ladder: Featured Priority (4) > Featured (3) > Enhanced (2) > Standard (1) > none (0).
--   An account-day takes its highest core tier; add-ons (Digital Retail, Pricing Tool, ...) and no row at all rank 0 = non-paying.
--   Tier is read on the LAST DAY of each month, so a mid-month cancellation shows up as
--   a lower month-end tier. Per reference month M, tier at end of M vs. end of M-1:
--     'churned'        had a core tier at end of M-1, none at end of M
--     'downgrade'      lower tier (e.g. Featured -> Enhanced)
--     'upgrade / new'  higher tier, or none -> tier
--     'none'           same tier
--     'non-paying'   no core tier at either month end
--   Limitation: a dealer that churned in M-1 and had no tier at either month end is
--   'non-paying' in M (it was flagged 'churned' in its own M-1 row).
--   Bridge to the dealer: comparison_monthly.sf_account_id (latest in month).
--
-- Inventory blackout (comparison_monthly, total inventory, not used-only):
--   avg_daily_inventory MoM change <= -50% (avg_inventory_mom_pct is stored in percent, e.g. -50 = -50%), OR >= 7 days in the month with no inventory.
--   dates_in_month is used because avg_daily_inventory only averages days that have rows,
--   so a mid-month outage does not drag the average down.
--   The same test is applied to M-1 (vs. M-2) as is_inventory_blackout_prior, giving blackout_pattern:
--   'repeat blackout' (M and M-1), 'new blackout' (M only), 'recovered' (M-1 only), 'no blackout'.
--
-- Deal-rating mix (used inventory, share of total_used_inventory_dates):
--   good_or_better = great + good; bad = high price + overpriced.
--   "worsened" = bad share up > 5pp OR good_or_better share down > 5pp vs. prior month.
-- Thresholds are inline below; change them there.
-- =====================================================================
create or replace transient table sandbox_bmorais.outlier_analysis.dealer_month_signals as
with base_dealers as (
    select distinct service_provider_id, inventory_date
    from sandbox_bmorais.outlier_analysis.outlier_base
),

comp as (
    select
        service_provider_id
        , inventory_date
        , sf_account_id
        , avg_daily_inventory
        , dates_in_month
        , day(last_day(inventory_date))                                              as days_in_month
        , total_used_inventory_dates
        , total_great_deal_used_inventory_dates
        , total_good_deal_used_inventory_dates
        , total_fair_deal_used_inventory_dates
        , total_high_price_used_inventory_dates
        , total_overpriced_used_inventory_dates
        , total_unrated_used_inventory_dates
    from analytics.unified_dealer_data_mart.performance_health_metric_comparison_monthly
    where country_code = 'US'
        and region = 'NA'
        and inventory_date between '2026-03-01' and '2026-10-01'                       -- window + 2 prior months (blackout check needs M-2)
),

-- Core listings tier per account-day (highest tier wins; add-on products rank 0)
acct_day as (
    select
        sf_account_id
        , calendar_date
        , max(case sub_product_family
                when 'Featured Priority' then 4
                when 'Featured'          then 3
                when 'Enhanced'          then 2
                when 'Standard'          then 1
                else 0
              end)                                                                    as tier_rank
    from analytics.mrr.daily_paying_subscriptions
    where calendar_date >= '2026-04-01' and calendar_date < '2026-11-01'              -- complete months only
    group by all
),

-- Tier on the last day of each month (no row that day = no core tier = 0)
acct_month_end as (
    select
        sf_account_id
        , date_trunc('month', calendar_date)                                          as month_start
        , tier_rank
        , decode(tier_rank, 4, 'Featured Priority', 3, 'Featured', 2, 'Enhanced', 1, 'Standard', 'Non-paying') as tier_name
    from acct_day
    where calendar_date = last_day(calendar_date)
),

joined as (
    select
        d.service_provider_id
        , d.inventory_date
        , cur.sf_account_id
        -- churn
        , coalesce(sp.tier_name, 'Non-paying')                                              as tier_prior
        , coalesce(sm.tier_name, 'Non-paying')                                              as tier_current
        , case
            when coalesce(sp.tier_rank, 0) = 0 and coalesce(sm.tier_rank, 0) = 0      then 'non-paying'
            when coalesce(sp.tier_rank, 0) > 0 and coalesce(sm.tier_rank, 0) = 0      then 'churned'
            when coalesce(sm.tier_rank, 0) < coalesce(sp.tier_rank, 0)                then 'downgrade'
            when coalesce(sm.tier_rank, 0) > coalesce(sp.tier_rank, 0)                then 'upgrade / new'
            else 'none'
          end                                                                         as churn_type
        , coalesce(sm.tier_rank, 0) - coalesce(sp.tier_rank, 0)                       as tier_change
        -- inventory blackout
        , cur.avg_daily_inventory
        , pri.avg_daily_inventory                                                     as avg_daily_inventory_prior
        , 100 * (cur.avg_daily_inventory / nullif(pri.avg_daily_inventory, 0) - 1)      as avg_inventory_mom_pct
        , cur.days_in_month - cur.dates_in_month                                      as days_without_inventory
        -- same blackout inputs for the prior month (M-1 vs. M-2)
        , 100 * (pri.avg_daily_inventory / nullif(pri2.avg_daily_inventory, 0) - 1)    as avg_inventory_mom_pct_prior
        , pri.days_in_month - pri.dates_in_month                                      as days_without_inventory_prior
        -- deal-rating mix (% of used inventory)
        , 100 * cur.total_great_deal_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)     as pct_great_current
        , 100 * cur.total_good_deal_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)      as pct_good_current
        , 100 * cur.total_fair_deal_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)      as pct_fair_current
        , 100 * cur.total_high_price_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)     as pct_high_price_current
        , 100 * cur.total_overpriced_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)     as pct_overpriced_current
        , 100 * cur.total_unrated_used_inventory_dates / nullif(cur.total_used_inventory_dates, 0)        as pct_unrated_current
        , 100 * (cur.total_great_deal_used_inventory_dates + cur.total_good_deal_used_inventory_dates)
              / nullif(cur.total_used_inventory_dates, 0)                             as pct_good_or_better_current
        , 100 * (pri.total_great_deal_used_inventory_dates + pri.total_good_deal_used_inventory_dates)
              / nullif(pri.total_used_inventory_dates, 0)                             as pct_good_or_better_prior
        , 100 * (cur.total_high_price_used_inventory_dates + cur.total_overpriced_used_inventory_dates)
              / nullif(cur.total_used_inventory_dates, 0)                             as pct_bad_current
        , 100 * (pri.total_high_price_used_inventory_dates + pri.total_overpriced_used_inventory_dates)
              / nullif(pri.total_used_inventory_dates, 0)                             as pct_bad_prior
    from base_dealers d
    inner join comp cur
        on d.service_provider_id = cur.service_provider_id
        and d.inventory_date = cur.inventory_date
    left join comp pri
        on d.service_provider_id = pri.service_provider_id
        and pri.inventory_date = dateadd('month', -1, d.inventory_date)
    left join comp pri2
        on d.service_provider_id = pri2.service_provider_id
        and pri2.inventory_date = dateadd('month', -2, d.inventory_date)
    left join acct_month_end sm
        on cur.sf_account_id = sm.sf_account_id
        and sm.month_start = d.inventory_date
    left join acct_month_end sp
        on cur.sf_account_id = sp.sf_account_id
        and sp.month_start = dateadd('month', -1, d.inventory_date)
)

select
    *
    , churn_type = 'churned'                                                          as is_churned
    , churn_type = 'downgrade'                                                        as is_downgrade
    , (avg_inventory_mom_pct <= -50 or days_without_inventory >= 7)                  as is_inventory_blackout
    , (avg_inventory_mom_pct_prior <= -50 or days_without_inventory_prior >= 7)      as is_inventory_blackout_prior
    , case
        when is_inventory_blackout and coalesce(is_inventory_blackout_prior, false)       then 'repeat blackout (also prior month)'
        when is_inventory_blackout                                                        then 'new blackout'
        when coalesce(is_inventory_blackout_prior, false)                                 then 'recovered (blackout last month only)'
        else 'no blackout'
      end                                                                         as blackout_pattern
    , pct_good_or_better_current - pct_good_or_better_prior                           as good_or_better_change_pp
    , pct_bad_current - pct_bad_prior                                                 as bad_change_pp
    , (pct_bad_current - pct_bad_prior >= 5
        or pct_good_or_better_current - pct_good_or_better_prior <= -5)                as is_deal_mix_worsened
from joined
;

-- =====================================================================
-- Step 5: Lower-outlier dealers with their signals (dealer-level detail)
-- =====================================================================
select
    b.metric_name
    , b.inventory_date                                                                as reference_month
    , b.service_provider_id
    , s.sf_account_id
    , b.inventory_size_bucket
    , b.current_account_category
    , round(b.mom_pct, 2)                                                             as mom_pct
    , round(b.distance_to_lower_fence, 2)                                             as distance_to_lower_fence_pp
    , s.churn_type
    , s.tier_prior
    , s.tier_current
    , round(s.avg_inventory_mom_pct, 2)                                               as avg_inventory_mom_pct
    , s.days_without_inventory
    , s.is_inventory_blackout
    , s.is_inventory_blackout_prior
    , s.blackout_pattern
    , round(s.good_or_better_change_pp, 2)                                            as good_or_better_change_pp
    , round(s.bad_change_pp, 2)                                                       as bad_change_pp
    , s.is_deal_mix_worsened
    , (s.is_churned or s.is_downgrade or s.is_inventory_blackout)                     as has_churn_or_blackout
from sandbox_bmorais.outlier_analysis.outlier_base b
inner join sandbox_bmorais.outlier_analysis.dealer_month_signals s
    on b.service_provider_id = s.service_provider_id
    and b.inventory_date = s.inventory_date
where b.is_lower_outlier
    and b.metric_name like 'LEADS_PER_UNIT%'
order by b.inventory_date, b.inventory_size_bucket, b.current_account_category, b.distance_to_lower_fence
;

-- =====================================================================
-- Step 6: Outliers vs. non-outliers on the signals, per reference month
-- Rates are % of dealers in the group. Cells with fewer than 10 dealers are dropped.
-- =====================================================================
select
    b.metric_name
    , b.inventory_date                                                                as reference_month
    , b.inventory_size_bucket
    , b.current_account_category
    , iff(b.is_lower_outlier, '🔴 lower outlier', '🟢 non-outlier')                         as dealer_group
    , count(*)                                                                        as dealers
    , round(100 * count_if(s.is_churned) / count(*), 2)                               as churned_pct
    , round(100 * count_if(s.is_downgrade) / count(*), 2)                             as downgrade_pct
    , round(100 * count_if(s.is_inventory_blackout) / count(*), 2)                    as inventory_blackout_pct
    , round(100 * count_if(s.blackout_pattern = 'repeat blackout (also prior month)') / count(*), 2) as repeat_blackout_pct
    , round(100 * count_if(s.blackout_pattern = 'new blackout') / count(*), 2)        as new_blackout_pct
    , round(100 * count_if(s.is_churned or s.is_downgrade or s.is_inventory_blackout) / count(*), 2)
                                                                                      as any_churn_or_blackout_pct
    , round(avg(s.pct_great_current), 2)                                              as avg_pct_great
    , round(avg(s.pct_good_current), 2)                                               as avg_pct_good
    , round(avg(s.pct_fair_current), 2)                                               as avg_pct_fair
    , round(avg(s.pct_high_price_current), 2)                                         as avg_pct_high_price
    , round(avg(s.pct_overpriced_current), 2)                                         as avg_pct_overpriced
    , round(avg(s.pct_unrated_current), 2)                                            as avg_pct_unrated
    , round(avg(s.good_or_better_change_pp), 2)                                       as avg_good_or_better_change_pp
    , round(avg(s.bad_change_pp), 2)                                                  as avg_bad_change_pp
    , round(100 * count_if(s.is_deal_mix_worsened) / count(s.is_deal_mix_worsened), 2) as deal_mix_worsened_pct
from sandbox_bmorais.outlier_analysis.outlier_base b
inner join sandbox_bmorais.outlier_analysis.dealer_month_signals s
    on b.service_provider_id = s.service_provider_id
    and b.inventory_date = s.inventory_date
where b.metric_name like 'LEADS_PER_UNIT%'
group by all
having dealers >= 10
order by metric_name, reference_month, inventory_size_bucket, current_account_category, dealer_group
;
