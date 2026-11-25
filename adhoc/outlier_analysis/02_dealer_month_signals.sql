-- =====================================================================
-- Dealer-month signals (churn / inventory blackout / deal-rating mix)
-- One row per dealer x month for every dealer in outlier_base, so outliers
-- can be compared with non-outliers. Metric-independent.
-- Built from the dealers in outlier_base (01_outlier_base_mom_tukey.sql). Dealer-months that exist only in
-- the percentile-method table (01b) are not covered (14 over May-Oct at the last check); point base_dealers
-- at a union of both tables if you need them.
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
--   "worsened" = bad share up at least 15pp OR good_or_better share down at least 15pp vs. prior month.
-- Thresholds are inline below; change them there (deal-mix threshold is 15pp; it was 5pp in earlier runs).
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
    , (pct_bad_current - pct_bad_prior >= 15
        or pct_good_or_better_current - pct_good_or_better_prior <= -15)                as is_deal_mix_worsened
from joined
;
