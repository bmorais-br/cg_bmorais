-- Follow-up queries on outlier_base + dealer_month_signals (run after Steps 1 and 4).
-- Change the metric in the where clause to look at another metric.
--
-- Reference months to look at (inclusive, first of the month). Single month = same start and end.
-- The tables only contain 2026-05-01 to 2026-10-01 (the Step 1 window), so stay inside that range.
set start_month = '2026-10-01';
set end_month   = '2026-10-01';

-- =====================================================================
-- A. Summary by peer group: reference month x size bucket x account category
--    This is the group the flag is defined in (percentile ranks and the outlier flag are
--    computed within each month x size bucket x account category), so flagged and not-flagged
--    dealers are compared only against dealers in the same peer group.
--    (dealer-level, includes every dealer, so totals differ slightly from Step 6,
--     which drops cells with fewer than 10 dealers)
--    Small cells are noisy: check the dealers column before reading the percentages.
-- =====================================================================
with d as (
    select
        b.inventory_date
        , b.inventory_size_bucket
        , b.current_account_category
        , iff(b.is_lower_outlier, '🔴 lower outlier', '🟢 non-outlier')              as dealer_group
        , s.is_churned
        , s.is_downgrade
        , s.is_inventory_blackout
        , s.is_deal_mix_worsened
        , s.pct_unrated_current
    from sandbox_bmorais.outlier_analysis.outlier_base b
    inner join sandbox_bmorais.outlier_analysis.dealer_month_signals s
        on b.service_provider_id = s.service_provider_id
        and b.inventory_date = s.inventory_date
    where b.metric_name like 'VDP_VIEWS_PER_UNIT%'
        and b.inventory_date between $start_month::date and $end_month::date
)
select
    inventory_date                                                                    as reference_month
    , inventory_size_bucket
    , current_account_category
    , dealer_group
    , count(*)                                                                        as dealers   -- dealers in this group
    , round(100 * count_if(is_churned) / count(*), 1)                                 as churned_pct
    , round(100 * count_if(is_downgrade) / count(*), 1)                               as downgrade_pct          -- lower paying tier (churn excluded)
    , round(100 * count_if(is_churned or is_downgrade) / count(*), 1)                 as any_tier_drop_pct      -- downgrade OR churn
    , round(100 * count_if(is_inventory_blackout) / count(*), 1)                      as inventory_blackout_pct
    , round(100 * count_if(is_churned or is_downgrade or is_inventory_blackout) / count(*), 1) as any_churn_or_blackout_pct
    , round(avg(pct_unrated_current), 1)                                              as avg_pct_unrated
    , round(100 * count_if(is_deal_mix_worsened) / count(is_deal_mix_worsened), 1)    as deal_mix_worsened_pct
from d
group by all
order by reference_month, inventory_size_bucket, current_account_category, dealer_group
;

-- =====================================================================
-- B. Unexplained lower outliers: no churn, downgrade or blackout (dealer level)
-- =====================================================================
select
    b.inventory_date
    , b.service_provider_id
    , s.sf_account_id
    , b.inventory_size_bucket
    , b.current_account_category
    , b.mom_pct
    , s.churn_type
    , s.tier_current
    , round(s.avg_inventory_mom_pct, 1)                                               as avg_inventory_mom_pct
    , s.days_without_inventory
    , round(s.pct_unrated_current, 1)                                                 as pct_unrated
    , round(s.good_or_better_change_pp, 1)                                            as good_or_better_change_pp
    , round(s.bad_change_pp, 1)                                                       as bad_change_pp
    , s.is_deal_mix_worsened                                                          -- added: bad share up >5pp OR good-or-better share down >5pp
from sandbox_bmorais.outlier_analysis.outlier_base b
inner join sandbox_bmorais.outlier_analysis.dealer_month_signals s
    on b.service_provider_id = s.service_provider_id
    and b.inventory_date = s.inventory_date
where b.is_lower_outlier
    and b.metric_name like 'VDP_VIEWS_PER_UNIT%'
    and b.inventory_date between $start_month::date and $end_month::date
    and not (s.is_churned or s.is_downgrade or s.is_inventory_blackout)
order by b.inventory_date, b.mom_pct
;
