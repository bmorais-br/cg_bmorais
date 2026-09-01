/*
  Pre/Post Pricing Analysis — Below-Benchmark CI Viewers
  Sources:
    - Views:        ANALYTICS.TRAFFIC.DEALER_DASHBOARD_EVENTS_NORMALIZED
    - Pricing:      ANALYTICS.INVENTORY.INVENTORY_LISTINGS_IMV
    - Performance:  ANALYTICS.UNIFIED_DEALER_DATA_MART.PERFORMANCE_HEALTH_METRIC_COMPARISONS

  Design: same pre/post window as price_change_after_ci_view.sql (14 days each side of first CI view).
  Cohort restriction: dealers ≥10% below their competitor set on at least one of leads/unit,
    VDPs/unit, or days on lot — measured over the 14-day pre-window.

  Below-benchmark thresholds (pre-window averages):
    - Leads/unit:   dealer_avg < competitor_avg * 0.90
    - VDPs/unit:    dealer_avg < competitor_avg * 0.90
    - Days on lot:  dealer_avg > competitor_avg * 1.10

  Listing change threshold: |max_price - min_price| > $0.50 within each window

  Responsiveness metric (days_to_first_reprice_post):
    Days from first_view_date to earliest post-window reprice event (delta > $0.50).
    Day +1 = 1. NULL for dealers with no post-window reprice; excluded from summary median.
*/

with
dealer_metadata as (
    select
        service_provider_id
      , current_dealer_name
      , current_account_category_simplified
      , split_part(current_account_category, '-', 2) as dealer_size
    from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
    where region = 'NA'
    qualify row_number() over (partition by region, service_provider_id order by inventory_date desc) = 1
)

, first_ci_views as (
    select
        service_provider_id
      , min(derived_tstamp::date) as first_view_date
    from analytics.traffic.dealer_dashboard_events_normalized
    where sd_application     = 'Dealer_Dashboard'
      and sd_product         in ('Performance', 'Competitors')
      and sd_product_section in ('Performance', 'Competitors')
      and source             = 'cargurus_dealer_pageview_tracking'
      and is_staff           = false
      and is_bot             = false
      and region             = 'NA'
    group by service_provider_id
    having first_view_date <= dateadd('day', -14, current_date())
)

, below_benchmark_dealers as (
    -- Dealers whose pre-window average was ≥10% below competitors on at least one metric.
    select
        p.service_provider_id
      , avg(p.total_leads_used_inventory      / nullif(p.total_used_inventory, 0))              as avg_leads_per_unit
      , avg(p.total_vdp_views_used_inventory  / nullif(p.total_used_inventory, 0))              as avg_vdps_per_unit
      , avg(p.avg_days_on_lot_used_inventory)                                                    as avg_days_on_lot
      , avg(p.competitor_total_leads_used_inventory     / nullif(p.competitor_total_used_inventory, 0)) as avg_comp_leads_per_unit
      , avg(p.competitor_total_vdp_views_used_inventory / nullif(p.competitor_total_used_inventory, 0)) as avg_comp_vdps_per_unit
      , avg(p.competitor_avg_days_on_lot_used_inventory)                                         as avg_comp_days_on_lot
    from analytics.unified_dealer_data_mart.performance_health_metric_comparisons p
    inner join first_ci_views fv on fv.service_provider_id = p.service_provider_id
    where p.inventory_date between dateadd('day', -14, fv.first_view_date)
                                and dateadd('day',  -1, fv.first_view_date)
      and p.country_code         = 'US'
      and p.total_used_inventory > 0
    group by p.service_provider_id
    having
        avg_leads_per_unit < avg_comp_leads_per_unit * 0.90
        or avg_vdps_per_unit  < avg_comp_vdps_per_unit  * 0.90
        or avg_days_on_lot    > avg_comp_days_on_lot    * 1.10
)

, listing_windows as (
    select
        il.inventory_listing_id
      , il.service_provider_id
      , max(case when il.process_start_time::date
                      between dateadd('day', -14, fv.first_view_date)
                          and dateadd('day',  -1, fv.first_view_date)
                 then il.price_shown_on_site end) as pre_window_price_max
      , min(case when il.process_start_time::date
                      between dateadd('day', -14, fv.first_view_date)
                          and dateadd('day',  -1, fv.first_view_date)
                 then il.price_shown_on_site end) as pre_window_price_min
      , max(case when il.process_start_time::date
                      between dateadd('day',   1, fv.first_view_date)
                          and dateadd('day',  14, fv.first_view_date)
                 then il.price_shown_on_site end) as post_window_price_max
      , min(case when il.process_start_time::date
                      between dateadd('day',   1, fv.first_view_date)
                          and dateadd('day',  14, fv.first_view_date)
                 then il.price_shown_on_site end) as post_window_price_min
    from analytics.inventory.inventory_listings_imv il
    inner join first_ci_views fv on fv.service_provider_id = il.service_provider_id
    where il.process_start_time::date
              between dateadd('day', -14, fv.first_view_date)
                  and dateadd('day',  14, fv.first_view_date)
      and il.region              = 'NA'
      and il.listing_status      = 'OPEN'
      and il.is_new              = false
      and il.price_shown_on_site > 0
    group by 1, 2
)

, price_snapshots_labeled as (
    select
        il.service_provider_id
      , il.inventory_listing_id
      , il.process_start_time::date as snapshot_date
      , il.price_shown_on_site
      , case
            when il.process_start_time::date
                     between dateadd('day', -14, fv.first_view_date)
                         and dateadd('day',  -1, fv.first_view_date) then 'pre'
            when il.process_start_time::date
                     between dateadd('day',   1, fv.first_view_date)
                         and dateadd('day',  14, fv.first_view_date) then 'post'
        end as window_label
    from analytics.inventory.inventory_listings_imv il
    inner join first_ci_views fv on fv.service_provider_id = il.service_provider_id
    where il.process_start_time::date
              between dateadd('day', -14, fv.first_view_date)
                  and dateadd('day',  14, fv.first_view_date)
      and il.region              = 'NA'
      and il.listing_status      = 'OPEN'
      and il.is_new              = false
      and il.price_shown_on_site > 0
)

, price_transitions_raw as (
    select
        service_provider_id
      , inventory_listing_id
      , snapshot_date
      , window_label
      , price_shown_on_site
      , lag(price_shown_on_site) over (
            partition by inventory_listing_id, service_provider_id, window_label
            order by snapshot_date
        ) as prev_price
    from price_snapshots_labeled
    where window_label is not null
)

, listing_reprice_events as (
    select
        service_provider_id
      , inventory_listing_id
      , sum(case when window_label = 'pre'  and prev_price is not null
                  and abs(price_shown_on_site - prev_price) > 0.5 then 1 else 0 end) as pre_reprice_events
      , sum(case when window_label = 'post' and prev_price is not null
                  and abs(price_shown_on_site - prev_price) > 0.5 then 1 else 0 end) as post_reprice_events
    from price_transitions_raw
    group by 1, 2
)

, dealer_reprice_events as (
    select
        lre.service_provider_id
      , sum(case when lw.pre_window_price_max is not null and lw.post_window_price_max is not null
                 then lre.pre_reprice_events  else 0 end) as total_reprice_events_pre
      , sum(case when lw.pre_window_price_max is not null and lw.post_window_price_max is not null
                 then lre.post_reprice_events else 0 end) as total_reprice_events_post
    from listing_reprice_events lre
    inner join listing_windows lw
        on  lw.inventory_listing_id = lre.inventory_listing_id
        and lw.service_provider_id  = lre.service_provider_id
    group by 1
)

, dealer_first_reprice as (
    select
        ptr.service_provider_id
      , datediff('day', fv.first_view_date, min(ptr.snapshot_date)) as days_to_first_reprice_post
    from price_transitions_raw ptr
    inner join first_ci_views fv on fv.service_provider_id = ptr.service_provider_id
    where ptr.window_label = 'post'
      and ptr.prev_price   is not null
      and abs(ptr.price_shown_on_site - ptr.prev_price) > 0.5
    group by ptr.service_provider_id, fv.first_view_date
)

, dealer_pp as (
    select
        service_provider_id
      , sum(case when pre_window_price_max is not null
                  and post_window_price_max is not null
                 then 1 else 0 end)                                               as eligible_listings
      , sum(case when pre_window_price_max  is not null
                  and post_window_price_max is not null
                  and pre_window_price_max  - pre_window_price_min  > 0.5
                 then 1 else 0 end)                                               as listings_repriced_pre
      , sum(case when pre_window_price_max  is not null
                  and post_window_price_max is not null
                  and post_window_price_max - post_window_price_min > 0.5
                 then 1 else 0 end)                                               as listings_repriced_post
      , round(100.0 * listings_repriced_pre  / nullif(eligible_listings, 0), 1)  as pct_repriced_pre
      , round(100.0 * listings_repriced_post / nullif(eligible_listings, 0), 1)  as pct_repriced_post
    from listing_windows
    group by 1
)

, dealer_level_results as (
    select
        fv.service_provider_id
      , fv.first_view_date
      , dm.current_dealer_name
      , dm.current_account_category_simplified
      , dm.dealer_size
      -- Below-benchmark flags for transparency
      , bb.avg_leads_per_unit                                                     as pre_avg_leads_per_unit
      , bb.avg_comp_leads_per_unit                                                as pre_avg_comp_leads_per_unit
      , bb.avg_vdps_per_unit                                                      as pre_avg_vdps_per_unit
      , bb.avg_comp_vdps_per_unit                                                 as pre_avg_comp_vdps_per_unit
      , bb.avg_days_on_lot                                                        as pre_avg_days_on_lot
      , bb.avg_comp_days_on_lot                                                   as pre_avg_comp_days_on_lot
      -- Pricing metrics
      , dp.eligible_listings
      , dp.listings_repriced_pre  > 0                                             as dealer_repriced_pre
      , dp.listings_repriced_post > 0                                             as dealer_repriced_post
      , dp.pct_repriced_pre
      , dp.pct_repriced_post
      , dp.pct_repriced_post - dp.pct_repriced_pre                                as pct_pt_lift_listings_repriced
      , dre.total_reprice_events_pre
      , dre.total_reprice_events_post
      , round((dre.total_reprice_events_post - dre.total_reprice_events_pre) / nullif(dre.total_reprice_events_pre, 0) * 100, 1) as pct_lift_reprice_events
      , dfr.days_to_first_reprice_post
    from first_ci_views fv
    inner join below_benchmark_dealers bb  on bb.service_provider_id  = fv.service_provider_id
    left join dealer_pp             dp     on dp.service_provider_id   = fv.service_provider_id
    left join dealer_metadata       dm     on dm.service_provider_id   = fv.service_provider_id
    left join dealer_reprice_events dre    on dre.service_provider_id  = fv.service_provider_id
    left join dealer_first_reprice  dfr    on dfr.service_provider_id  = fv.service_provider_id
)

, summary as (
    select
        current_account_category_simplified
      , dealer_size
      , count(distinct service_provider_id)                                         as total_dealers_viewed_ci
      , round(
            100.0 * count(case when dealer_repriced_pre  then service_provider_id end)
            / nullif(count(distinct service_provider_id), 0)
        , 1)                                                                        as pct_dealers_repriced_pre
      , round(
            100.0 * count(case when dealer_repriced_post then service_provider_id end)
            / nullif(count(distinct service_provider_id), 0)
        , 1)                                                                        as pct_dealers_repriced_post
      , pct_dealers_repriced_post - pct_dealers_repriced_pre                        as pct_pt_lift_dealers_repriced
      , round(avg(pct_repriced_pre),  1)                                            as avg_pct_listings_repriced_pre
      , round(avg(pct_repriced_post), 1)                                            as avg_pct_listings_repriced_post
      , round(avg(pct_pt_lift_listings_repriced),    1)                             as avg_pct_pt_lift_listings_repriced
      , round(median(pct_pt_lift_listings_repriced), 1)                             as median_pct_pt_lift_listings_repriced
      , sum(total_reprice_events_pre)                                               as total_reprice_events_pre
      , sum(total_reprice_events_post)                                              as total_reprice_events_post
      , round(
            (sum(total_reprice_events_post) - sum(total_reprice_events_pre))
            / nullif(sum(total_reprice_events_pre), 0) * 100
        , 1)                                                                        as pct_lift_reprice_events
      , median(days_to_first_reprice_post)                                          as median_days_to_first_reprice_post
    from dealer_level_results
    group by 1, 2
)

select * from summary
-- select * from dealer_level_results
;
