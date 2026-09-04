/*
  Durability Analysis — Pricing & VDP Behavior Change After CI View
  Sources:
    - Events:   ANALYTICS.TRAFFIC.DEALER_DASHBOARD_EVENTS_NORMALIZED
    - Pricing:  ANALYTICS.INVENTORY.INVENTORY_LISTINGS_IMV
    - VDPs:     ANALYTICS.UNIFIED_DEALER_DATA_MART.PERFORMANCE_HEALTH_METRIC_COMPARISONS

  Design: same within-dealer pre/post window as price_change_after_ci_view.sql.
  Window size is controlled by the `params` CTE — change `window_days` to 14, 30, or 60.
  The cohort `having` clause and all date arithmetic automatically adapt to window_days.

  Metrics tracked:
    - pct_lift_reprice_events:         % change in total reprice events post vs pre
    - pct_pt_lift_dealers_repriced:    ppt change in % of dealers who repriced at least one listing
    - avg/median_pct_lift_vdps_per_unit: % change in VDPs/used unit post vs pre
    - median_days_to_first_reprice_post: median days from first_view_date to first post-window reprice

  Summary dimensions: current_account_category_simplified, dealer_size, first_viewed_tab, window_days

  Note on 60-day cohort: product launched May 21, 2026. A 60-day post-window means only dealers
  who first viewed CI in the first ~3 weeks after launch qualify. Cohort will be thin — check
  total_dealers before drawing any conclusions.
*/

with
params as (
    select 30 as window_days  -- change to 14, 30, or 60
)

, dealer_metadata as (
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
    having first_view_date <= dateadd('day', -(select window_days from params), current_date())
)

, dealer_first_tab as (
    select
        service_provider_id
      , sd_product as first_viewed_tab
    from analytics.traffic.dealer_dashboard_events_normalized
    where sd_application     = 'Dealer_Dashboard'
      and sd_product         in ('Performance', 'Competitors')
      and sd_product_section in ('Performance', 'Competitors')
      and source             = 'cargurus_dealer_pageview_tracking'
      and is_staff           = false
      and is_bot             = false
      and region             = 'NA'
    qualify row_number() over (partition by service_provider_id order by derived_tstamp) = 1
)

, dealer_engagement_raw as (
    select
        e.service_provider_id
      , count(distinct e.derived_tstamp::date)
        / nullif(datediff('day', fv.first_view_date, current_date()), 0) as session_rate
    from analytics.traffic.dealer_dashboard_events_normalized e
    inner join first_ci_views fv on fv.service_provider_id = e.service_provider_id
    where e.sd_application     = 'Dealer_Dashboard'
      and e.sd_product         in ('Performance', 'Competitors')
      and e.sd_product_section in ('Performance', 'Competitors')
      and e.source             = 'cargurus_dealer_pageview_tracking'
      and e.is_staff           = false
      and e.is_bot             = false
      and e.region             = 'NA'
      and e.derived_tstamp::date >= fv.first_view_date
    group by e.service_provider_id, fv.first_view_date
)

, dealer_engagement as (
    -- session_rate = distinct CI view days / total days since first_view_date.
    -- Denominator is current_date() so the rate is a window-agnostic lifetime engagement metric.
    -- Q1 = least frequent users, Q4 = most frequent.
    select
        service_provider_id
      , session_rate
      , concat('Q', ntile(4) over (order by session_rate)) as engagement_quartile
    from dealer_engagement_raw
)

, listing_windows as (
    select
        il.inventory_listing_id
      , il.service_provider_id
      , max(case when il.process_start_time::date
                      between dateadd('day', -(select window_days from params), fv.first_view_date)
                          and dateadd('day',  -1, fv.first_view_date)
                 then il.price_shown_on_site end) as pre_window_price_max
      , min(case when il.process_start_time::date
                      between dateadd('day', -(select window_days from params), fv.first_view_date)
                          and dateadd('day',  -1, fv.first_view_date)
                 then il.price_shown_on_site end) as pre_window_price_min
      , max(case when il.process_start_time::date
                      between dateadd('day',   1, fv.first_view_date)
                          and dateadd('day',  (select window_days from params), fv.first_view_date)
                 then il.price_shown_on_site end) as post_window_price_max
      , min(case when il.process_start_time::date
                      between dateadd('day',   1, fv.first_view_date)
                          and dateadd('day',  (select window_days from params), fv.first_view_date)
                 then il.price_shown_on_site end) as post_window_price_min
    from analytics.inventory.inventory_listings_imv il
    inner join first_ci_views fv on fv.service_provider_id = il.service_provider_id
    where il.process_start_time::date
              between dateadd('day', -(select window_days from params), fv.first_view_date)
                  and dateadd('day',  (select window_days from params), fv.first_view_date)
      and il.region              = 'NA'
      and il.listing_status      = 'OPEN'
      and il.is_new              = false
      and il.price_shown_on_site > 0
    group by 1, 2
)

, price_snapshots_labeled as (
    -- Partitioning LAG by window_label prevents cross-boundary transitions.
    select
        il.service_provider_id
      , il.inventory_listing_id
      , il.process_start_time::date as snapshot_date
      , il.price_shown_on_site
      , case
            when il.process_start_time::date
                     between dateadd('day', -(select window_days from params), fv.first_view_date)
                         and dateadd('day',  -1, fv.first_view_date) then 'pre'
            when il.process_start_time::date
                     between dateadd('day',   1, fv.first_view_date)
                         and dateadd('day',  (select window_days from params), fv.first_view_date) then 'post'
        end as window_label
    from analytics.inventory.inventory_listings_imv il
    inner join first_ci_views fv on fv.service_provider_id = il.service_provider_id
    where il.process_start_time::date
              between dateadd('day', -(select window_days from params), fv.first_view_date)
                  and dateadd('day',  (select window_days from params), fv.first_view_date)
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
    -- Earliest post-window reprice day per dealer; NULL if none. Day +1 = 1.
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

, performance_snapshots as (
    select
        m.service_provider_id
      , m.total_vdp_views_used_inventory / nullif(m.total_used_inventory, 0)              as vdps_per_unit
      , m.competitor_total_vdp_views_used_inventory / nullif(m.competitor_total_used_inventory, 0) as comp_vdps_per_unit
      , vdps_per_unit - comp_vdps_per_unit                                                as vdps_per_unit_gap
      , case
            when m.inventory_date between dateadd('day', -(select window_days from params), fv.first_view_date)
                                      and dateadd('day',  -1, fv.first_view_date) then 'pre'
            when m.inventory_date between dateadd('day',   1, fv.first_view_date)
                                      and dateadd('day',  (select window_days from params), fv.first_view_date) then 'post'
        end as window_label
    from analytics.unified_dealer_data_mart.performance_health_metric_comparisons m
    inner join first_ci_views fv on fv.service_provider_id = m.service_provider_id
    where m.inventory_date between dateadd('day', -(select window_days from params), fv.first_view_date)
                                and dateadd('day',  (select window_days from params), fv.first_view_date)
      and m.country_code         = 'US'
)

, dealer_vdp_windows as (
    select
        service_provider_id
      , avg(case when window_label = 'pre'  then vdps_per_unit     end) as avg_vdps_per_unit_pre
      , avg(case when window_label = 'post' then vdps_per_unit     end) as avg_vdps_per_unit_post
      , avg(case when window_label = 'pre'  then vdps_per_unit_gap end) as avg_vdps_per_unit_gap_pre
      , avg(case when window_label = 'post' then vdps_per_unit_gap end) as avg_vdps_per_unit_gap_post
    from performance_snapshots
    where window_label is not null
    group by 1
)

, dealer_level_results as (
    select
        fv.service_provider_id
      , fv.first_view_date
      , dm.current_dealer_name
      , dm.current_account_category_simplified
      , dm.dealer_size
      , ft.first_viewed_tab
      , de.session_rate
      , de.engagement_quartile
      , dp.eligible_listings
      , dp.listings_repriced_pre  > 0                                             as dealer_repriced_pre
      , dp.listings_repriced_post > 0                                             as dealer_repriced_post
      , dp.pct_repriced_pre
      , dp.pct_repriced_post
      , dp.pct_repriced_post - dp.pct_repriced_pre                                as pct_pt_lift_listings_repriced
      , dre.total_reprice_events_pre
      , dre.total_reprice_events_post
      , dfr.days_to_first_reprice_post
      , round(
            (dv.avg_vdps_per_unit_post - dv.avg_vdps_per_unit_pre)
            / nullif(dv.avg_vdps_per_unit_pre, 0) * 100
        , 1)                                                                      as pct_lift_vdps_per_unit
      , round(
            (dv.avg_vdps_per_unit_gap_post - dv.avg_vdps_per_unit_gap_pre)
            / nullif(abs(dv.avg_vdps_per_unit_gap_pre), 0) * 100
        , 1)                                                                      as pct_lift_vdps_per_unit_gap
    from first_ci_views fv
    left join dealer_pp             dp  on dp.service_provider_id  = fv.service_provider_id
    left join dealer_metadata       dm  on dm.service_provider_id  = fv.service_provider_id
    left join dealer_reprice_events dre on dre.service_provider_id = fv.service_provider_id
    left join dealer_first_reprice  dfr on dfr.service_provider_id = fv.service_provider_id
    left join dealer_vdp_windows    dv  on dv.service_provider_id  = fv.service_provider_id
    left join dealer_first_tab      ft  on ft.service_provider_id  = fv.service_provider_id
    left join dealer_engagement     de  on de.service_provider_id  = fv.service_provider_id
)

, summary as (
    select
        first_viewed_tab
      , engagement_quartile
      , (select window_days from params)                                            as window_days
      , count(distinct service_provider_id)                                         as total_dealers
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
      , round(avg(pct_lift_vdps_per_unit),           1)                              as avg_pct_lift_vdps_per_unit
      , round(median(pct_lift_vdps_per_unit),        1)                              as median_pct_lift_vdps_per_unit
      , round(avg(pct_lift_vdps_per_unit_gap),       1)                              as avg_pct_lift_vdps_per_unit_gap
      , round(median(pct_lift_vdps_per_unit_gap),    1)                              as median_pct_lift_vdps_per_unit_gap
      , median(days_to_first_reprice_post)                                           as median_days_to_first_reprice_post
    from dealer_level_results
    group by 1, 2, 3
)

select * from summary order by first_viewed_tab, engagement_quartile
-- select * from dealer_level_results
;
