/*
  Pre/Post Merchandising Health Analysis — Competitive Intelligence (Performance Tab)
  Sources:
    - Events:       ANALYTICS.TRAFFIC.DEALER_DASHBOARD_EVENTS_NORMALIZED
    - Merchandising: ANALYTICS.UNIFIED_DEALER_DATA_MART.AUTO_ENTITY_PERFORMANCE_METRICS

  Design: within-dealer pre/post (14 days each side of first CI Performance view date)
  Clean cohort: dealers with first_view_date <= current_date - 14 (necessary to grab complete post-window)
  Scope: Performance tab only — merchandising health (photos, price, options) is not exposed in
    the Competitors tab, so the cohort is restricted to sd_product = 'Performance'.

  Metrics:
    - pct_without_photos:   % of used inventory lacking photos
    - pct_without_price:    % of used inventory without a price
    - pct_without_options:  % of used inventory without options
  Each is averaged across daily snapshots in the window. Negative delta (post - pre) = improvement.
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

, first_ci_performance_views as (
    -- One row per dealer: earliest Performance tab view, excluding staff/bots.
    -- Restricted to dealers with a complete 14-day post-window.
    select
        service_provider_id
        , min(derived_tstamp::date) as first_view_date
    from analytics.traffic.dealer_dashboard_events_normalized
    where
        sd_application          = 'Dealer_Dashboard'
        and sd_product          = 'Performance'
        and sd_product_section  = 'Performance'
        -- Restricting the dealer cohort to those that clicked the
        -- MerchandisingRecommendation QuickAction. It is reasonable for the proxy
        -- "they saw the gap and engaged with it". Data is likely thin, but useful
        and sd_feature          = 'QuickActions'
        and sd_element          = 'MerchandisingRecommendation'
        and is_staff            = false
        and is_bot              = false
        and region              = 'NA'
    group by service_provider_id
    having first_view_date <= dateadd('day', -14, current_date())
)

, merchandising_snapshots as (
    -- Daily merchandising metrics per dealer, tagged to their window (pre/post).
    select
        m.service_provider_id
        , m.inventory_date
        , m.total_used_inventory
        , round((m.total_used_inventory - m.total_used_inventory_with_photos) / nullif(m.total_used_inventory, 0) * 100, 1) as pct_without_photos
        , round((m.total_used_inventory - m.total_priced_used_inventory)      / nullif(m.total_used_inventory, 0) * 100, 1) as pct_without_price
        , round((m.total_used_inventory - m.total_options_used_inventory)     / nullif(m.total_used_inventory, 0) * 100, 1) as pct_without_options
        , case
            when m.inventory_date between dateadd('day', -14, fv.first_view_date)
                                      and dateadd('day',  -1, fv.first_view_date) then 'pre'
            when m.inventory_date between dateadd('day',   1, fv.first_view_date)
                                      and dateadd('day',  14, fv.first_view_date) then 'post'
        end as window_label
    from analytics.unified_dealer_data_mart.auto_entity_performance_metrics m
    inner join first_ci_performance_views fv on fv.service_provider_id = m.service_provider_id
    where m.inventory_date between dateadd('day', -14, fv.first_view_date)
                                and dateadd('day',  14, fv.first_view_date)
      and m.country_code          = 'US'
      and m.total_used_inventory  > 0
)

, dealer_merchandising_windows as (
    -- Per-dealer average of each metric within each window.
    select
        service_provider_id
        , avg(case when window_label = 'pre'  then pct_without_photos  end) as avg_pct_without_photos_pre
        , avg(case when window_label = 'post' then pct_without_photos  end) as avg_pct_without_photos_post
        , avg(case when window_label = 'pre'  then pct_without_price   end) as avg_pct_without_price_pre
        , avg(case when window_label = 'post' then pct_without_price   end) as avg_pct_without_price_post
        , avg(case when window_label = 'pre'  then pct_without_options end) as avg_pct_without_options_pre
        , avg(case when window_label = 'post' then pct_without_options end) as avg_pct_without_options_post
    from merchandising_snapshots
    where window_label is not null
    group by service_provider_id
)

, dealer_level_results as (
    select
        fv.service_provider_id
        , fv.first_view_date
        , dm.current_dealer_name
        , dm.current_account_category_simplified
        , dm.dealer_size
        , round(mw.avg_pct_without_photos_pre,   1) as avg_pct_without_photos_pre
        , round(mw.avg_pct_without_photos_post,  1) as avg_pct_without_photos_post
        , round(mw.avg_pct_without_photos_post  - mw.avg_pct_without_photos_pre,  1) as pct_pt_delta_without_photos
        , round(mw.avg_pct_without_price_pre,    1) as avg_pct_without_price_pre
        , round(mw.avg_pct_without_price_post,   1) as avg_pct_without_price_post
        , round(mw.avg_pct_without_price_post   - mw.avg_pct_without_price_pre,   1) as pct_pt_delta_without_price
        , round(mw.avg_pct_without_options_pre,  1) as avg_pct_without_options_pre
        , round(mw.avg_pct_without_options_post, 1) as avg_pct_without_options_post
        , round(mw.avg_pct_without_options_post - mw.avg_pct_without_options_pre, 1) as pct_pt_delta_without_options
    from first_ci_performance_views fv
    left join dealer_merchandising_windows mw on mw.service_provider_id = fv.service_provider_id
    left join dealer_metadata              dm on dm.service_provider_id  = fv.service_provider_id
)

, summary as (
    select
        current_account_category_simplified
        , dealer_size
        , count(distinct service_provider_id)                            as total_dealers_viewed_performance
        -- Photos
        , round(avg(avg_pct_without_photos_pre),          1)             as avg_pct_without_photos_pre
        , round(avg(avg_pct_without_photos_post),         1)             as avg_pct_without_photos_post
        , round(avg(pct_pt_delta_without_photos),         1)             as avg_pct_pt_delta_without_photos
        , round(median(pct_pt_delta_without_photos),      1)             as median_pct_pt_delta_without_photos
        -- Price
        , round(avg(avg_pct_without_price_pre),           1)             as avg_pct_without_price_pre
        , round(avg(avg_pct_without_price_post),          1)             as avg_pct_without_price_post
        , round(avg(pct_pt_delta_without_price),          1)             as avg_pct_pt_delta_without_price
        , round(median(pct_pt_delta_without_price),       1)             as median_pct_pt_delta_without_price
        -- Options
        , round(avg(avg_pct_without_options_pre),         1)             as avg_pct_without_options_pre
        , round(avg(avg_pct_without_options_post),        1)             as avg_pct_without_options_post
        , round(avg(pct_pt_delta_without_options),        1)             as avg_pct_pt_delta_without_options
        , round(median(pct_pt_delta_without_options),     1)             as median_pct_pt_delta_without_options
    from dealer_level_results
    group by 1, 2
)

select
    *
from summary
-- select * from dealer_level_results
;
