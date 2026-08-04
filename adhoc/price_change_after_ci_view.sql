/*
  Pre/Post Pricing Analysis — Competitive Intelligence (CI Tab)
  Sources:
    - Views:    ANALYTICS.TRAFFIC.DEALER_DASHBOARD_EVENTS_NORMALIZED
    - Pricing:  ANALYTICS.INVENTORY.INVENTORY_LISTINGS_IMV
    - Tiers:    ANALYTICS.COMPETITIVE_INTELLIGENCE.PERFORMANCE_HEALTH_METRIC_COMPARISON_WEEKLY
  
  Design: within-dealer pre/post (14 days each side of first CI view date)
  Clean cohort: dealers with first_view_date <= current_date - 14 (necessary to grab complete post-window)
  Listing change threshold: |max_price - min_price| > $0.50 within each window
*/

with first_ci_views as (
    -- One row per dealer: their earliest CI page view, excluding staff/bots.
    -- Restrict to dealers with a complete 14-day post-window.
    select
        service_provider_id
        , min(derived_tstamp::date) as first_view_date
    from analytics.traffic.dealer_dashboard_events_normalized
    where sd_application        = 'Dealer_Dashboard'
      and sd_product            in ('Performance', 'Competitors')
      and sd_product_section    in ('Performance', 'Competitors')
      and source    = 'cargurus_dealer_pageview_tracking'
      and is_staff  = false
      and is_bot    = false
      and region    = 'NA'
    group by service_provider_id
    having first_view_date <= dateadd('day', -14, current_date())
)

, listing_windows as (
    -- For each dealer, compute pre/post price range per listing.
    -- Pre:  14 days before first view (exclusive of view date)
    -- Post: 14 days after first view (exclusive of view date)
    select
        il.inventory_listing_id
      , il.service_provider_id
      , max(case when il.process_start_time::date
                      between dateadd('day', -14, fv.first_view_date)
                          and dateadd('day', -1,  fv.first_view_date)
                 then il.price_shown_on_site end) as pre_max -- Maximum price the listing had BEFORE the CI view date
      , min(case when il.process_start_time::date
                      between dateadd('day', -14, fv.first_view_date)
                          and dateadd('day', -1,  fv.first_view_date)
                 then il.price_shown_on_site end) as pre_min -- Minimum price the listing had BEFORE the CI view date
      , max(case when il.process_start_time::date
                      between dateadd('day',  1,  fv.first_view_date)
                          and dateadd('day', 14,  fv.first_view_date)
                 then il.price_shown_on_site end) as post_max -- Maximum price the listing had AFTER the CI view date
      , min(case when il.process_start_time::date
                      between dateadd('day',  1,  fv.first_view_date)
                          and dateadd('day', 14,  fv.first_view_date)
                 then il.price_shown_on_site end) as post_min -- Minimum price the listing had AFTER the CI view date
    from analytics.inventory.inventory_listings_imv il
    inner join first_ci_views fv
        on fv.service_provider_id = il.service_provider_id
    where
        il.process_start_time::date between dateadd('day', -14, fv.first_view_date) and dateadd('day',  14, fv.first_view_date)
        and il.region         = 'NA'
        and il.listing_status = 'OPEN'
        and il.is_new         = false
        and il.price_shown_on_site > 0
    group by 1, 2
)

, dealer_pp as (
    select
        service_provider_id
        -- Null values mean that the car was sold before the view date
        , sum(case when pre_max is not null and post_max is not null then 1 else 0 end) as listings
        /*
            For each window (pre or post CI view) we're checking whether the listing had a price change
            The threshold is set to $0.50 to consider an actual price change
            [pre/post]_max - [pre/post]_min > 0.5 -----> True, price changed. False otherwise
        */
        , sum(case when pre_max  is not null and post_max is not null and pre_max  - pre_min  > 0.5 then 1 else 0 end) as changed_pre
        , sum(case when pre_max  is not null and post_max is not null and post_max - post_min > 0.5 then 1 else 0 end) as changed_post
        , round(100 * changed_pre/nullif(listings, 0), 1) as pct_pre 
        , round(100 * changed_post/nullif(listings, 0), 1) as pct_post
    from listing_windows
    group by 1
)

, dealer_metadata as (
    select
        service_provider_id
      , current_dealer_name
      , current_account_category_simplified
      , split_part(current_account_category, '-', 2) as dealer_size
    from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
    where
        region = 'NA'
    -- Grab the most recent month for each dealer
    qualify
        row_number() over (partition by region, service_provider_id order by inventory_date desc) = 1
)
-- ─── Dealer-level results ─────────────────────────────────────────────────────
, dealer_level_results as (
    select
        fv.service_provider_id
        , fv.first_view_date
        , dm.current_dealer_name
        , dm.current_account_category_simplified
        , dm.dealer_size
        , dp.listings
        , dp.pct_pre   as pct_listings_repriced_pre
        , dp.pct_post  as pct_listings_repriced_post
        , dp.pct_post - dp.pct_pre as lift_pp
    from first_ci_views fv
    left join dealer_pp dp
        on dp.service_provider_id = fv.service_provider_id
    left join dealer_metadata dm
        on dm.service_provider_id = fv.service_provider_id
    --order by
    --lift_pp desc nulls last
)

select
    dlr.current_account_category_simplified
    , dlr.dealer_size
    , count(distinct dlr.service_provider_id)   as total_dealers_viewed_ci
    , round(avg(pct_listings_repriced_pre))     as avg_pct_pre
    , round(avg(pct_listings_repriced_post))    as avg_pct_post
    , round(avg(lift_pp))                       as avg_lift_pp
    , round(median(lift_pp))                    as median_lift_pp
from dealer_level_results dlr
group by 1, 2;


