/*
  Pre/Post Competitive Gap & Performance Lift Analysis — CI (Performance + Competitors Tabs)
  Sources:
    - Events:       ANALYTICS.TRAFFIC.DEALER_DASHBOARD_EVENTS_NORMALIZED
    - Performance:  ANALYTICS.UNIFIED_DEALER_DATA_MART.PERFORMANCE_HEALTH_METRIC_COMPARISONS

  Design: within-dealer pre/post (14 days each side of first CI view date)
  Clean cohort: dealers with first_view_date <= current_date - 14 (necessary to grab complete post-window)

  Metrics (all scoped to used inventory):
    - leads_per_unit:  dealer leads / total used inventory
    - vdps_per_unit:   dealer VDP views / total used inventory
    - days_on_lot:     dealer average days on lot
    Competitive gap = dealer metric - competitor metric (positive = dealer ahead for leads/vdps;
    negative = dealer ahead for days on lot).

  Delta interpretation (post - pre) / |pre| * 100:
    Positive = improvement for leads/vdps. Negative = improvement for days on lot.
    Always cite median alongside avg — gap metrics are sensitive to outliers.
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

, performance_snapshots as (
    select
        m.service_provider_id
      , m.inventory_date
      , m.total_leads_used_inventory     / nullif(m.total_used_inventory, 0)              as leads_per_unit
      , m.total_vdp_views_used_inventory / nullif(m.total_used_inventory, 0)              as vdps_per_unit
      , m.avg_days_on_lot_used_inventory                                                   as days_on_lot
      , m.competitor_total_leads_used_inventory     / nullif(m.competitor_total_used_inventory, 0) as comp_leads_per_unit
      , m.competitor_total_vdp_views_used_inventory / nullif(m.competitor_total_used_inventory, 0) as comp_vdps_per_unit
      , m.competitor_avg_days_on_lot_used_inventory                                        as comp_days_on_lot
      , leads_per_unit - comp_leads_per_unit    as leads_per_unit_gap
      , vdps_per_unit  - comp_vdps_per_unit     as vdps_per_unit_gap
      , days_on_lot    - comp_days_on_lot       as days_on_lot_gap
      , case
            when m.inventory_date between dateadd('day', -14, fv.first_view_date)
                                      and dateadd('day',  -1, fv.first_view_date) then 'pre'
            when m.inventory_date between dateadd('day',   1, fv.first_view_date)
                                      and dateadd('day',  14, fv.first_view_date) then 'post'
        end as window_label
    from analytics.unified_dealer_data_mart.performance_health_metric_comparisons m
    inner join first_ci_performance_views fv on fv.service_provider_id = m.service_provider_id
    where m.inventory_date between dateadd('day', -14, fv.first_view_date)
                                and dateadd('day',  14, fv.first_view_date)
      and m.country_code         = 'US'
)

, dealer_performance_windows as (
    select
        service_provider_id
      , avg(case when window_label = 'pre'  then leads_per_unit      end) as avg_leads_per_unit_pre
      , avg(case when window_label = 'post' then leads_per_unit      end) as avg_leads_per_unit_post
      , avg(case when window_label = 'pre'  then vdps_per_unit       end) as avg_vdps_per_unit_pre
      , avg(case when window_label = 'post' then vdps_per_unit       end) as avg_vdps_per_unit_post
      , avg(case when window_label = 'pre'  then days_on_lot         end) as avg_days_on_lot_pre
      , avg(case when window_label = 'post' then days_on_lot         end) as avg_days_on_lot_post
      , avg(case when window_label = 'pre'  then leads_per_unit_gap  end) as avg_leads_per_unit_gap_pre
      , avg(case when window_label = 'post' then leads_per_unit_gap  end) as avg_leads_per_unit_gap_post
      , avg(case when window_label = 'pre'  then vdps_per_unit_gap   end) as avg_vdps_per_unit_gap_pre
      , avg(case when window_label = 'post' then vdps_per_unit_gap   end) as avg_vdps_per_unit_gap_post
      , avg(case when window_label = 'pre'  then days_on_lot_gap     end) as avg_days_on_lot_gap_pre
      , avg(case when window_label = 'post' then days_on_lot_gap     end) as avg_days_on_lot_gap_post
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
      -- Absolute performance
      , round(pw.avg_leads_per_unit_pre,   2) as avg_leads_per_unit_pre
      , round(pw.avg_leads_per_unit_post,  2) as avg_leads_per_unit_post
      , round((pw.avg_leads_per_unit_post  - pw.avg_leads_per_unit_pre)  / nullif(pw.avg_leads_per_unit_pre,  0) * 100, 1) as pct_lift_leads_per_unit
      , round(pw.avg_vdps_per_unit_pre,    2) as avg_vdps_per_unit_pre
      , round(pw.avg_vdps_per_unit_post,   2) as avg_vdps_per_unit_post
      , round((pw.avg_vdps_per_unit_post   - pw.avg_vdps_per_unit_pre)   / nullif(pw.avg_vdps_per_unit_pre,   0) * 100, 1) as pct_lift_vdps_per_unit
      , round(pw.avg_days_on_lot_pre,      1) as avg_days_on_lot_pre
      , round(pw.avg_days_on_lot_post,     1) as avg_days_on_lot_post
      , round((pw.avg_days_on_lot_post     - pw.avg_days_on_lot_pre)     / nullif(pw.avg_days_on_lot_pre,     0) * 100, 1) as pct_lift_days_on_lot
      -- Competitive gap
      , round(pw.avg_leads_per_unit_gap_pre,   2) as avg_leads_per_unit_gap_pre
      , round(pw.avg_leads_per_unit_gap_post,  2) as avg_leads_per_unit_gap_post
      , round((pw.avg_leads_per_unit_gap_post  - pw.avg_leads_per_unit_gap_pre)  / nullif(abs(pw.avg_leads_per_unit_gap_pre),  0) * 100, 1) as pct_lift_leads_per_unit_gap
      , round(pw.avg_vdps_per_unit_gap_pre,    2) as avg_vdps_per_unit_gap_pre
      , round(pw.avg_vdps_per_unit_gap_post,   2) as avg_vdps_per_unit_gap_post
      , round((pw.avg_vdps_per_unit_gap_post   - pw.avg_vdps_per_unit_gap_pre)   / nullif(abs(pw.avg_vdps_per_unit_gap_pre),   0) * 100, 1) as pct_lift_vdps_per_unit_gap
      , round(pw.avg_days_on_lot_gap_pre,      1) as avg_days_on_lot_gap_pre
      , round(pw.avg_days_on_lot_gap_post,     1) as avg_days_on_lot_gap_post
      , round((pw.avg_days_on_lot_gap_post     - pw.avg_days_on_lot_gap_pre)     / nullif(abs(pw.avg_days_on_lot_gap_pre),     0) * 100, 1) as pct_lift_days_on_lot_gap
    from first_ci_performance_views fv
    left join dealer_performance_windows pw on pw.service_provider_id = fv.service_provider_id
    left join dealer_metadata            dm on dm.service_provider_id  = fv.service_provider_id
)

, summary as (
    select
        current_account_category_simplified
      , dealer_size
      , count(distinct service_provider_id)                        as total_dealers
      , round(avg(pct_lift_leads_per_unit),           1)            as avg_pct_lift_leads_per_unit
      , round(median(pct_lift_leads_per_unit),        1)            as median_pct_lift_leads_per_unit
      , round(avg(pct_lift_leads_per_unit_gap),       1)            as avg_pct_lift_leads_per_unit_gap
      , round(median(pct_lift_leads_per_unit_gap),    1)            as median_pct_lift_leads_per_unit_gap
      , round(avg(pct_lift_vdps_per_unit),            1)            as avg_pct_lift_vdps_per_unit
      , round(median(pct_lift_vdps_per_unit),         1)            as median_pct_lift_vdps_per_unit
      , round(avg(pct_lift_vdps_per_unit_gap),        1)            as avg_pct_lift_vdps_per_unit_gap
      , round(median(pct_lift_vdps_per_unit_gap),     1)            as median_pct_lift_vdps_per_unit_gap
      , round(avg(pct_lift_days_on_lot),              1)            as avg_pct_lift_days_on_lot
      , round(median(pct_lift_days_on_lot),           1)            as median_pct_lift_days_on_lot
      , round(avg(pct_lift_days_on_lot_gap),          1)            as avg_pct_lift_days_on_lot_gap
      , round(median(pct_lift_days_on_lot_gap),       1)            as median_pct_lift_days_on_lot_gap
    from dealer_level_results
    group by 1, 2
)

select * from summary
-- select * from dealer_level_results
;
