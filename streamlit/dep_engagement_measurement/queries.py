import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

from config import DEALER_LIST


def _sql_in_list(values: list[str]) -> str:
    """Return a SQL IN-list literal, e.g. ('foo', 'bar').

    Each value is wrapped in single quotes and any embedded single quote
    (chr(39) == "'") is doubled — the SQL standard escape for a literal
    apostrophe inside a single-quoted string.
    """
    escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in values)
    return f"({escaped})"


# ─── Interaction signal definitions ──────────────────────────────────────────
# Update these tuples to add or remove tracked features / elements.

PERF_INTERACTION_FEATURES = ('QuickActions', 'PerformanceNavigation')
PERF_INTERACTION_ELEMENTS = (
    'PricingRecommendation', 'SourcingRecommendation',
    'MerchandisingRecommendation', 'DealRatingLearnMore', 'LowVDPsLearnMore',
)

COMP_INTERACTION_FEATURES = ('CompetitorsFilters', 'CompetitorsList', 'InsightsBanner', 'ComparisonOverlay')
COMP_INTERACTION_ELEMENTS = (
    'TableSort', 'RadiusSelector', 'SearchOverlapSelector',
    'FranchiseTypeSelector', 'ViewStats', 'ShowTopCompetitorsButton', 'AttributeToggle',
)

INTERACTION_EVENT_TYPES = ('Click', 'Change')

# Pre-built SQL IN-list strings — used directly in f-string SQL queries.
_PERF_F_SQL = _sql_in_list(list(PERF_INTERACTION_FEATURES))
_PERF_E_SQL = _sql_in_list(list(PERF_INTERACTION_ELEMENTS))
_COMP_F_SQL = _sql_in_list(list(COMP_INTERACTION_FEATURES))
_COMP_E_SQL = _sql_in_list(list(COMP_INTERACTION_ELEMENTS))
_EVENTS_SQL = _sql_in_list(list(INTERACTION_EVENT_TYPES))


def common_ctes(ew: str, days: int, account_categories: list[str] | None = None, accept_staff: bool = False, ddi_only: bool = True, dealer_sizes: list[str] | None = None) -> str:
    """Build the shared WITH-clause CTEs used by every dashboard query.

    Parameters
    ----------
    ew : str
        SQL boolean expression for the engagement filter, either
        ``"used_dep_last_180_days"`` or ``"true"``.
    days : int
        Look-back window for base_events (e.g. 60 for last 60 days).
    account_categories : list[str] | None
        If provided, restrict totals/dealer_totals to these account categories.
    accept_staff : bool
        When False, excludes staff events from base_events.
    ddi_only : bool
        When True, joins against DDI subscriptions so only DDI-enrolled dealers appear.
    dealer_sizes : list[str] | None
        If provided, restrict totals/dealer_totals to these dealer sizes.
    """
    cat_filter = (
        f"and current_account_category_simplified in {_sql_in_list(account_categories)}"
        if account_categories
        else ""
    )
    size_filter = (
        f"and dealer_size in {_sql_in_list(dealer_sizes)}"
        if dealer_sizes
        else ""
    )
    ddi_cte = """
    , ddi_dealers as (
        select distinct ddr._region_, ddr.service_provider_id
        from warehouse.site.drs_dealer_reports ddr
        inner join warehouse.site.drs_reports_to_subscribers drs
            on ddr.id = drs.report_id
            and ddr._region_ = drs._region_
        where ddr.active = 1
          and drs.is_subscribed = 1
          and ddr._region_ = 'NA'
    )""" if ddi_only else ""
    ddi_join = "inner join ddi_dealers dd on dd.service_provider_id = sp.location_id and dd._region_ = sp._region_" if ddi_only else ""
    return f"""
    user_engagement as (
        select
            service_provider_id
          , region
          , user_id
          , min(derived_tstamp::date) as min_date
        from analytics.traffic.dealer_dashboard_events_normalized
        where derived_tstamp >= current_date() - 180
          and sd_application = 'Dealer_Dashboard'
          and is_staff = false
        group by all
    ){ddi_cte}
    , dealer_metadata as (
        select
            region
          , service_provider_id
          , current_dealer_name
          , current_account_category_simplified
          , split_part(current_account_category, '-', 2) as dealer_size
        from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
        where inventory_date = (
            select max(inventory_date)
            from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
        )
    )
    , dealers_and_users as (
        select
            sp._region_
          , sp.location_id as service_provider_id
          , s.user_uuid
          , dm.current_dealer_name
          , dm.current_account_category_simplified
          , dm.dealer_size
        from warehouse.site.service_providers sp
        left join warehouse.site.person_roles pr
            on cast(substring(pr.entity_id, 3) as int) = sp.location_id
        left join warehouse.site.people p on p.id = pr.person_id
        left join warehouse.site.subscribers s on s.person_id = pr.person_id
        left join dealer_metadata dm
            on  dm.service_provider_id = sp.location_id
            and dm.region              = sp._region_
        {ddi_join}
        where pr.enabled = 1
          and pr.role_name = 'ROLE_DD_ADMIN'
          and sp._region_ = 'NA'
          and sp.location_id in ({DEALER_LIST})
        qualify row_number() over (partition by sp.location_id, s.user_uuid order by pr.id) = 1
    )
    , user_engagement_flags as (
        select
            d.service_provider_id
          , d._region_
          , d.user_uuid
          , d.current_dealer_name
          , d.current_account_category_simplified
          , d.dealer_size
          , ue.min_date is not null as used_dep_last_180_days
        from dealers_and_users d
        left join user_engagement ue
            on  ue.service_provider_id = d.service_provider_id
            and ue.region              = d._region_
            and ue.user_id             = d.user_uuid
    )
    , totals as (
        select
            _region_
          , count(distinct service_provider_id) as total_dealers
          , count(distinct user_uuid)           as total_users
        from user_engagement_flags
        where {ew}
          {cat_filter}
          {size_filter}
        group by all
    )
    , dealer_totals as (
        select
            _region_
          , service_provider_id
          , any_value(current_dealer_name)     as current_dealer_name
          , any_value(current_account_category_simplified) as current_account_category_simplified
          , any_value(dealer_size)             as dealer_size
          , count(distinct user_uuid)           as total_users_at_dealer
        from user_engagement_flags
        where {ew}
          {cat_filter}
          {size_filter}
        group by all
    )
    , base_events as (
        select tbl.*
        from analytics.traffic.dealer_dashboard_events_normalized tbl
        where tbl.derived_tstamp >= current_date() - {days}
          and tbl.region = 'NA'
          and tbl.sd_application = 'Dealer_Dashboard'
          and tbl.sd_product in ('Competitors', 'Performance')
          {"" if accept_staff else "and tbl.is_staff = false"}
    )
    """


@st.cache_data(ttl=3600, show_spinner=False)
def load_account_categories(_session) -> list[str]:
    sql = """
    select distinct current_account_category_simplified
    from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
    where country_code = 'US'
      and current_account_category_simplified is not null
      and inventory_date = (
          select max(inventory_date)
          from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
          where country_code = 'US'
      )
    order by 1
    """
    return _session.sql(sql).to_pandas()["CURRENT_ACCOUNT_CATEGORY_SIMPLIFIED"].tolist()


@st.cache_data(ttl=3600, show_spinner=False)
def load_dealer_sizes(_session) -> list[str]:
    sql = """
    select distinct split_part(current_account_category, '-', 2) as dealer_size
    from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
    where current_account_category is not null
      and split_part(current_account_category, '-', 2) != ''
    order by 1
    """
    return _session.sql(sql).to_pandas()["DEALER_SIZE"].tolist()


@st.cache_data(ttl=3600, show_spinner=False)
def load_overview(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True, ddi_only: bool = False, dealer_sizes: tuple[str, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql_weekly = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , page_views as (
        select
            be.region
          , be.derived_tstamp::date                       as date
          , be.sd_product_section                         as product
          , count(distinct be.service_provider_id)        as dealers_viewing
          , count(distinct be.user_id)                    as users_viewing
        from base_events be
        where
          be.sd_product_section in ('Competitors', 'Performance')
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    select
        pv.date
      , pv.product
      , t.total_dealers
      , t.total_users
      , pv.dealers_viewing
      , pv.users_viewing
      , round(pv.dealers_viewing / nullif(t.total_dealers, 0) * 100, 1) as dealers_viewing_pct
      , round(pv.users_viewing   / nullif(t.total_users,   0) * 100, 1) as users_viewing_pct
    from totals t
    inner join page_views pv on t._region_ = pv.region
    order by pv.date desc
    """

    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , page_views as (
        select
            be.region
          , be.sd_product_section as product
          , count(distinct be.service_provider_id) as dealers_viewing
          , count(distinct be.user_id)             as users_viewing
        from base_events be
        where be.sd_product_section in ('Competitors', 'Performance')
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    select
        pv.product
      , t.total_dealers
      , t.total_users
      , pv.dealers_viewing
      , pv.users_viewing
      , round(pv.dealers_viewing / nullif(t.total_dealers, 0) * 100, 1) as dealers_viewing_pct
      , round(pv.users_viewing   / nullif(t.total_users,   0) * 100, 1) as users_viewing_pct
    from totals t
    inner join page_views pv on t._region_ = pv.region
    """

    return _session.sql(sql_weekly).to_pandas(), _session.sql(sql_period).to_pandas()


@st.cache_data(ttl=3600, show_spinner=False)
def load_performance(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True, ddi_only: bool = False, dealer_sizes: tuple[str, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , summary as (
        select
            count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in {_PERF_F_SQL}
          and be.sd_element in {_PERF_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
    )
    select
        t.total_dealers
      , t.total_users
      , s.dealers_interacting
      , s.users_interacting
      , round(s.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealers_interacting_pct
      , round(s.users_interacting   / nullif(t.total_users,   0) * 100, 1) as users_interacting_pct
    from totals t
    cross join summary s
    """

    sql_dealer = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , page_views as (
        select
            be.service_provider_id
          , be.derived_tstamp::date              as date
          , count(distinct be.user_id) as users_viewing
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    , interactions as (
        select
            be.service_provider_id
          , be.derived_tstamp::date              as date
          , count(distinct be.user_id) as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in {_PERF_F_SQL}
          and be.sd_element in {_PERF_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
        group by all
    )
    , dates as (
        select distinct date from page_views
        union
        select distinct date from interactions
    )
    select
        d.date
      , dt.service_provider_id                                                                 as dealer_id
      , dt.current_dealer_name                                                                 as dealer_name
      , dt.current_account_category_simplified                                                 as account_category
      , dt.total_users_at_dealer                                                               as eligible_users
      , coalesce(pv.users_viewing,    0)                                                       as users_viewing
      , coalesce(i.users_interacting, 0)                                                       as users_interacting
      , round(coalesce(pv.users_viewing,    0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as viewing_pct
      , round(coalesce(i.users_interacting, 0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as interaction_pct
    from dealer_totals dt
    cross join dates d
    left join page_views pv  on pv.service_provider_id = dt.service_provider_id and pv.date = d.date
    left join interactions i on i.service_provider_id  = dt.service_provider_id and i.date  = d.date
    order by d.date desc, interaction_pct desc nulls last
    """

    return (
        _session.sql(sql_dealer).to_pandas(),
        _session.sql(sql_period).to_pandas(),
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_competitors(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True, ddi_only: bool = False, dealer_sizes: tuple[str, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , summary as (
        select
            count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in {_COMP_F_SQL}
          and be.sd_element in {_COMP_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
    )
    select
        t.total_dealers
      , t.total_users
      , s.dealers_interacting
      , s.users_interacting
      , round(s.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealers_interacting_pct
      , round(s.users_interacting   / nullif(t.total_users,   0) * 100, 1) as users_interacting_pct
    from totals t
    cross join summary s
    """

    sql_dealer = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , page_views as (
        select
            be.service_provider_id
          , be.derived_tstamp::date              as date
          , count(distinct be.user_id) as users_viewing
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    , interactions as (
        select
            be.service_provider_id
          , be.derived_tstamp::date              as date
          , count(distinct be.user_id) as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in {_COMP_F_SQL}
          and be.sd_element in {_COMP_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
        group by all
    )
    , dates as (
        select distinct date from page_views
        union
        select distinct date from interactions
    )
    select
        d.date
      , dt.service_provider_id                                                                 as dealer_id
      , dt.current_dealer_name                                                                 as dealer_name
      , dt.current_account_category_simplified                                                 as account_category
      , dt.total_users_at_dealer                                                               as eligible_users
      , coalesce(pv.users_viewing,    0)                                                       as users_viewing
      , coalesce(i.users_interacting, 0)                                                       as users_interacting
      , round(coalesce(pv.users_viewing,    0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as viewing_pct
      , round(coalesce(i.users_interacting, 0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as interaction_pct
    from dealer_totals dt
    cross join dates d
    left join page_views pv  on pv.service_provider_id = dt.service_provider_id and pv.date = d.date
    left join interactions i on i.service_provider_id  = dt.service_provider_id and i.date  = d.date
    order by d.date desc, interaction_pct desc nulls last
    """

    return (
        _session.sql(sql_dealer).to_pandas(),
        _session.sql(sql_period).to_pandas(),
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_feature_breakdown(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True, ddi_only: bool = False, dealer_sizes: tuple[str, ...] = ()) -> pd.DataFrame:
    sql = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff, ddi_only, list(dealer_sizes))}
    , vin_interactions_by_element as (
        select
            be.region
          , 'Performance'                          as tab
          , be.derived_tstamp::date                as date
          , be.sd_element                          as cta
          , count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in {_PERF_F_SQL}
          and be.sd_element in {_PERF_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
        group by all
    )
    , competitors_interactions_by_element as (
        select
            be.region
          , 'Competitors'                          as tab
          , be.derived_tstamp::date                as date
          , case
                when be.sd_feature = 'ComparisonOverlay' and be.sd_element = 'AttributeToggle'
                    then 'Map Comparison · ' || parse_json(be.sd_element_value):"attribute"::string
                else be.sd_element
            end                                    as cta
          , count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in {_COMP_F_SQL}
          and be.sd_element in {_COMP_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}
        group by all
    )
    select
        v.tab
      , v.date
      , v.cta
      , v.dealers_interacting
      , v.users_interacting
      , round(v.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealer_share_pct
      , round(v.users_interacting   / nullif(t.total_users,   0) * 100, 1) as user_share_pct
    from vin_interactions_by_element v
    inner join totals t on t._region_ = v.region

    union all

    select
        c.tab
      , c.date
      , c.cta
      , c.dealers_interacting
      , c.users_interacting
      , round(c.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealer_share_pct
      , round(c.users_interacting   / nullif(t.total_users,   0) * 100, 1) as user_share_pct
    from competitors_interactions_by_element c
    inner join totals t on t._region_ = c.region

    order by tab, date desc, dealer_share_pct desc nulls last
    """
    return _session.sql(sql).to_pandas()


@st.cache_data(ttl=3600, show_spinner=False)
def load_data_freshness(_session) -> pd.DataFrame:
    sql = """
    select
        max(_transformed_at_) as max_transformed_at
      , max(derived_tstamp)   as max_derived_tstamp
    from analytics.traffic.dealer_dashboard_events_normalized
    where 1=1
      and is_staff = false
      and is_bot   = false
      and sd_product in ('Performance', 'Competitors')
    """
    return _session.sql(sql).to_pandas()


@st.cache_data(ttl=3600, show_spinner=False)
def load_user_breakdown(_session, dealer_id: int, product_section: str, days: int, engagement_only: bool) -> pd.DataFrame:
    if product_section == "Performance":
        interaction_filter = f"""
          and be.sd_feature in {_PERF_F_SQL}
          and be.sd_element in {_PERF_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}"""
    else:
        interaction_filter = f"""
          and be.sd_feature in {_COMP_F_SQL}
          and be.sd_element in {_COMP_E_SQL}
          and be.sd_event_type in {_EVENTS_SQL}"""

    engagement_join = f"""
    inner join (
        select distinct user_id
        from analytics.traffic.dealer_dashboard_events_normalized
        where derived_tstamp >= current_date() - 180
          and sd_application = 'Dealer_Dashboard'
          and is_staff = false
          and service_provider_id = {dealer_id}
    ) eng on eng.user_id = s.user_uuid""" if engagement_only else ""

    sql = f"""
    with dealer_users as (
        select
            s.user_uuid
          , any_value(s.email) as email
        from warehouse.site.service_providers sp
        left join warehouse.site.person_roles pr
            on cast(substring(pr.entity_id, 3) as int) = sp.location_id
        left join warehouse.site.people p on p.id = pr.person_id
        left join warehouse.site.subscribers s on s.person_id = pr.person_id
        {engagement_join}
        where pr.enabled = 1
          and pr.role_name = 'ROLE_DD_ADMIN'
          and sp.location_id = {dealer_id}
        group by s.user_uuid
    )
    , page_views as (
        select
            be.user_id
          , count(*) as page_view_count
        from analytics.traffic.dealer_dashboard_events_normalized be
        where be.derived_tstamp >= current_date() - {days}
          and be.region = 'NA'
          and be.sd_application = 'Dealer_Dashboard'
          and be.sd_product_section = '{product_section}'
          and be.source = 'cargurus_dealer_pageview_tracking'
          and be.service_provider_id = {dealer_id}
        group by all
    )
    , interactions as (
        select
            be.user_id
          , count(*) as interaction_count
        from analytics.traffic.dealer_dashboard_events_normalized be
        where be.derived_tstamp >= current_date() - {days}
          and be.region = 'NA'
          and be.sd_application = 'Dealer_Dashboard'
          and be.sd_product_section = '{product_section}'
          and be.service_provider_id = {dealer_id}
          {interaction_filter}
        group by all
    )
    select
        du.user_uuid                              as user_id
      , du.email
      , coalesce(pv.page_view_count, 0)           as page_views
      , coalesce(i.interaction_count, 0)          as interactions
    from dealer_users du
    left join page_views pv  on pv.user_id = du.user_uuid
    left join interactions i on i.user_id  = du.user_uuid
    order by interactions desc nulls last, page_views desc nulls last
    """
    return _session.sql(sql).to_pandas()
