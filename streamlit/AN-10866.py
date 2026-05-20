import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(
    page_title="Dealer Dashboard · Engagement",
    page_icon="📊",
    layout="wide",
)

DEALER_LIST = "54077,283470,66205,297439,55490,297177,54534,282549,291629,335671,367058,63842,66456,60421,282352,287086,66446,63840,55378,67362,273996,58711,287087,58223,304494,64657,412815,289297,53811,279634,433241,55841,287317,59349,58736,57180,58650,368892,459744,337667,284835,431377,447168,279879,276171,377067,101337,50415,54098,279723,289989,63494,275253,306754,61442,276248,414813,84329,379256,311039,54295,282140,54554,290383,49920,283972,408480,49888,49876,114130,59980,287183,434784,297590,275971,281789,458694,63707,334007,83363,400283,275282,275285,342616,370399,380000,334305,277660,303618,443635,67710,59061,382378,312994,273827,310521,413115,325533,452465,436973,340136,67590,283096,306576,334514,367132,66184,336961,68061,284188,443402,66064,66198,277332,291060,50190,67659,407794,407791,65557,344517,53877,306780"

C_BLUE   = "#4F8EF7"
C_GREEN  = "#34D399"
C_AMBER  = "#FBBF24"
C_PURPLE = "#A78BFA"

# True = include staff events (during internal testing); False = exclude staff after client release
ACCEPT_STAFF = True


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


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Filters")
    engagement_only = st.toggle("Engaged users only (last 180d)", value=True)
    st.caption(
        "When ON, denominators include only dealers/users who visited "
        "the Dealer Dashboard in the last 180 days."
    )
    st.divider()
    days_options = {
        "Last 7 days":   7,
        "Last 14 days":  14,
        "Last 30 days":  30,
        "Last 60 days":  60,
        "Last 90 days":  90,
        "Custom…":       None,
    }
    period_label = st.selectbox("Date range", list(days_options.keys()), index=3)
    if days_options[period_label] is None:
        max_days = (pd.Timestamp.today() - pd.Timestamp("2026-01-01")).days
        custom_days = st.number_input("Number of days", min_value=1, max_value=max_days, value=30, step=1)
        days = int(custom_days)
        period_label = f"Last {days} days"
    else:
        days = days_options[period_label]
    st.divider()
    all_categories = load_account_categories(session)
    selected_categories = st.multiselect(
        "Account category",
        options=all_categories,
        default=all_categories,
        help="Filter dealer breakdown by account category.",
    )

engagement_where = "used_dep_last_180_days" if engagement_only else "true"

# ─────────────────────────────────────────────────────────────────────────────
# Shared CTEs
# ─────────────────────────────────────────────────────────────────────────────
def _sql_in_list(values: list[str]) -> str:
    escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in values)
    return f"({escaped})"


def common_ctes(ew: str, days: int, account_categories: list[str] | None = None, accept_staff: bool = True) -> str:
    cat_filter = (
        f"and current_account_category_simplified in {_sql_in_list(account_categories)}"
        if account_categories
        else ""
    )
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
    )
    , dealers_and_users as (
        select
            sp._region_
          , sp.location_id as service_provider_id
          , s.user_uuid
          , dm.current_dealer_name
          , dm.current_account_category_simplified
        from warehouse.site.service_providers sp
        left join warehouse.site.person_roles pr
            on cast(substring(pr.entity_id, 3) as int) = sp.location_id
        left join warehouse.site.people p on p.id = pr.person_id
        left join warehouse.site.subscribers s on s.person_id = pr.person_id
        left join (
            select service_provider_id, current_dealer_name, current_account_category_simplified
            from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
            where country_code = 'US'
              and inventory_date = (
                select max(inventory_date)
                from analytics.competitive_intelligence.performance_health_metric_comparison_monthly
                where country_code = 'US'
              )
        ) dm on dm.service_provider_id = sp.location_id
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
        group by all
    )
    , dealer_totals as (
        select
            _region_
          , service_provider_id
          , any_value(current_dealer_name)     as current_dealer_name
          , any_value(current_account_category_simplified) as current_account_category_simplified
          , count(distinct user_uuid)           as total_users_at_dealer
        from user_engagement_flags
        where {ew}
          {cat_filter}
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

# ─────────────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_overview(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql_weekly = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , page_views as (
        select
            be.region
          , date_trunc('week', be.derived_tstamp::date)  as week_date
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
        pv.week_date
      , pv.product
      , t.total_dealers
      , t.total_users
      , pv.dealers_viewing
      , pv.users_viewing
      , round(pv.dealers_viewing / nullif(t.total_dealers, 0) * 100, 1) as dealers_viewing_pct
      , round(pv.users_viewing   / nullif(t.total_users,   0) * 100, 1) as users_viewing_pct
    from totals t
    inner join page_views pv on t._region_ = pv.region
    order by pv.week_date desc
    """

    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
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
def load_performance(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sql_agg = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , interactions as (
        select
            be.region
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.service_provider_id)      as dealers_interacting
          , count(distinct be.user_id)                  as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in ('QuickActions', 'PerformanceNavigation')
          and be.sd_element in ('PricingRecommendation', 'SourcingRecommendation',
                                'MerchandisingRecommendation', 'DealRatingLearnMore', 'LowVDPsLearnMore')
          and be.sd_event_type in ('Click', 'Change')
        group by all
    )
    select
        i.week_date
      , t.total_dealers
      , t.total_users
      , i.dealers_interacting
      , i.users_interacting
      , round(i.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealers_interacting_pct
      , round(i.users_interacting   / nullif(t.total_users,   0) * 100, 1) as users_interacting_pct
    from totals t
    inner join interactions i on t._region_ = i.region
    order by i.week_date desc
    """

    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , summary as (
        select
            count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in ('QuickActions', 'PerformanceNavigation')
          and be.sd_element in ('PricingRecommendation', 'SourcingRecommendation',
                                'MerchandisingRecommendation', 'DealRatingLearnMore', 'LowVDPsLearnMore')
          and be.sd_event_type in ('Click', 'Change')
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
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , page_views as (
        select
            be.service_provider_id
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.user_id) as users_viewing
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    , interactions as (
        select
            be.service_provider_id
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.user_id) as users_interacting
        from base_events be
        where be.sd_product_section = 'Performance'
          and be.sd_feature in ('QuickActions', 'PerformanceNavigation')
          and be.sd_element in ('PricingRecommendation', 'SourcingRecommendation',
                                'MerchandisingRecommendation', 'DealRatingLearnMore', 'LowVDPsLearnMore')
          and be.sd_event_type in ('Click', 'Change')
        group by all
    )
    , weeks as (
        select distinct week_date from page_views
        union
        select distinct week_date from interactions
    )
    select
        w.week_date
      , dt.service_provider_id                                                                 as dealer_id
      , dt.current_dealer_name                                                                 as dealer_name
      , dt.current_account_category_simplified                                                 as account_category
      , dt.total_users_at_dealer                                                               as eligible_users
      , coalesce(pv.users_viewing,    0)                                                       as users_viewing
      , coalesce(i.users_interacting, 0)                                                       as users_interacting
      , round(coalesce(pv.users_viewing,    0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as viewing_pct
      , round(coalesce(i.users_interacting, 0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as interaction_pct
    from dealer_totals dt
    cross join weeks w
    left join page_views pv  on pv.service_provider_id = dt.service_provider_id and pv.week_date = w.week_date
    left join interactions i on i.service_provider_id  = dt.service_provider_id and i.week_date  = w.week_date
    order by w.week_date desc, interaction_pct desc nulls last
    """

    return (
        _session.sql(sql_agg).to_pandas(),
        _session.sql(sql_dealer).to_pandas(),
        _session.sql(sql_period).to_pandas(),
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_competitors(_session, ew: str, days: int, account_categories: tuple[str, ...] = (), accept_staff: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sql_agg = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , interactions as (
        select
            be.region
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.service_provider_id)      as dealers_interacting
          , count(distinct be.user_id)                  as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in ('CompetitorsFilters', 'CompetitorsList', 'ShowTopCompetitorsButton')
          and be.sd_element in ('TableSort', 'RadiusSelector', 'SearchOverlapSelector',
                                'FranchiseTypeSelector', 'ViewStats', 'ShowTopCompetitorsButton')
          and be.sd_event_type in ('Click', 'Change')
        group by all
    )
    select
        i.week_date
      , t.total_dealers
      , t.total_users
      , i.dealers_interacting
      , i.users_interacting
      , round(i.dealers_interacting / nullif(t.total_dealers, 0) * 100, 1) as dealers_interacting_pct
      , round(i.users_interacting   / nullif(t.total_users,   0) * 100, 1) as users_interacting_pct
    from totals t
    inner join interactions i on t._region_ = i.region
    order by i.week_date desc
    """

    sql_period = f"""
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , summary as (
        select
            count(distinct be.service_provider_id) as dealers_interacting
          , count(distinct be.user_id)             as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in ('CompetitorsFilters', 'CompetitorsList', 'ShowTopCompetitorsButton')
          and be.sd_element in ('TableSort', 'RadiusSelector', 'SearchOverlapSelector',
                                'FranchiseTypeSelector', 'ViewStats', 'ShowTopCompetitorsButton')
          and be.sd_event_type in ('Click', 'Change')
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
    with {common_ctes(ew, days, list(account_categories), accept_staff)}
    , page_views as (
        select
            be.service_provider_id
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.user_id) as users_viewing
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.source = 'cargurus_dealer_pageview_tracking'
        group by all
    )
    , interactions as (
        select
            be.service_provider_id
          , date_trunc('week', be.derived_tstamp::date) as week_date
          , count(distinct be.user_id) as users_interacting
        from base_events be
        where be.sd_product_section = 'Competitors'
          and be.sd_feature in ('CompetitorsFilters', 'CompetitorsList')
          and be.sd_element in ('TableSort', 'RadiusSelector', 'SearchOverlapSelector',
                                'FranchiseTypeSelector', 'ViewStats')
          and be.sd_event_type in ('Click', 'Change')
        group by all
    )
    , weeks as (
        select distinct week_date from page_views
        union
        select distinct week_date from interactions
    )
    select
        w.week_date
      , dt.service_provider_id                                                                 as dealer_id
      , dt.current_dealer_name                                                                 as dealer_name
      , dt.current_account_category_simplified                                                 as account_category
      , dt.total_users_at_dealer                                                               as eligible_users
      , coalesce(pv.users_viewing,    0)                                                       as users_viewing
      , coalesce(i.users_interacting, 0)                                                       as users_interacting
      , round(coalesce(pv.users_viewing,    0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as viewing_pct
      , round(coalesce(i.users_interacting, 0) / nullif(dt.total_users_at_dealer, 0) * 100, 1) as interaction_pct
    from dealer_totals dt
    cross join weeks w
    left join page_views pv  on pv.service_provider_id = dt.service_provider_id and pv.week_date = w.week_date
    left join interactions i on i.service_provider_id  = dt.service_provider_id and i.week_date  = w.week_date
    order by w.week_date desc, interaction_pct desc nulls last
    """

    return (
        _session.sql(sql_agg).to_pandas(),
        _session.sql(sql_dealer).to_pandas(),
        _session.sql(sql_period).to_pandas(),
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_user_breakdown(_session, dealer_id: int, product_section: str, days: int, engagement_only: bool) -> pd.DataFrame:
    if product_section == "Performance":
        interaction_filter = """
          and be.sd_feature in ('QuickActions', 'PerformanceNavigation')
          and be.sd_element in ('PricingRecommendation', 'SourcingRecommendation',
                                'MerchandisingRecommendation', 'DealRatingLearnMore', 'LowVDPsLearnMore')
          and be.sd_event_type in ('Click', 'Change')"""
    else:
        interaction_filter = """
          and be.sd_feature in ('CompetitorsFilters', 'CompetitorsList', 'ShowTopCompetitorsButton')
          and be.sd_element in ('TableSort', 'RadiusSelector', 'SearchOverlapSelector',
                                'FranchiseTypeSelector', 'ViewStats', 'ShowTopCompetitorsButton')
          and be.sd_event_type in ('Click', 'Change')"""

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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper() for c in df.columns]
    return df


def trend_chart(df, y_col, color_col, title, color_map):
    df = df.copy()
    df["WEEK_DATE"] = pd.to_datetime(df["WEEK_DATE"]).dt.strftime("%b %d")
    return (
        alt.Chart(df)
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=60), strokeWidth=2.5)
        .encode(
            x=alt.X("WEEK_DATE:O", title="Week starting", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y(f"{y_col}:Q", title="% of Total", scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(values=[0,25,50,75,100])),
            color=alt.Color(
                f"{color_col}:N",
                scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())),
                legend=alt.Legend(orient="bottom"),
            ),
            tooltip=[
                alt.Tooltip("WEEK_DATE:O", title="Week starting"),
                alt.Tooltip(f"{color_col}:N", title="Segment"),
                alt.Tooltip(f"{y_col}:Q", title="% Total", format=".1f"),
            ],
        )
        .properties(title=title, height=280)
    )


def dealer_table(df: pd.DataFrame, pct_cols: list[str]):
    col_config = {
        c: st.column_config.ProgressColumn(c, min_value=0, max_value=100, format="%.1f%%")
        for c in pct_cols
    }
    st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_config)


def user_breakdown_section(tab_key: str, product_section: str, days: int, df_dealer: pd.DataFrame):
    st.divider()
    st.subheader("User breakdown · drill into a dealer")

    options = [None] + [
        (int(row["DEALER_ID"]), row["DEALER_NAME"] or f"ID {row['DEALER_ID']}")
        for _, row in df_dealer.iterrows()
    ]
    selected = st.selectbox(
        "Select dealer",
        options=options,
        format_func=lambda x: "— select a dealer —" if x is None else f"{x[0]} · {x[1]}",
        key=f"dealer_input_{tab_key}",
    )
    if selected is None:
        return

    dealer_id = selected[0]
    with st.spinner("Loading user data…"):
        df_users = normalize_cols(load_user_breakdown(session, dealer_id, product_section, days, engagement_only))

    if df_users.empty:
        st.info("No users found for this dealer.")
        return

    st.dataframe(
        df_users.rename(columns={
            "USER_ID":      "User ID",
            "EMAIL":        "Email",
            "PAGE_VIEWS":   "Page Views",
            "INTERACTIONS": "Interactions",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────
cat_tuple = tuple(selected_categories)

with st.spinner("Loading data…"):
    df_overview, df_overview_kpi             = [normalize_cols(d) for d in load_overview(session, engagement_where, days, cat_tuple, ACCEPT_STAFF)]
    df_perf_agg, df_perf_dealer, df_perf_kpi = [normalize_cols(d) for d in load_performance(session, engagement_where, days, cat_tuple, ACCEPT_STAFF)]
    df_comp_agg, df_comp_dealer, df_comp_kpi = [normalize_cols(d) for d in load_competitors(session, engagement_where, days, cat_tuple, ACCEPT_STAFF)]

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_overview, tab_performance, tab_competitors = st.tabs(
    ["📈 Overview", "🚗 Performance", "🏁 Competitive Landscape"]
)

# ── Overview ──────────────────────────────────────────────────────────────────
with tab_overview:
    st.header(f"Engagement Overview · {period_label} (weekly)")

    kpi_comp = df_overview_kpi[df_overview_kpi["PRODUCT"] == "Competitors"]
    kpi_perf = df_overview_kpi[df_overview_kpi["PRODUCT"] == "Performance"]
    total_d = int(kpi_comp["TOTAL_DEALERS"].iloc[0]) if not kpi_comp.empty else 0
    total_u = int(kpi_comp["TOTAL_USERS"].iloc[0])   if not kpi_comp.empty else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Eligible Dealers", f"{total_d:,}")
    c2.metric("Total Eligible Users",   f"{total_u:,}")
    c3.metric(
        "Dealers Viewing · Competitors",
        f"{kpi_comp['DEALERS_VIEWING_PCT'].iloc[0]:.1f}%" if not kpi_comp.empty else "—",
    )
    c4.metric(
        "Users Viewing · Competitors",
        f"{kpi_comp['USERS_VIEWING_PCT'].iloc[0]:.1f}%" if not kpi_comp.empty else "—",
    )
    c5.metric(
        "Dealers Viewing · Performance",
        f"{kpi_perf['DEALERS_VIEWING_PCT'].iloc[0]:.1f}%" if not kpi_perf.empty else "—",
    )
    c6.metric(
        "Users Viewing · Performance",
        f"{kpi_perf['USERS_VIEWING_PCT'].iloc[0]:.1f}%" if not kpi_perf.empty else "—",
    )

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(
            trend_chart(df_overview, "DEALERS_VIEWING_PCT", "PRODUCT",
                        "% Dealers Viewing Page (weekly)",
                        {"Competitors": C_BLUE, "Performance": C_GREEN}),
            use_container_width=True,
        )
    with col2:
        st.altair_chart(
            trend_chart(df_overview, "USERS_VIEWING_PCT", "PRODUCT",
                        "% Users Viewing Page (weekly)",
                        {"Competitors": C_AMBER, "Performance": C_PURPLE}),
            use_container_width=True,
        )

    with st.expander("Raw data"):
        st.dataframe(df_overview, use_container_width=True, hide_index=True)


# ── Performance ───────────────────────────────────────────────────────────────
with tab_performance:
    st.header("Performance · VIN-specific Insights Interaction")

    kp = df_perf_kpi.iloc[0] if not df_perf_kpi.empty else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Eligible Dealers", f"{int(kp['TOTAL_DEALERS']):,}" if kp is not None else "—")
    c2.metric("Total Eligible Users",   f"{int(kp['TOTAL_USERS']):,}"   if kp is not None else "—")
    c3.metric(
        f"Dealers Interacting · {period_label.lower()}",
        f"{kp['DEALERS_INTERACTING_PCT']:.1f}%" if kp is not None else "—",
    )
    c4.metric(
        f"Users Interacting · {period_label.lower()}",
        f"{kp['USERS_INTERACTING_PCT']:.1f}%" if kp is not None else "—",
    )

    st.divider()
    st.subheader(f"Dealer breakdown · {period_label.lower()}")
    dealer_table(
        df_perf_dealer.rename(columns={
            "WEEK_DATE":         "Week",
            "DEALER_ID":         "Dealer ID",
            "DEALER_NAME":       "Dealer Name",
            "ACCOUNT_CATEGORY":  "Account Category",
            "ELIGIBLE_USERS":    "Eligible Users",
            "USERS_VIEWING":     "Users Viewing",
            "USERS_INTERACTING": "Users Interacting",
            "VIEWING_PCT":       "Viewing %",
            "INTERACTION_PCT":   "Interaction %",
        }),
        pct_cols=["Viewing %", "Interaction %"],
    )
    user_breakdown_section("perf", "Performance", days, df_perf_dealer)


# ── Competitive Landscape ─────────────────────────────────────────────────────
with tab_competitors:
    st.header("Competitive Landscape · Filter & List Interaction")

    kc = df_comp_kpi.iloc[0] if not df_comp_kpi.empty else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Eligible Dealers", f"{int(kc['TOTAL_DEALERS']):,}" if kc is not None else "—")
    c2.metric("Total Eligible Users",   f"{int(kc['TOTAL_USERS']):,}"   if kc is not None else "—")
    c3.metric(
        f"Dealers Interacting · {period_label.lower()}",
        f"{kc['DEALERS_INTERACTING_PCT']:.1f}%" if kc is not None else "—",
    )
    c4.metric(
        f"Users Interacting · {period_label.lower()}",
        f"{kc['USERS_INTERACTING_PCT']:.1f}%" if kc is not None else "—",
    )

    st.divider()
    st.subheader(f"Dealer breakdown · {period_label.lower()}")
    dealer_table(
        df_comp_dealer.rename(columns={
            "WEEK_DATE":         "Week",
            "DEALER_ID":         "Dealer ID",
            "DEALER_NAME":       "Dealer Name",
            "ACCOUNT_CATEGORY":  "Account Category",
            "ELIGIBLE_USERS":    "Eligible Users",
            "USERS_VIEWING":     "Users Viewing",
            "USERS_INTERACTING": "Users Interacting",
            "VIEWING_PCT":       "Viewing %",
            "INTERACTION_PCT":   "Interaction %",
        }),
        pct_cols=["Viewing %", "Interaction %"],
    )
    user_breakdown_section("comp", "Competitors", days, df_comp_dealer)