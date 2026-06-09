import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

from config import ACCEPT_STAFF, C_BLUE, C_GREEN, C_AMBER, C_PURPLE, TZ_EASTERN
from queries import (
    load_account_categories,
    load_dealer_sizes,
    load_overview,
    load_performance,
    load_competitors,
    load_feature_breakdown,
    load_active_rates,
    load_data_freshness,
)
from components import normalize_cols, trend_chart, dealer_table

session = get_active_session()

st.set_page_config(
    page_title="Dealer Dashboard · Engagement",
    page_icon="📊",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Filters")
    engagement_only = st.toggle(
        "Engaged users only (last 6 months)",
        value=True,
        help="When ON, denominators include only dealers/users who visited the Dealer Dashboard in the last 180 days.",
    )
    ddi_only = st.toggle(
        "DDI report active only",
        value=True,
        help="When ON, limits to dealers with an active DDI report subscription.",
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
    all_dealer_sizes = load_dealer_sizes(session)
    selected_dealer_sizes = st.multiselect(
        "Dealer size",
        options=all_dealer_sizes,
        default=all_dealer_sizes,
        help="Filter by dealer size derived from account category.",
    )

engagement_where = "used_dep_last_180_days" if engagement_only else "true"
ddi_filter_active = ddi_only
dealer_size_tuple = tuple(selected_dealer_sizes)
cat_tuple = tuple(selected_categories)

# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Loading data…"):
    df_freshness                             = normalize_cols(load_data_freshness(session))
    df_overview, df_overview_kpi             = [normalize_cols(d) for d in load_overview(session, engagement_where, days, cat_tuple, ACCEPT_STAFF, ddi_filter_active, dealer_size_tuple)]
    df_perf_dealer, df_perf_kpi = [normalize_cols(d) for d in load_performance(session, engagement_where, days, cat_tuple, ACCEPT_STAFF, ddi_filter_active, dealer_size_tuple)]
    df_comp_dealer, df_comp_kpi = [normalize_cols(d) for d in load_competitors(session, engagement_where, days, cat_tuple, ACCEPT_STAFF, ddi_filter_active, dealer_size_tuple)]
    df_feature_breakdown                     = normalize_cols(load_feature_breakdown(session, engagement_where, days, cat_tuple, ACCEPT_STAFF, ddi_filter_active, dealer_size_tuple))
    df_active_rates                          = normalize_cols(load_active_rates(session, engagement_where, days, cat_tuple, ACCEPT_STAFF, ddi_filter_active, dealer_size_tuple))

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_overview, tab_performance, tab_competitors = st.tabs(
    ["📈 Overview", "🚗 Performance", "🏁 Competitive Landscape"]
)

def to_et(val):
    ts = pd.to_datetime(val)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(TZ_EASTERN)


# ── Overview ──────────────────────────────────────────────────────────────────
with tab_overview:
    st.header(f"Engagement Overview · {period_label} (daily)")

    if not df_freshness.empty:
        now = pd.Timestamp.now("UTC").tz_convert(TZ_EASTERN)

        max_transformed = to_et(df_freshness["MAX_TRANSFORMED_AT"].iloc[0])
        max_derived     = to_et(df_freshness["MAX_DERIVED_TSTAMP"].iloc[0])
        is_fresh = (now - max_transformed <= pd.Timedelta(days=1)) and (now - max_derived <= pd.Timedelta(days=1))
        accent  = C_GREEN if is_fresh else C_AMBER
        status  = "Data is up to date." if is_fresh else "Data may be stale (last update was more than 1 day ago)."
        st.markdown(
            f"""<div style="background-color:{accent}33; border:1px solid {accent}99;
                            border-radius:6px; padding:10px 16px; margin-bottom:12px;">
                <strong>{status}</strong><br>
                <span style="font-size:0.85em;">
                    Model last run: <strong>{max_transformed.strftime('%Y-%m-%d %H:%M ET')}</strong>
                    &nbsp;·&nbsp;
                    Last interaction recorded: <strong>{max_derived.strftime('%Y-%m-%d %H:%M ET')}</strong>
                </span><br>
                <span style="font-size:0.8em; opacity:0.65;">
                    Reflects Performance &amp; Competitors tab activity in the Dealer Engagement Platform.
                </span>
            </div>""",
            unsafe_allow_html=True,
        )

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

    ar = df_active_rates.iloc[0] if not df_active_rates.empty else None
    c7, c8, c9 = st.columns(3)
    c7.metric(
        f"DAU Rate (avg daily active dealers · {period_label.lower()})",
        f"{ar['DAU_PCT']:.1f}%" if ar is not None else "—",
        help="Average daily unique dealers visiting Competitors or Performance, divided by total eligible dealers.",
    )
    c8.metric(
        f"WAU Rate (avg weekly active dealers · {period_label.lower()})",
        f"{ar['WAU_PCT']:.1f}%" if ar is not None else "—",
        help="Average weekly unique dealers visiting Competitors or Performance, divided by total eligible dealers.",
    )
    c9.metric(
        f"MAU Rate (avg monthly active dealers · {period_label.lower()})",
        f"{ar['MAU_PCT']:.1f}%" if ar is not None else "—",
        help="Average monthly unique dealers visiting Competitors or Performance, divided by total eligible dealers.",
    )

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(
            trend_chart(df_overview, "DEALERS_VIEWING_PCT", "PRODUCT",
                        "% Dealers Viewing Page (daily)",
                        {"Competitors": C_BLUE, "Performance": C_GREEN}),
            use_container_width=True,
        )
    with col2:
        st.altair_chart(
            trend_chart(df_overview, "USERS_VIEWING_PCT", "PRODUCT",
                        "% Users Viewing Page (daily)",
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
            "DATE":              "Date",
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
    st.divider()
    st.subheader(f"CTA breakdown · {period_label.lower()}")
    df_perf_features = df_feature_breakdown[df_feature_breakdown["TAB"] == "Performance"].drop(columns="TAB")
    dealer_table(
        df_perf_features.rename(columns={
            "DATE":                "Date",
            "CTA":                 "CTA",
            "DEALERS_INTERACTING": "Dealers Interacting",
            "USERS_INTERACTING":   "Users Interacting",
            "DEALER_SHARE_PCT":    "Dealer Share %",
            "USER_SHARE_PCT":      "User Share %",
        }),
        pct_cols=["Dealer Share %", "User Share %"],
    )
    # TODO: remove after user-level QA
    # user_breakdown_section("perf", "Performance", days, df_perf_dealer, session, engagement_only)


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
            "DATE":              "Date",
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
    st.divider()
    st.subheader(f"CTA breakdown · {period_label.lower()}")
    df_comp_features = df_feature_breakdown[df_feature_breakdown["TAB"] == "Competitors"].drop(columns="TAB")
    dealer_table(
        df_comp_features.rename(columns={
            "DATE":                "Date",
            "CTA":                 "CTA",
            "DEALERS_INTERACTING": "Dealers Interacting",
            "USERS_INTERACTING":   "Users Interacting",
            "DEALER_SHARE_PCT":    "Dealer Share %",
            "USER_SHARE_PCT":      "User Share %",
        }),
        pct_cols=["Dealer Share %", "User Share %"],
    )
    # TODO: remove after user-level QA
    # user_breakdown_section("comp", "Competitors", days, df_comp_dealer, session, engagement_only)
