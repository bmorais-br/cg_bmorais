# DEP Engagement Measurement — Streamlit Dashboard

## What this is

A **Streamlit in Snowflake (SiS)** app that measures dealer engagement with the Dealer Engagement Platform (DEP) — specifically the **Competitive Insights (Competitors tab)** and **Performance** features — for a closed beta cohort of dealers.

The dashboard is deployed inside Snowflake and uses `get_active_session()` to query Snowflake directly. There is no local dev server; all SQL runs in Snowflake.

## File map

| File | Role |
|---|---|
| `AN-10866.py` | **Primary dashboard file — all edits go here.** Streamlit entry point: sidebar filters, data loading, tab layout. |
| `queries.py` | All SQL queries as Python functions. Each function returns a `pd.DataFrame` via `_session.sql(...).to_pandas()`. Results are cached with `@st.cache_data(ttl=3600)`. |
| `components.py` | Reusable UI helpers: `normalize_cols`, `trend_chart` (Altair), `dealer_table`, `user_breakdown_section`. |
| `config.py` | Constants: `ACCEPT_STAFF`, color hex values, `TZ_EASTERN`. Imports all cohort lists and dates from `dealers.py`. |
| `dealers.py` | **Generated file — do not edit by hand.** Contains one `*_LIST` / `*_DATES` pair per dealer cohort. Regenerate via `generate_dealers.py`. |
| `generate_dealers.py` | Local-only script. Reads all 4 CSVs and prints updated `dealers.py` content to stdout. Run `python generate_dealers.py`, then paste output into `dealers.py`. |
| `data/Competitive Insights _ Beta Dealer Sign Ups - Finalized Dealer List (DO NOT EDIT).csv` | Source of truth for the Beta Dealers cohort. Maintained externally (Google Sheets export). |
| `data/Competitive Insights _ Beta Dealer Sign Ups - AutoNation Beta Dealers.csv` | Source of truth for the AutoNation cohort. |
| `data/Competitive Insights _ Beta Dealer Sign Ups - Ken Graff Automotive Dealers.csv` | Source of truth for the Ken Graff Automotive cohort. |
| `data/Competitive Insights _ Beta Dealer Sign Ups - Shopper Signals Signups.csv` | Source of truth for the Shopper Signals cohort. |

## Dealer cohort workflow

The sidebar "Dealer cohort" selectbox switches the entire dealer population (denominators + events) across all metrics. The four cohorts are:

| Label | Variable prefix | CSV |
|---|---|---|
| Beta Dealers | `DEALER_` | `Finalized Dealer List (DO NOT EDIT).csv` |
| AutoNation | `AUTONATION_` | `AutoNation Beta Dealers.csv` |
| Ken Graff Automotive | `KEN_GRAFF_` | `Ken Graff Automotive Dealers.csv` |
| Shopper Signals | `SHOPPER_SIGNALS_` | `Shopper Signals Signups.csv` |

When any CSV is updated, regenerate `dealers.py`:

```bash
python generate_dealers.py
# paste stdout into dealers.py
```

The `_COHORTS` dict in `AN-10866.py` maps each label to `(dealer_list_str, dealer_dates_tuple)`. Every `load_*` function in `queries.py` accepts a `dealer_list_override` param; `load_adoption_rate` additionally accepts `dealer_dates` as a `tuple[tuple[int, str], ...]` (required to be hashable for `@st.cache_data`).

**Adding a fifth cohort:** add a new CSV to `data/`, add a `parse_*` function (or reuse `parse_generic_csv`) in `generate_dealers.py`, regenerate `dealers.py`, import the new pair in `config.py`, and add an entry to `_COHORTS` in `AN-10866.py`.

**Important known issue (Beta Dealers only):** `generate_dealers.py` does NOT filter on the `INCLUDED IN BETA?` column — it only skips rows with "Removed" in `Added/Removed`. This means it may include dealers where `INCLUDED IN BETA? = N`. The fix is to add: `if row[beta_idx].strip().upper() != "Y": continue`. Also watch for duplicate SPIDs (they produce duplicate entries in `DEALER_LIST` and the later `DEALER_DATES` entry wins).

## Key design decisions

- **`ACCEPT_STAFF = False`** in `config.py` — staff events are excluded from all queries in production. Do not change to `True` unless doing internal testing.
- All queries share a common CTE builder `common_ctes()` in `queries.py`. When adding new queries, use this function to build the `WITH` clause rather than writing CTEs from scratch.
- The `engagement_where` toggle controls whether denominators use all eligible dealers/users or only those active in the last 180 days (`used_dep_last_180_days`).
- The `ddi_only` toggle restricts the cohort to dealers with an active DDI report subscription.
- `*_DATES` dicts (per-dealer beta start dates) are used only by `load_adoption_rate` to measure 30-day adoption from each dealer's individual start date. Passed as a `tuple[tuple[int, str], ...]` for `@st.cache_data` hashability.

## Snowflake tables used

| Table | Purpose |
|---|---|
| `analytics.traffic.dealer_dashboard_events_normalized` | Raw normalized event stream — page views and interactions |
| `analytics.traffic.dealer_dashboard_daily_stats` | Pre-aggregated daily stats for DAU/WAU/MAU |
| `analytics.unified_dealer_data_mart.performance_health_metric_comparison_monthly` | Dealer metadata: name, account category, dealer size |
| `warehouse.site.service_providers` | Dealer/SP records |
| `warehouse.site.person_roles` | User-to-dealer role assignments (`ROLE_DD_ADMIN`) |
| `warehouse.site.subscribers` | User UUIDs |
| `warehouse.site.drs_dealer_reports` / `drs_reports_to_subscribers` | DDI subscription data |

## Dashboard tabs

- **Overview** — daily trend charts (% dealers/users viewing Competitors and Performance), data freshness banner, DAU/WAU/MAU activity rates.
- **Adoption Insights** — 30-day adoption rate (per-dealer start dates), 30-day return rate, cross-page usage rate.
- **Performance** — VIN-specific interaction metrics, dealer breakdown table, CTA breakdown.
- **Competitive Landscape** — filter & list interaction metrics, dealer breakdown table, CTA breakdown.

## Interaction signal definitions (in `queries.py`)

`PERF_INTERACTION_FEATURES` / `PERF_INTERACTION_ELEMENTS` and `COMP_INTERACTION_FEATURES` / `COMP_INTERACTION_ELEMENTS` define which Snowplow feature/element combinations count as an "interaction" for each tab. Update these tuples when new CTAs are instrumented.
