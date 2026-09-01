# CI Business Impact Analysis — Context for Claude

This folder contains pre/post behavioral analyses measuring how dealers change pricing and
merchandising behavior after their first Competitive Intelligence (CI) product view.

---

## Product Context

- **Product launch date:** May 21, 2026
- **CI tabs:** Performance, Competitors
- **Tracking surface:** `analytics.traffic.dealer_dashboard_events_normalized`
- **Region filter:** `region = 'NA'` — this is a literal string value, NOT NULL

---

## Analysis Design Fundamentals

### Pre/Post Window
- **Default:** 14 days each side of `first_view_date`
- **Durability variants:** 30-day and 60-day post-windows
- The view date itself is excluded from both windows (pre ends at day −1, post starts at day +1)

### Cohort Eligibility
The `having` clause in the cohort CTE must match the window size:
- 14-day: `first_view_date <= dateadd('day', -14, current_date())`
- 30-day: `first_view_date <= dateadd('day', -30, current_date())`
- 60-day: `first_view_date <= dateadd('day', -60, current_date())`

The **session rate denominator** in `dealer_engagement_raw` must use the same offset:
`datediff('day', fv.first_view_date, dateadd('day', -<window_size>, current_date()))`

### 60-Day Cohort Warning
The product launched May 21, 2026. A 60-day post-window requires `first_view_date ≤ Jun 13, 2026`,
meaning only dealers who viewed CI in the first ~3 weeks after launch qualify. Expect a small cohort.
Always report `total_dealers` prominently and flag thin segments before drawing conclusions.

---

## Source Tables

| Table | Used For |
|---|---|
| `analytics.traffic.dealer_dashboard_events_normalized` | CI view/interaction events |
| `analytics.inventory.inventory_listings_imv` | Listing-level daily price snapshots |
| `analytics.unified_dealer_data_mart.performance_health_metric_comparisons` | Dealer vs competitor performance (leads, VDPs, days on lot) |
| `analytics.unified_dealer_data_mart.auto_entity_performance_metrics` | Merchandising health (photos, price, options) |
| `analytics.competitive_intelligence.performance_health_metric_comparison_monthly` | Dealer metadata (name, account category, size) |

- Performance/merchandising tables filter on `country_code = 'US'` (not region)
- Events table filters on `region = 'NA'`

---

## Tracking Limitations — Critical

We capture **events** (page views, clicks, feature interactions), NOT the KPI values rendered on screen.

**What we CAN segment by:**
- `sd_product` — which tab (Performance, Competitors)
- `sd_product_section` — section within tab
- `sd_feature` — feature interacted with (e.g. QuickActions)
- `sd_element` — specific element (e.g. MerchandisingRecommendation)

**What we CANNOT do:**
- Infer whether a dealer saw a below-benchmark leads/unit, VDPs/unit, or days on lot value
- Know which specific metric was displayed as "red" or "below benchmark" at time of view
- Confirm a dealer read a particular section vs. just loading the page

Do not attempt to filter cohorts by "dealers who saw a gap on metric X."
Use interaction events (clicks, QuickAction usage) as the closest available proxy.

---

## Metric Interpretation Directions

| Metric | Better direction | Notes |
|---|---|---|
| Leads / unit | Higher = better | Sensitive to outliers; use median |
| VDPs / unit | Higher = better | Most consistent signal across segments |
| Days on lot | Lower = better | Positive delta = MORE days = worse |
| % without photos | Lower = better | Negative delta = improvement |
| % without price | Lower = better | Negative delta = improvement |
| % without options | Lower = better | Near-zero in most segments |
| Reprice rate / events | Context-dependent | More repricing = responding to signal |

### Gap metrics
- Gap = dealer metric − competitor metric
- For leads/VDPs: positive gap = dealer ahead
- For days on lot: negative gap = dealer ahead
- Use `nullif(abs(pre_gap), 0)` as denominator for pct lift to handle negative baselines

---

## Known Data Issues

- **Franchise Large Q4 — leads/unit gap avg = 561.8%**: extreme outlier effect. The median
  is the correct figure to cite. Always lead with median for competitive gap metrics.
- **Merchandising cohort**: only ~7 dealers qualify under the QuickAction filter. Any further
  segmentation produces unreliable results. Consider broadening the cohort.
- **Small segments (n < 10)**: discard from conclusions; surface numbers for completeness only.

---

## Engagement Quartile Classification

Session rate = `distinct CI view days since first_view_date / days from first_view_date to current_date - <window_size>`.
Bucketed into Q1–Q4 via `ntile(4) over (order by session_rate)`:
- **Q1** = least frequent users
- **Q4** = most frequent users

Classification is **cohort-relative** — Q4 in the 14-day analysis does not equal Q4 in the 60-day
analysis. Do not compare quartile labels across window sizes.

---

## Tab Segmentation

`first_viewed_tab` = the `sd_product` value of the dealer's very first CI event, captured via
`qualify row_number() over (partition by service_provider_id order by derived_tstamp) = 1`.
Values: `'Performance'` or `'Competitors'`.

Not applicable to `merchandising_health_after_ci_view.sql` — that cohort is already scoped to
the Performance tab.

---

## Below-Benchmark Definition

A dealer qualifies if, in the **pre-window average**, at least one of:
- `avg_leads_per_unit < avg_comp_leads_per_unit * 0.90`
- `avg_vdps_per_unit  < avg_comp_vdps_per_unit  * 0.90`
- `avg_days_on_lot    > avg_comp_days_on_lot    * 1.10`

Pre-window is used intentionally — avoids contaminating the cohort with post-view behavior.

---

## Listing Eligibility (Pricing Analyses)

- Used inventory only (`is_new = false`)
- Open listings only (`listing_status = 'OPEN'`)
- Price > $0
- Must have observations in **both** pre and post windows to count in reprice rate
- Price change threshold: `|delta| > $0.50` to filter rounding noise

---

## Summarization & Communication Rules

### Small cohorts
Any segment with **fewer than 10 dealers** must be flagged explicitly before any finding is stated.
Do not draw directional conclusions — surface the numbers and note "insufficient sample."

### Document structure
**Key takeaways always go first** — before methodology, before tables, before caveats.

### Quantify everything
Never write vague statements like "High engagement dealers show more price changes."
Always include the actual number and the comparison it's relative to:
- ✅ "Q4 Franchise Large dealers show a +9.5 ppt lift in listing reprice rate vs. +3.0 ppts for Q1"
- ✅ "Franchise Small below-benchmark dealers repriced 14.3 ppts more listings post-window"

When comparing two groups, state **both** values — not just the direction or the difference.

### Median vs average
For competitive gap metrics, **always lead with the median**. Note averages only when they diverge
significantly, and explain why. Never cite a gap avg without the median in the same sentence.

---

## Control Group (Matched Cohort) — Future Work

Not implemented. Would require propensity matching on dealer size, type, geographic market,
and pre-period performance. Do not approximate with a simple "never viewed CI" filter —
unmatched comparisons introduce severe selection bias (CI viewers are likely more proactive
by default).

---

## File Inventory

| File | Window | Description |
|---|---|---|
| `price_change_after_ci_view.sql` | 14d | Pricing change — full cohort, by engagement quartile + first tab |
| `price_change_below_benchmark_after_ci_view.sql` | 14d | Pricing change — below-benchmark dealers only |
| `price_change_durability_30d.sql` | 30d | Pricing change — durability test at 30 days |
| `price_change_durability_60d.sql` | 60d | Pricing change — durability test at 60 days (thin cohort) |
| `merchandising_health_after_ci_view.sql` | 14d | Merchandising gaps — QuickAction cohort, by engagement quartile |
| `competitive_gap_after_ci_view.sql` | 14d | Competitive gap — by engagement quartile + first tab |
| `competitive_gap_durability_30d.sql` | 30d | Competitive gap — durability test at 30 days |
| `competitive_gap_durability_60d.sql` | 60d | Competitive gap — durability test at 60 days (thin cohort) |
