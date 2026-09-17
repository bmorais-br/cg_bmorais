# CI Product Business Impact — Pricing Durability (14-Day vs 30-Day)

**Prepared by:** Bruno Morais | **Date:** September 2026 | **Branch:** AN-11916
**Sources:** `ci_tabs_post_action_durability.sql` (tab breakdown) and `quartiles_post_action_durability.sql` (engagement tiers, pooled) — run at window_days = 14 and 30.
**Cohorts:** 14d: 84 dealers (50 Performance, 34 Competitors). 30d: 83 dealers (49 Performance, 34 Competitors).
**Quartile design:** Engagement quartiles are computed across all CI users (both tabs pooled). Tab effects are captured separately via ci_tabs. This gives uniform n=21 per quartile cell vs n=7–14 in a Tab×Quartile split.

---

## KEY TAKEAWAYS

1. **Performance inventory repricing is fully durable — zero decay from 14d to 30d.** Median ppt lift: +5.3 at 14d, +5.3 at 30d. This is the headline finding.

2. **Competitors repricing is front-loaded and largely reverts.** Median ppt lift: +4.4 at 14d, +0.9 at 30d (−3.5 ppt decay). Event volume collapses −40.9 ppt. The initial burst doesn't hold.

3. **Q1 (least frequent users) shows the most durable inventory repricing across the full cohort.** +7.1 ppt at 14d, +6.2 ppt at 30d (Δ −0.9 ppt). Quiet users with no burst pattern make the most persistent adjustments.

4. **Q4 (most frequent users) is second-best for durability.** +6.0 → +4.1 ppt (Δ −1.9 ppt). Event volume declines sharply (−36.4 ppt), but inventory adjustment holds, and the VDP competitive gap swings strongly positive at 30d (−10.0% → +9.5%).

5. **Q3 shows a pricing-vs-VDP divergence that warrants investigation.** Inventory repricing turns negative at 30d (−1.8 ppt, from +3.2 at 14d), yet the VDP competitive gap keeps improving (+25.6% → +38.6%). Do not present this as a pricing story — something else is driving competitive visibility gains.

6. **Q2 is the only tier where both signals worsen.** Inventory: +2.7 → +1.7 ppt. VDP gap: 0.0% → −10.1%. These dealers made a partial pricing response that wasn't sustained, and are falling further behind competitively.

7. **All engagement tiers front-load reprice events — this is universal.** Event volume declines from 14d to 30d across every quartile. The durable signal is inventory share (ppt lift), not event count. Use events only to characterize response speed and intensity, not persistence.

8. **Performance VDPs/unit decline in absolute terms at 30d (−6.3%), but the competitive gap still improves (+4.9%).** Competitors are deteriorating faster than Performance dealers. This is a relative-position win, not an absolute growth story — important framing for the presentation.

---

## DURABILITY — Top Line (by Tab)

> Most reliable rows. Cohorts are near-identical (one Performance dealer difference at 30d). Use these for the main durability argument. Always cite **median** for VDP metrics — see Avg vs Median section.

| First Tab | n 14d / 30d | Median Inv Ppt 14d | Median Inv Ppt 30d | Δ | Events % 14d | Events % 30d | Δ | Median VDP % 14d | Median VDP % 30d | Median VDP Gap 14d | Median VDP Gap 30d | Δ Gap | Days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Performance | 50 / 49 | **+5.3** | **+5.3** | **0.0** | +4.4% | −3.7% | −8.1 ppt | +1.9% | −6.3% | +3.6% | +4.9% | +1.3 ppt | 3 |
| Competitors | 34 / 34 | **+4.4** | **+0.9** | **−3.5** | +27.8% | −13.1% | −40.9 ppt | +9.3% | +1.1% | +6.0% | +8.5% | +2.5 ppt | 2 |

**Median Inv Ppt** = median per-dealer (pct_listings_repriced_post − pct_listings_repriced_pre). **Events %** = aggregate (post − pre) / pre. **Median VDP %** = median % change in dealer VDPs/used unit (absolute). **Median VDP Gap** = median % change in dealer-minus-competitor VDPs/unit; positive = dealer closed the gap. **Δ** = 30d minus 14d value.

> ⚠️ Performance dealers lose absolute VDP ground at 30d (−6.3%), but the competitive gap still improves (+4.9%). Competitors are declining even faster. Frame as a relative-position win.

---

## DURABILITY — By Engagement Quartile (all CI users pooled)

> Quartiles pool both tabs. Tab × Quartile interaction is not captured here — see Key Takeaway #1/#2 for the tab-level story. All cells n≥20; no small-sample warnings apply to inventory or events metrics. VDP gap still requires caution — see Avg vs Median section below.

| Q | n 14d / 30d | Inv Ppt 14d | Inv Ppt 30d | Δ Inv | Events % 14d | Events % 30d | Δ Events | VDP Gap 14d | VDP Gap 30d | Δ VDP Gap | Days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | 21 / 21 | +7.1 | +6.2 | −0.9 | +4.2% | +1.7% | −2.5 ppt | +3.8% | −2.3% | −6.1 ppt | 2 |
| Q2 | 21 / 21 | +2.7 | +1.7 | −1.0 | +16.3% | −2.0% | −18.3 ppt | 0.0% | −10.1% | −10.1 ppt | 2 |
| Q3 | 21 / 21 | +3.2 | −1.8 | −5.0 | +10.4% | −14.3% | −24.7 ppt | +25.6% | +38.6% | +13.0 ppt | 2 |
| Q4 | 21 / 20 * | +6.0 | +4.1 | −1.9 | +24.2% | −12.2% | −36.4 ppt | −10.0% | +9.5% | +19.5 ppt | 2 |

\* One dealer dropped from Q4 at 30d (first_view_date too recent for a complete 30d post-window).

**Cells with reliable VDP gap estimates (Δ Inv and Δ Events):** All cells at n=21/20 are reliable for inventory and events. VDP gap requires caution in Q3 (outlier inflation) and Q4 30d (extreme avg/median divergence). See table below.

---

## AVERAGE vs MEDIAN DIVERGENCE — Do Not Cite Averages for VDP Gap

| Cell | Window | Avg VDP Gap | Median VDP Gap | Risk |
|---|---|---|---|---|
| Q1 | 14d | +14.4% | **+3.8%** | Moderate inflation; use median |
| Q1 | 30d | +16.4% | **−2.3%** | Sign flip — average positive, median negative |
| Q2 | 14d | +30.7% | **0.0%** | Large inflation; average misleadingly positive |
| Q2 | 30d | −22.8% | **−10.1%** | Consistent direction; moderate divergence |
| Q3 | 14d | +67.9% | **+25.6%** | Large inflation from outlier(s); cite median only |
| Q3 | 30d | +64.8% | **+38.6%** | Same outlier persisting; cite median only |
| Q4 | 14d | −44.4% | **−10.0%** | Consistent direction; cite median |
| Q4 | 30d | −157.3% | **+9.5%** | Extreme sign flip — **never cite average** |

---

## CAVEATS

- **Observational, not causal.** Dealers who seek out competitive data may be more proactive by default. No matched control group exists.
- **All quartile cells are now n≥20.** Pooling across tabs eliminates the small-sample problem that affected the Tab×Quartile breakdown. Inventory repricing and events metrics are reliable for all cells.
- **Quartile comparability across windows is valid.** Session rate denominator is `current_date()` (window-agnostic). The 14d and 30d cohorts differ by one dealer only (Q4 at 30d). Δ columns track the same dealer groups across both windows.
- **Tab × Quartile interaction is not captured.** Whether, say, Q4 Competitors behaves differently from Q4 Performance users is not visible in the current quartile output. The tab story is captured at the aggregate level only.
- **Always use median for VDP gap.** Multiple cells show extreme avg/median divergence including sign flips. Q4 30d avg (−157.3%) vs median (+9.5%) is the most severe — never cite the average. See table above.
- **Performance Q3 VDP gap improvement is not a pricing story.** Inventory repricing turns negative at 30d (−1.8 ppt). The +38.6% VDP gap improvement is almost certainly driven by merchandising or market movement, not price changes.
- **Performance dealers show absolute VDP/unit decline at 30d (−6.3%).** The competitive gap still improves because competitors decline faster. Distinguish relative-position gains from absolute performance when presenting.
- **No account_category breakdowns in current results.** Franchise Large vs Small vs Independent segmentation requires adding `current_account_category_simplified` and `dealer_size` back to the `quartiles_post_action_durability.sql` summary grouping.

---

## NEXT STEPS

1. **Add account category segmentation to `quartiles_post_action_durability.sql`.** Uncomment or add `current_account_category_simplified` and `dealer_size` to the summary GROUP BY to get Franchise Large-specific findings. Run at 30d and save a CSV.
2. **Investigate Q3 VDP gap anomaly.** Inventory repricing is negative at 30d but VDP gap keeps improving. Check merchandising health via `merchandising_health_after_ci_view.sql` — these dealers may be adjusting photos/options rather than price.
3. **Watch Q2 closely.** Only segment where both inventory repricing and VDP gap deteriorate from 14d to 30d, with n=21. Worth understanding whether these dealers misread the signal or responded with the wrong adjustment.
4. **Track a 60d window for the early-launch cohort.** Only dealers with first_view_date ≤ Jun 13, 2026 qualify — expect ~20–30 dealers. Even a thin 60d signal on Q1 and Q4 would strengthen the durability narrative.
5. **Consider re-enabling `first_viewed_tab` in the quartile query** if the audience asks about Performance vs Competitors engagement tiers specifically. Current design trades that interaction for better cell sizes (n=21 vs n=7–14).

---

*Branch AN-11916 | CI product launch: May 21, 2026*
