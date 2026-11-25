# Outlier analysis: dealers with sharp drops in leads / connections per unit

Goal: flag "lower outlier" dealers (month-over-month drop in leads or connections per unit of used inventory)
and check whether the flagged dealers also show churn/downgrade, an inventory blackout, or a deal-rating shift.
Results go to a Jira ticket read by non-technical stakeholders AND engineers, so write takeaways for both
(a short plain-language summary, then definitions and technical notes).

## Files (run in this order, in a Snowflake worksheet)

| File | Creates / does |
|---|---|
| `01_outlier_base_mom_tukey.sql` | `sandbox_bmorais.outlier_analysis.outlier_base`: MoM % change of 4 per-unit metrics, flagged with a Tukey lower fence (Q1 - 1.5 IQR) AND below p05, per metric x month x size bucket x account category |
| `01b_outlier_base_percentile_method.sql` | `sandbox_bmorais.outlier_analysis.outlier_base_percentile_rank_method`: percentile rank of leads/connections per unit within month x size bucket x account category; change = rank this month - rank last month (in positions). Flag = change below the lower fence |
| `02_dealer_month_signals.sql` | `sandbox_bmorais.outlier_analysis.dealer_month_signals`: one row per dealer x month with churn/downgrade, inventory blackout and deal-mix flags |
| `03_followup_queries.sql` | Summary by peer group (A) and list of flagged dealers with no churn/downgrade/blackout (B). Month range via `set start_month` / `set end_month` |

The percentile method is the one used for the shared results. Its fence is Tukey in the file, but the shared results
used a hardcoded fence of -20 positions (see the commented alternative in the stats CTE of 01b).

## Environment and workflow

- The Snowflake MCP in Claude Code is READ-ONLY: `create` statements are rejected. Test logic with plain
  SELECT/CTE versions, then give the user the SQL to run. The user exports result CSVs to `~/Downloads`
  (names like `dealer_month_signals_<date>-<time>_leads.csv`), then asks for an interpretation.
- Sources: `analytics.unified_dealer_data_mart.performance_health_metric_monthly_changes` (per-unit rates),
  `analytics.unified_dealer_data_mart.performance_health_metric_comparison_monthly` (inventory, deal ratings, sf_account_id),
  `analytics.mrr.daily_paying_subscriptions` (tiers). dbt models live in `dbt-central/models/` (mrr/, unified_dealer_data_mart/).
- Window: reference months 2026-05-01 to 2026-10-01 (US, region NA). Earlier months are pulled only for prior-month
  comparisons. November 2026 is a partial month and is excluded.
- Peer groups with fewer than 30 dealers are dropped.

## Conventions (important)

- Every column or alias ending in `_pct` is a percent (5 = 5%), never a fraction. Differences between percentages are `_pp`.
  A past bug came from `avg_inventory_mom_pct` being a fraction while the blackout test assumed percent
  (non-outlier blackout rate came out at 44% instead of about 2-3%). Sanity check after rebuilding signals:
  non-outlier `inventory_blackout_pct` should be about 2-3%.
- Group labels in outputs: `🔴 lower outlier` and `🟢 non-outlier`.
- The group the flag is defined in is month x size bucket x account category; compare flagged vs. not flagged inside it.
  Do not add rows across different cuts, they cover the same dealers.

## Definitions

- Size bucket: avg daily used inventory `< 50`, `50-150`, `> 150`.
- Tier ladder (last day of month): Featured Priority > Featured > Enhanced > Standard > non-paying. Add-ons rank as non-paying.
  Churn = had a tier at end of prior month, none at end of this month. Downgrade = lower tier. `any_tier_drop` = either.
- Inventory blackout: avg daily inventory down 50%+ vs. prior month, or 7+ days in the month with no inventory.
- Deal-mix worsened: high-price + overpriced share up 15pp+, OR great + good share down 15pp+ (was 5pp in early runs).
- Percentile rank: `percent_rank() * 100` (0 = lowest in peer group; ties share the same rank).

## Results so far (October 2026 vs. September 2026, -20 position fence)

- Flagged: 2,025 of 32,106 dealers on leads per unit (6.3%); 1,417 on connections per unit (4.4%).
- Flagged vs. not flagged, any churn/downgrade/blackout: 13.4% vs 2.7% (leads), 16.2% vs 2.8% (connections).
  Deal mix worsened: 16.2% vs 4.4% (leads), 17.9% vs 4.6% (connections). About 4-6x more likely.
- These are a MINORITY of flagged dealers: even adding both signals (ceiling, assuming no overlap) it is at most about 30-34%.
  Do not claim "most" flagged dealers have churn/blackout/pricing issues. Describe it as "much more likely than other dealers".
- Share of unrated inventory is about the same for flagged and not flagged dealers (about 24%).
- The earlier MoM-Tukey results (VDP views per unit) looked different (about two thirds with churn/blackout) because it is a different method
  and population; do not mix the two sets of numbers.

## Known caveats

- Blind spot of the percentile method: a dealer ranked below p20 last month cannot drop 20 positions, so it can never be flagged
  (about 19% of dealer-months), even if its leads go to zero.
- A dealer that changes size bucket between months is compared against a different peer group each month.
- Many dealers tie at rank 0 (leads or connections per unit exactly 0).
- `02_dealer_month_signals.sql` builds its dealer list from `outlier_base`; dealer-months only in the percentile table
  (14 over May-Oct at the last check) get no signals.
- Per-unit = total / average daily inventory, and the average only covers days with inventory, so a mid-month inventory loss
  can lower the rate mechanically.
- Associations, not causes. One month only.

## Working with this repo and user

- The GitHub repo is PUBLIC. The user confirmed publishing these SQL files is fine, but confirm before pushing anything new,
  and never push CSV exports or data. Work on a branch (not `main`); commit messages end with the Claude co-author trailer.
- Keep edits minimal: change what was asked, and ask before extra clean-ups.
- Unrelated work may be uncommitted in the main clone (`~/cg_bmorais`, branch `AN-11915`); use a separate worktree instead of touching it.
- Cross-check numbers computed from CSVs against the SQL when possible, and say what was not re-run (the MCP cannot create tables).
