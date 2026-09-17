"""
CI Pricing Durability — Presentation Plots
Reads from results/ CSVs and writes PNGs to results/plots/.
Run: python3 generate_plots.py
"""

import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── paths ────────────────────────────────────────────────────────────────────
BASE = os.path.dirname(__file__)
RESULTS = os.path.join(BASE, "results")
OUT = os.path.join(RESULTS, "plots")
os.makedirs(OUT, exist_ok=True)

tabs_14 = pd.read_csv(os.path.join(RESULTS, "ci_tabs_post_action_durability_14d.csv"))
tabs_30 = pd.read_csv(os.path.join(RESULTS, "ci_tabs_post_action_durability_30d.csv"))
qtl_14  = pd.read_csv(os.path.join(RESULTS, "quartiles_post_action_durability_14d.csv"))
qtl_30  = pd.read_csv(os.path.join(RESULTS, "quartiles_post_action_durability_30d.csv"))

# normalise column names
for df in [tabs_14, tabs_30, qtl_14, qtl_30]:
    df.columns = df.columns.str.lower()

# ── colour palette ────────────────────────────────────────────────────────────
PERF_14  = "#1B6EC2"   # blue, solid
PERF_30  = "#6EB5F0"   # blue, light
COMP_14  = "#E06A00"   # orange, solid
COMP_30  = "#F5B97F"   # orange, light

ZERO_LINE = dict(color="#888888", linewidth=0.8, linestyle="--", zorder=0)

def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path}")

def padded_ylim(ax, vals, pct=0.22):
    """Set ylim so labels above/below bars never clip against axis boundaries."""
    lo, hi = min(vals), max(vals)
    span = hi - lo if hi != lo else abs(hi) if hi != 0 else 1
    ax.set_ylim(lo - span * pct, hi + span * pct)

def blabel(ax, bar, val, span, pct=False):
    """Place a value label cleanly above positive bars and below negative ones."""
    suffix = "%" if pct else ""
    txt = f"+{val:.1f}{suffix}" if val >= 0 else f"{val:.1f}{suffix}"
    offset = span * 0.05
    if val >= 0:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + offset,
                txt, ha="center", va="bottom", fontsize=10, fontweight="bold")
    else:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() - offset,
                txt, ha="center", va="top", fontsize=10, fontweight="bold")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 1 — Top-line inventory repricing durability
# Grouped bar: Performance vs Competitors, 14d and 30d
# ─────────────────────────────────────────────────────────────────────────────
def plot_topline_inv():
    perf_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Performance", "median_pct_pt_lift_listings_repriced"].iloc[0]
    perf_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Performance", "median_pct_pt_lift_listings_repriced"].iloc[0]
    comp_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Competitors", "median_pct_pt_lift_listings_repriced"].iloc[0]
    comp_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Competitors", "median_pct_pt_lift_listings_repriced"].iloc[0]

    x = np.array([0, 1])
    w = 0.32
    fig, ax = plt.subplots(figsize=(7, 4.5))

    vals = [perf_14, perf_30, comp_14, comp_30]
    span = max(vals) - min(vals)

    bars_p14 = ax.bar(x[0] - w/2, perf_14, w, color=PERF_14, label="Performance 14d", zorder=3)
    bars_p30 = ax.bar(x[0] + w/2, perf_30, w, color=PERF_30, label="Performance 30d", zorder=3)
    bars_c14 = ax.bar(x[1] - w/2, comp_14, w, color=COMP_14, label="Competitive Landscape 14d", zorder=3)
    bars_c30 = ax.bar(x[1] + w/2, comp_30, w, color=COMP_30, label="Competitive Landscape 30d", zorder=3)

    for bar, val in [(bars_p14[0], perf_14), (bars_p30[0], perf_30),
                     (bars_c14[0], comp_14), (bars_c30[0], comp_30)]:
        blabel(ax, bar, val, span)

    padded_ylim(ax, vals)
    ax.axhline(0, **ZERO_LINE)
    ax.set_xticks(x)
    ax.set_xticklabels(["Performance", "Competitive Landscape"], fontsize=12)
    ax.set_ylabel("Median ppt lift\n(% listings repriced post − pre)", fontsize=10)
    ax.set_title("Inventory Repricing — 14d vs 30d", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.4)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "01_topline_inventory_repricing.png")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 2 — Reprice event burst
# Grouped bar: Events % change at 14d and 30d for each tab
# ─────────────────────────────────────────────────────────────────────────────
def plot_event_burst():
    perf_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Performance", "pct_lift_reprice_events"].iloc[0]
    perf_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Performance", "pct_lift_reprice_events"].iloc[0]
    comp_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Competitors", "pct_lift_reprice_events"].iloc[0]
    comp_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Competitors", "pct_lift_reprice_events"].iloc[0]

    x = np.array([0, 1])
    w = 0.32
    fig, ax = plt.subplots(figsize=(7, 4.5))

    vals = [perf_14, perf_30, comp_14, comp_30]
    span = max(vals) - min(vals)

    for bar_x, val14, val30, c14, c30, label in [
        (x[0], perf_14, perf_30, PERF_14, PERF_30, "Performance"),
        (x[1], comp_14, comp_30, COMP_14, COMP_30, "Competitive Landscape"),
    ]:
        b14 = ax.bar(bar_x - w/2, val14, w, color=c14, label=f"{label} 14d", zorder=3)
        b30 = ax.bar(bar_x + w/2, val30, w, color=c30, label=f"{label} 30d", zorder=3)
        blabel(ax, b14[0], val14, span, pct=True)
        blabel(ax, b30[0], val30, span, pct=True)

    padded_ylim(ax, vals)
    ax.axhline(0, **ZERO_LINE)
    ax.set_xticks(x)
    ax.set_xticklabels(["Performance", "Competitive Landscape"], fontsize=12)
    ax.set_ylabel("Reprice events % change\n(post vs pre window)", fontsize=10)
    ax.set_title("Reprice Event Volume — Burst Pattern", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.4)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "02_reprice_event_burst.png")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 3 — VDP competitive gap keeps improving
# Grouped bar: VDP gap at 14d and 30d for each tab
# ─────────────────────────────────────────────────────────────────────────────
def plot_vdp_gap():
    perf_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Performance", "median_pct_lift_vdps_per_unit_gap"].iloc[0]
    perf_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Performance", "median_pct_lift_vdps_per_unit_gap"].iloc[0]
    comp_14 = tabs_14.loc[tabs_14.first_viewed_tab == "Competitors", "median_pct_lift_vdps_per_unit_gap"].iloc[0]
    comp_30 = tabs_30.loc[tabs_30.first_viewed_tab == "Competitors", "median_pct_lift_vdps_per_unit_gap"].iloc[0]

    x = np.array([0, 1])
    w = 0.32
    fig, ax = plt.subplots(figsize=(7, 4.5))

    vals = [perf_14, perf_30, comp_14, comp_30]
    span = max(vals) - min(vals)

    for bar_x, val14, val30, c14, c30, label in [
        (x[0], perf_14, perf_30, PERF_14, PERF_30, "Performance"),
        (x[1], comp_14, comp_30, COMP_14, COMP_30, "Competitive Landscape"),
    ]:
        b14 = ax.bar(bar_x - w/2, val14, w, color=c14, label=f"{label} 14d", zorder=3)
        b30 = ax.bar(bar_x + w/2, val30, w, color=c30, label=f"{label} 30d", zorder=3)
        blabel(ax, b14[0], val14, span, pct=True)
        blabel(ax, b30[0], val30, span, pct=True)

    padded_ylim(ax, vals)
    ax.axhline(0, **ZERO_LINE)
    ax.set_xticks(x)
    ax.set_xticklabels(["Performance", "Competitive Landscape"], fontsize=12)
    ax.set_ylabel("Median % change in\ndealer − competitor VDPs/unit", fontsize=10)
    ax.set_title("Competitive VDP Gap — Improves from 14d to 30d", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.4)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "03_vdp_competitive_gap.png")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 4 — Engagement tier durability (connected dot plot)
# Q1–Q4 inventory ppt lift at 14d and 30d, connected by lines
# ─────────────────────────────────────────────────────────────────────────────
def plot_quartile_durability():
    qs = ["Q1", "Q2", "Q3", "Q4"]
    inv_14 = [qtl_14.loc[qtl_14.engagement_quartile == q, "median_pct_pt_lift_listings_repriced"].iloc[0] for q in qs]
    inv_30 = [qtl_30.loc[qtl_30.engagement_quartile == q, "median_pct_pt_lift_listings_repriced"].iloc[0] for q in qs]

    x = np.arange(len(qs))
    fig, ax = plt.subplots(figsize=(8, 5))

    # connecting lines
    for i in range(len(qs)):
        ax.plot([x[i], x[i]], [inv_14[i], inv_30[i]],
                color="#cccccc", linewidth=1.5, zorder=1)

    ax.scatter(x, inv_14, color=PERF_14, s=110, zorder=4, label="14-day window")
    ax.scatter(x, inv_30, color=PERF_30, s=110, zorder=4, label="30-day window", marker="D")

    all_vals = inv_14 + inv_30
    span = max(all_vals) - min(all_vals)
    offset = span * 0.06

    # value labels — Q1 is leftmost so both labels go to the right to avoid y-axis clipping
    for i, (v14, v30) in enumerate(zip(inv_14, inv_30)):
        if i == 0:
            ax.text(x[i] + 0.15, v14, f"+{v14:.1f}" if v14 >= 0 else f"{v14:.1f}",
                    ha="left", va="center", fontsize=9, color=PERF_14, fontweight="bold")
            ax.text(x[i] + 0.15, v30, f"+{v30:.1f}" if v30 >= 0 else f"{v30:.1f}",
                    ha="left", va="center", fontsize=9, color="#3a7fc1", fontweight="bold")
        else:
            ax.text(x[i] - 0.15, v14, f"+{v14:.1f}" if v14 >= 0 else f"{v14:.1f}",
                    ha="right", va="center", fontsize=9, color=PERF_14, fontweight="bold")
            ax.text(x[i] + 0.15, v30, f"+{v30:.1f}" if v30 >= 0 else f"{v30:.1f}",
                    ha="left", va="center", fontsize=9, color="#3a7fc1", fontweight="bold")

    padded_ylim(ax, all_vals)
    ax.set_xlim(-0.4, 3.6)
    ax.axhline(0, **ZERO_LINE)
    ax.set_xticks(x)
    ax.set_xticklabels(
        ["Q1\nLeast frequent", "Q2", "Q3", "Q4\nMost frequent"],
        fontsize=11
    )
    ax.set_ylabel("Median ppt lift\n(% listings repriced post − pre)", fontsize=10)
    ax.set_title("Inventory Repricing by Engagement Tier", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.4)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "04_quartile_durability.png")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 5 — Q4 spotlight: three metrics across 14d / 30d
# Small multiples: Inv Ppt, Events %, VDP Gap side by side
# ─────────────────────────────────────────────────────────────────────────────
def plot_q4_spotlight():
    q4_14 = qtl_14[qtl_14.engagement_quartile == "Q4"].iloc[0]
    q4_30 = qtl_30[qtl_30.engagement_quartile == "Q4"].iloc[0]

    metrics = {
        "Inventory\nRepricing (ppt)": (q4_14.median_pct_pt_lift_listings_repriced,
                                        q4_30.median_pct_pt_lift_listings_repriced),
        "Reprice\nEvents (%)":         (q4_14.pct_lift_reprice_events,
                                        q4_30.pct_lift_reprice_events),
        "Competitive\nVDP Gap (%)":    (q4_14.median_pct_lift_vdps_per_unit_gap,
                                        q4_30.median_pct_lift_vdps_per_unit_gap),
    }

    fig, axes = plt.subplots(1, 3, figsize=(10, 4.5))
    fig.suptitle("Q4 — Most Frequent CI Users (n=21/20)", fontsize=13, fontweight="bold", y=1.01)

    w = 0.45
    for ax, (label, (v14, v30)) in zip(axes, metrics.items()):
        vals = [v14, v30]
        span = max(vals) - min(vals) if max(vals) != min(vals) else max(abs(v14), abs(v30), 1)
        colors = [PERF_14, PERF_30]
        bars = ax.bar([0, 1], vals, width=w, color=colors, zorder=3)
        for bar, val in zip(bars, vals):
            blabel(ax, bar, val, span)
        # If all values are positive, start y-axis at 0 to avoid misleading compression
        if min(vals) >= 0:
            ax.set_ylim(0, max(vals) * 1.30)
        else:
            padded_ylim(ax, vals)
        ax.axhline(0, **ZERO_LINE)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["14d", "30d"], fontsize=11)
        ax.set_title(label, fontsize=10, pad=8)
        ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    save(fig, "05_q4_spotlight.png")


# ─────────────────────────────────────────────────────────────────────────────
# PLOT 6 — Engagement tier VDP competitive gap (connected dot plot)
# Companion to plot 4 — same format, different metric
# ─────────────────────────────────────────────────────────────────────────────
def plot_quartile_vdp_gap():
    qs = ["Q1", "Q2", "Q3", "Q4"]
    gap_14 = [qtl_14.loc[qtl_14.engagement_quartile == q, "median_pct_lift_vdps_per_unit_gap"].iloc[0] for q in qs]
    gap_30 = [qtl_30.loc[qtl_30.engagement_quartile == q, "median_pct_lift_vdps_per_unit_gap"].iloc[0] for q in qs]

    x = np.arange(len(qs))
    fig, ax = plt.subplots(figsize=(8, 5))

    # connecting lines
    for i in range(len(qs)):
        ax.plot([x[i], x[i]], [gap_14[i], gap_30[i]],
                color="#cccccc", linewidth=1.5, zorder=1)

    ax.scatter(x, gap_14, color=PERF_14, s=110, zorder=4, label="14-day window")
    ax.scatter(x, gap_30, color=PERF_30, s=110, zorder=4, label="30-day window", marker="D")

    all_vals = gap_14 + gap_30
    span = max(all_vals) - min(all_vals)
    offset = span * 0.06

    # value labels — Q1 leftmost: both go right; others: 14d left, 30d right
    for i, (v14, v30) in enumerate(zip(gap_14, gap_30)):
        if i == 0:
            ax.text(x[i] + 0.15, v14, f"+{v14:.1f}%" if v14 >= 0 else f"{v14:.1f}%",
                    ha="left", va="center", fontsize=9, color=PERF_14, fontweight="bold")
            ax.text(x[i] + 0.15, v30, f"+{v30:.1f}%" if v30 >= 0 else f"{v30:.1f}%",
                    ha="left", va="center", fontsize=9, color="#3a7fc1", fontweight="bold")
        else:
            ax.text(x[i] - 0.15, v14, f"+{v14:.1f}%" if v14 >= 0 else f"{v14:.1f}%",
                    ha="right", va="center", fontsize=9, color=PERF_14, fontweight="bold")
            ax.text(x[i] + 0.15, v30, f"+{v30:.1f}%" if v30 >= 0 else f"{v30:.1f}%",
                    ha="left", va="center", fontsize=9, color="#3a7fc1", fontweight="bold")

    padded_ylim(ax, all_vals)
    ax.set_xlim(-0.4, 3.6)
    ax.axhline(0, **ZERO_LINE)
    ax.set_xticks(x)
    ax.set_xticklabels(
        ["Q1\nLeast frequent", "Q2", "Q3", "Q4\nMost frequent"],
        fontsize=11
    )
    ax.set_ylabel("Median % change in\ndealer − competitor VDPs/unit", fontsize=10)
    ax.set_title("Competitive VDP Gap by Engagement Tier\n(median shown — averages distorted by outliers)", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.4)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "06_quartile_vdp_gap.png")


if __name__ == "__main__":
    print("Generating plots…")
    plot_topline_inv()
    plot_event_burst()
    plot_vdp_gap()
    plot_quartile_durability()
    plot_q4_spotlight()
    plot_quartile_vdp_gap()
    print("Done. PNGs written to results/plots/")
