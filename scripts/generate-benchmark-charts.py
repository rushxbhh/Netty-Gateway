#!/usr/bin/env python3
"""
Phase 4 Benchmark Chart Generator — ULL-FLIG
Reads latency_benchmark_results.json and produces:
  - latency_percentiles.png  : p50/p95/p99 bar chart per suite
  - throughput_comparison.png: throughput bar chart per suite
  - e2e_cdf.png              : cumulative distribution (ingress vs E2E)
  - ingress_vs_db.png        : stacked bar chart ingress + db commit latency

Usage:
    python scripts/generate-benchmark-charts.py [--json latency_benchmark_results.json]
"""
import json
import sys
import os
import argparse

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    print("ERROR: matplotlib and numpy are required.")
    print("  Install: pip install matplotlib numpy")
    sys.exit(1)

COLORS = {
    "ingress": "#2563EB",   # blue
    "db":      "#16A34A",   # green
    "e2e":     "#DC2626",   # red
    "bar1":    "#3B82F6",
    "bar2":    "#10B981",
    "bar3":    "#F59E0B",
    "bar4":    "#EF4444",
}

def load_results(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def suite_labels(suites):
    return [s["suite"] for s in suites]

# ── Chart 1: Latency Percentiles grouped bar ──────────────────────────────────
def chart_latency_percentiles(suites, out_dir: str):
    labels = suite_labels(suites)
    n = len(labels)
    x = np.arange(n)
    width = 0.25

    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=False)
    fig.suptitle("ULL-FLIG — Latency Percentiles by Suite", fontsize=14, fontweight="bold")

    sections = [
        ("ingress", "Ingress Latency (ms)", "Gateway ingress only"),
        ("db_audit", "DB Audit Commit Latency (ms)", "SPSC + C++ + DB commit"),
        ("e2e", "End-to-End Latency (ms)", "Total client-observed"),
    ]

    for ax, (key, title, subtitle) in zip(axes, sections):
        p50s = [s[key]["p50"] for s in suites]
        p95s = [s[key]["p95"] for s in suites]
        p99s = [s[key]["p99"] for s in suites]

        ax.bar(x - width, p50s, width, label="p50", color="#3B82F6")
        ax.bar(x,         p95s, width, label="p95", color="#F59E0B")
        ax.bar(x + width, p99s, width, label="p99", color="#EF4444")
        ax.set_title(f"{title}\n({subtitle})", fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=8)
        ax.set_ylabel("Latency (ms)")
        ax.legend(fontsize=8)
        ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    out = os.path.join(out_dir, "latency_percentiles.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")

# ── Chart 2: Throughput comparison ───────────────────────────────────────────
def chart_throughput(suites, out_dir: str):
    labels = suite_labels(suites)
    throughputs = [s["throughput"] for s in suites]
    concs = [s["concurrency"] for s in suites]
    colors = [COLORS["bar1"], COLORS["bar2"], COLORS["bar3"], COLORS["bar4"]][:len(labels)]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, throughputs, color=colors, edgecolor="white", linewidth=0.5)
    ax.set_title("ULL-FLIG — Throughput per Load Suite (tx/sec)", fontsize=13, fontweight="bold")
    ax.set_ylabel("Throughput (transactions/second)")
    ax.set_xlabel("Benchmark Suite")
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    for bar, tp, c in zip(bars, throughputs, concs):
        ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + 0.05,
                f"{tp:.2f} tx/s\n(concurrency={c})", ha="center", va="bottom", fontsize=8)

    ax.set_ylim(0, max(throughputs) * 1.3)
    plt.tight_layout()
    out = os.path.join(out_dir, "throughput_comparison.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")

# ── Chart 3: Stacked ingress vs DB-commit latency ────────────────────────────
def chart_ingress_vs_db(suites, out_dir: str):
    labels = suite_labels(suites)
    n = len(labels)
    x = np.arange(n)
    width = 0.35

    percentiles = ["p50", "p95", "p99"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=False)
    fig.suptitle("ULL-FLIG — Ingress vs DB Commit Breakdown (ms)", fontsize=13, fontweight="bold")

    for ax, pct in zip(axes, percentiles):
        ingress_vals = [s["ingress"][pct] for s in suites]
        db_vals      = [s["db_audit"][pct] for s in suites]

        ax.bar(x, ingress_vals, width, label="Gateway Ingress", color=COLORS["ingress"])
        ax.bar(x, db_vals, width, bottom=ingress_vals, label="DB Audit Commit", color=COLORS["db"])
        ax.set_title(f"{pct.upper()} Latency Breakdown", fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=8)
        ax.set_ylabel("Latency (ms)")
        ax.legend(fontsize=8)
        ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    out = os.path.join(out_dir, "ingress_vs_db.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")

# ── Chart 4: E2E latency scaling (concurrency vs p50/p99) ───────────────────
def chart_concurrency_scaling(suites, out_dir: str):
    concs = [s["concurrency"] for s in suites]
    e2e_p50 = [s["e2e"]["p50"] for s in suites]
    e2e_p99 = [s["e2e"]["p99"] for s in suites]
    db_p50  = [s["db_audit"]["p50"] for s in suites]
    db_p99  = [s["db_audit"]["p99"] for s in suites]
    labels  = suite_labels(suites)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("ULL-FLIG — Latency vs Concurrency", fontsize=13, fontweight="bold")

    # E2E
    ax1.plot(labels, e2e_p50, "o-", color=COLORS["bar1"], label="E2E p50", linewidth=2, markersize=7)
    ax1.plot(labels, e2e_p99, "s--", color=COLORS["bar4"], label="E2E p99", linewidth=2, markersize=7)
    ax1.set_title("End-to-End Latency Scaling")
    ax1.set_xlabel("Benchmark Suite")
    ax1.set_ylabel("Latency (ms)")
    ax1.legend()
    ax1.grid(linestyle="--", alpha=0.5)
    ax1.tick_params(axis="x", rotation=15)

    # DB commit
    ax2.plot(labels, db_p50, "o-", color=COLORS["bar2"], label="DB Audit p50", linewidth=2, markersize=7)
    ax2.plot(labels, db_p99, "s--", color=COLORS["bar3"], label="DB Audit p99", linewidth=2, markersize=7)
    ax2.set_title("DB Audit Commit Latency Scaling")
    ax2.set_xlabel("Benchmark Suite")
    ax2.set_ylabel("Latency (ms)")
    ax2.legend()
    ax2.grid(linestyle="--", alpha=0.5)
    ax2.tick_params(axis="x", rotation=15)

    plt.tight_layout()
    out = os.path.join(out_dir, "concurrency_scaling.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="latency_benchmark_results.json")
    parser.add_argument("--out", default="benchmark-charts")
    args = parser.parse_args()

    if not os.path.exists(args.json):
        print(f"ERROR: JSON file not found: {args.json}")
        sys.exit(1)

    os.makedirs(args.out, exist_ok=True)
    data = load_results(args.json)
    suites = data["suites"]
    env = data.get("environment", {})

    print(f"\nULL-FLIG Phase 4 — Benchmark Chart Generator")
    print(f"  Input : {args.json}  ({len(suites)} suites)")
    print(f"  Output: {args.out}/")
    print(f"  Env   : {env.get('os', '?')} | JVM {env.get('jvm', '?')} | {env.get('processors', '?')} cores")
    print(f"  DB    : {env.get('database', '?')}")
    print(f"  C++ Engine: {env.get('sequencer_engine', '?')}\n")

    chart_latency_percentiles(suites, args.out)
    chart_throughput(suites, args.out)
    chart_ingress_vs_db(suites, args.out)
    chart_concurrency_scaling(suites, args.out)

    print("\nAll charts generated successfully.")

if __name__ == "__main__":
    main()
