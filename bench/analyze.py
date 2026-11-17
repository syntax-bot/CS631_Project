#!/usr/bin/env python3
"""Tables and figures from bench/results.csv (written by bench/bench.sh run).

Usage: python3 bench/analyze.py [results.csv]

Prints one table per experiment (median of the repetitions) and saves them to
bench/figures/summary.md.  With matplotlib installed it also draws
bench/figures/E1.png ... E5.png for the report.
"""
import csv
import os
import statistics
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "results.csv")
FIGURES = os.path.join(HERE, "figures")

# A fixed color and marker per configuration, the same in every figure.
STYLE = {
    "nocache":         ("No cache",           "#898781", "o"),
    "qcache":          ("qcache",             "#2a78d6", "s"),
    "mysql":           ("MySQL-style",        "#eb6834", "^"),
    "partitions_only": ("Partitions only",    "#1baf7a", "D"),
    "versions_only":   ("Versions only",      "#eda100", "v"),
    "qcache_admit":    ("qcache + admission", "#e87ba4", "X"),
}
INK, INK2, GRID, SURFACE = "#0b0b0b", "#55544f", "#e4e3df", "#fcfcfb"

# experiment: (x column, y column, title, x label, y label, number format)
EXPERIMENTS = {
    "E1": ("query", "latency_ms", "E1. Latency of one query (1 client, read only)",
           "query", "ms per query", "{:.3f}"),
    "E2": ("write_pct", "tps", "E2. Writes to the tables the queries read",
           "writes %", "transactions/s", "{:,.0f}"),
    "E3": ("write_pct", "tps", "E3. Writes to another table: the cost of invalidating",
           "writes %", "transactions/s", "{:,.0f}"),
    "E4": ("clients", "tps", "E4. Throughput as clients are added (5% writes elsewhere)",
           "clients", "transactions/s", "{:,.0f}"),
    "E5": ("entries", "commit_ms", "E5. Cost of one committing write",
           "cached results", "ms per commit", "{:.3f}"),
}


def collect(rows, exp):
    """{config: {x: (median, min, max)}} for one experiment."""
    xcol, ycol = EXPERIMENTS[exp][:2]
    values = defaultdict(list)
    for r in rows:
        if r["experiment"] == exp and r[ycol]:
            x = r[xcol] if xcol == "query" else float(r[xcol])
            values[(r["config"], x)].append(float(r[ycol]))
    data = defaultdict(dict)
    for (cfg, x), ys in values.items():
        data[cfg][x] = (statistics.median(ys), min(ys), max(ys))
    return {c: data[c] for c in STYLE if c in data}


def as_table(exp, data):
    xcol, _, title, xlabel, ylabel, fmt = EXPERIMENTS[exp]
    xs = sorted({x for points in data.values() for x in points})
    lines = [f"### {title}", "", f"| {xlabel} | " + " | ".join(STYLE[c][0] for c in data) + " |",
             "|---" * (len(data) + 1) + "|"]
    for x in xs:
        name = x if isinstance(x, str) else f"{x:g}"
        cells = [fmt.format(data[c][x][0]) if x in data[c] else "" for c in data]
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    return "\n".join(lines + ["", f"({ylabel}, median of the repetitions)", ""])


def hit_rates(rows, exp):
    """Share of lookups that were hits, per configuration and write share."""
    count = defaultdict(lambda: [0, 0])
    for r in rows:
        if r["experiment"] == exp and r["hits"]:
            count[(r["config"], float(r["write_pct"]))][0] += int(r["hits"])
            count[(r["config"], float(r["write_pct"]))][1] += int(r["hits"]) + int(r["misses"])
    cfgs = [c for c in STYLE if any(k[0] == c for k in count) and c != "nocache"]
    xs = sorted({k[1] for k in count})
    lines = [f"### {exp}. Hit rate", "", "| writes % | " + " | ".join(STYLE[c][0] for c in cfgs) + " |",
             "|---" * (len(cfgs) + 1) + "|"]
    for x in xs:
        cells = [f"{100 * h / t:.0f}%" if t else "" for h, t in (count[(c, x)] for c in cfgs)]
        lines.append(f"| {x:g} | " + " | ".join(cells) + " |")
    return "\n".join(lines + [""])


def draw(exp, data, plt, ticker):
    xcol, _, title, xlabel, ylabel, _ = EXPERIMENTS[exp]
    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=150)
    all_x = sorted({x for points in data.values() for x in points})
    for i, (cfg, points) in enumerate(data.items()):
        label, color, marker = STYLE[cfg]
        xs = sorted(points)
        med = [points[x][0] for x in xs]
        err = [[points[x][0] - points[x][1] for x in xs], [points[x][2] - points[x][0] for x in xs]]
        if exp == "E1":
            # dot plot: one row per query, configurations side by side
            pos = [all_x.index(x) + (i - (len(data) - 1) / 2) * 0.22 for x in xs]
            ax.errorbar(med, pos, xerr=err, color=color, marker=marker, markersize=7, linewidth=0,
                        elinewidth=1, capsize=3, label=label, markeredgecolor=SURFACE)
        else:
            # E2, E3: the write shares at even steps, so 0 and 1 % do not overlap
            pos = [all_x.index(x) for x in xs] if exp in ("E2", "E3") else xs
            ax.errorbar(pos, med, yerr=err, color=color, marker=marker, markersize=6, linewidth=2,
                        elinewidth=1, capsize=3, label=label, markeredgecolor=SURFACE)
    plain = ticker.FuncFormatter(lambda v, _: f"{v:,.0f}" if abs(v) >= 100 else f"{v:g}")
    if exp == "E1":
        ax.set_yticks(range(len(all_x)), all_x)
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(plain)
        ax.set_xlabel(ylabel + " (log scale)")
        ax.grid(axis="y", visible=False)
    else:
        if exp in ("E2", "E3"):
            ax.set_xticks(range(len(all_x)), [f"{x:g}" for x in all_x])
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(ticker.LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
            ax.yaxis.set_minor_formatter(ticker.NullFormatter())
            ylabel += " (log scale)"
        if exp == "E4":
            ax.set_xscale("log", base=2)
            ax.set_xticks(all_x)
            ax.xaxis.set_major_formatter(plain)
        ax.yaxis.set_major_formatter(plain)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
    ax.set_title(title, color=INK)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=9)
    fig.text(0.01, 0.005, "median of the repetitions; whiskers: min to max", fontsize=8, color=INK2)
    fig.tight_layout()
    fig.savefig(os.path.join(FIGURES, exp + ".png"), bbox_inches="tight")
    plt.close(fig)


def main():
    with open(RESULTS, newline="") as f:
        rows = list(csv.DictReader(f))
    os.makedirs(FIGURES, exist_ok=True)
    found = {exp: collect(rows, exp) for exp in EXPERIMENTS}
    found = {exp: data for exp, data in found.items() if data}
    text = "\n".join(as_table(exp, data) for exp, data in found.items())
    for exp in ("E2", "E3"):
        if exp in found:
            text += "\n" + hit_rates(rows, exp)
    print(text)
    with open(os.path.join(FIGURES, "summary.md"), "w") as f:
        f.write(text)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib import ticker
    except ImportError:
        print("(install matplotlib for the figures)")
        return
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "axes.edgecolor": GRID,
        "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2, "font.size": 10,
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
        "grid.color": GRID, "axes.axisbelow": True, "axes.titlesize": 12,
        "axes.titleweight": "bold", "axes.titlelocation": "left", "legend.frameon": False,
    })
    for exp, data in found.items():
        draw(exp, data, plt, ticker)
    print(f"figures in {FIGURES}")


if __name__ == "__main__":
    main()
