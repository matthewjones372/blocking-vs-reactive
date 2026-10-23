#!/usr/bin/env python3
"""Draws the README's charts from results/ as SVG into docs/charts/. Plain SVG, no plotting library, so anyone can
re-run it after a benchmark run. Colour follows the library (Lark blue, Reactor orange, Pekko aqua, plain virtual
threads yellow) and every mark carries its name and value as text, so colour is never the only way to read one."""
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent.parent
out_dir = root / "docs/charts"
out_dir.mkdir(parents=True, exist_ok=True)

# ---- data ---------------------------------------------------------------------------------------------------------

rows = defaultdict(lambda: defaultdict(list))
for line in (root / "results/io.txt").read_text().splitlines():
    if not line.startswith("RESULT"):
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    for k, v in kv.items():
        if k not in ("scenario", "contestant", "iteration", "fork"):
            try:
                rows[(kv["scenario"], kv["contestant"])][k].append(float(v))
            except ValueError:
                pass


def med(scenario, contestant, metric):
    values = [v for v in rows[(scenario, contestant)].get(metric, []) if not math.isnan(v)]
    return statistics.median(values) if values else None


def most(scenario, contestant, metric):
    values = rows[(scenario, contestant)].get(metric, [])
    return max(values) if values else None


jmh = {r["benchmark"].replace("bvr.", ""): r["primaryMetric"]["score"]
       for r in json.loads((root / "results/jmh.json").read_text())}

# ---- style --------------------------------------------------------------------------------------------------------

STYLE = """<style>
text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
.bg { fill: #fcfcfb; } .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .mu { fill: #898781; }
.grid { stroke: #e1e0d9; stroke-width: 1; } .axis { stroke: #c3c2b7; stroke-width: 1; }
.lark { fill: #2a78d6; } .reactor { fill: #eb6834; } .pekko { fill: #1baf7a; } .vt { fill: #eda100; } .alt { fill: #e87ba4; }
.lark-s { stroke: #2a78d6; } .reactor-s { stroke: #eb6834; } .pekko-s { stroke: #1baf7a; } .vt-s { stroke: #eda100; } .alt-s { stroke: #e87ba4; }
.ring { stroke: #fcfcfb; stroke-width: 2; }
@media (prefers-color-scheme: dark) {
  .bg { fill: #1a1a19; } .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .mu { fill: #898781; }
  .grid { stroke: #2c2c2a; } .axis { stroke: #383835; }
  .lark { fill: #3987e5; } .reactor { fill: #d95926; } .pekko { fill: #199e70; } .vt { fill: #c98500; } .alt { fill: #d55181; }
  .lark-s { stroke: #3987e5; } .reactor-s { stroke: #d95926; } .pekko-s { stroke: #199e70; } .vt-s { stroke: #c98500; } .alt-s { stroke: #d55181; }
  .ring { stroke: #1a1a19; }
}
</style>"""

# Every contestant, its label, and the library its colour follows. One order for every chart, so a row is always
# in the same place.
CONTESTANTS = [
    ("lark-async", "Lark mapAsync", "lark"),
    ("lark-async-unordered", "Lark mapAsyncUnordered", "lark"),
    ("lark", "Lark mapPar", "lark"),
    ("lark-unordered", "Lark mapParUnordered", "lark"),
    ("reactor", "Reactor flatMap", "reactor"),
    ("pekko", "Pekko mapAsyncUnordered", "pekko"),
    ("vt-plain", "Virtual threads + Semaphore", "vt"),
]


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, s, cls="t2", size=12, anchor="start", weight="normal"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}" font-size="{size}" text-anchor="{anchor}" '
            f'font-weight="{weight}">{esc(s)}</text>')


def svg(width, height, body, title, desc):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'role="img" aria-labelledby="t d"><title id="t">{esc(title)}</title><desc id="d">{esc(desc)}</desc>'
            f'{STYLE}<rect class="bg" width="{width}" height="{height}" rx="8"/>{"".join(body)}</svg>\n')


def bar(x0, x1, y, h, cls):
    """A horizontal bar anchored at x0, with a 4px rounded data end at x1."""
    r = min(4.0, max(0.0, x1 - x0), h / 2)
    return (f'<path class="{cls}" d="M{x0:.1f},{y:.1f} H{x1 - r:.1f} Q{x1:.1f},{y:.1f} {x1:.1f},{y + r:.1f} '
            f'V{y + h - r:.1f} Q{x1:.1f},{y + h:.1f} {x1 - r:.1f},{y + h:.1f} H{x0:.1f} Z"/>')


def nice_max(v):
    exp = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 2.5, 5, 10):
        if m * exp >= v:
            return m * exp
    return 10 * exp


def compact(v):
    def trim(n, digits):
        s = f"{n:.{digits}f}"
        return s.rstrip("0").rstrip(".") if "." in s else s
    if v >= 1e6:
        return trim(v / 1e6, 2) + "M"
    if v >= 1e3:
        return trim(v / 1e3, 1 if v < 1e4 else 0) + "k"
    if v >= 10:
        return trim(v, 1)
    return trim(v, 2)


def hbars(x, y, width, title, subtitle, items, fmt=compact, log=False):
    """A panel of horizontal bars, one per (label, value, colour class), value labelled at each bar's end."""
    label_w, row_h, bar_h = 178, 24, 14
    plot_x, plot_w = x + label_w, width - label_w - 64
    body = [text(x, y + 14, title, "t1", 13, weight="600"), text(x, y + 30, subtitle, "mu", 11)]
    top = y + 44
    present = [i for i in items if i[1] is not None]
    if log:
        lo = 10 ** math.floor(math.log10(min(v for _, v, _ in present)))
        hi = 10 ** math.ceil(math.log10(max(v for _, v, _ in present)))
        def pos(v): return plot_x + plot_w * (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo))
        ticks = [10 ** e for e in range(int(math.log10(lo)), int(math.log10(hi)) + 1)]
    else:
        hi = nice_max(max(v for _, v, _ in present))
        def pos(v): return plot_x + plot_w * v / hi
        ticks = [hi * i / 4 for i in range(5)]
    bottom = top + row_h * len(items)
    for t in ticks:
        tx = pos(t)
        body.append(f'<line class="grid" x1="{tx:.1f}" y1="{top - 4}" x2="{tx:.1f}" y2="{bottom}"/>')
        body.append(text(tx, bottom + 14, compact(t) if t else "0", "mu", 10, "middle"))
    body.append(f'<line class="axis" x1="{plot_x}" y1="{top - 4}" x2="{plot_x}" y2="{bottom}"/>')
    for i, (label, value, cls) in enumerate(items):
        ry = top + i * row_h
        body.append(text(plot_x - 8, ry + bar_h - 2, label, "t2", 12, "end"))
        if value is None:
            body.append(text(plot_x + 6, ry + bar_h - 2, "not measured", "mu", 11))
            continue
        if log:
            cx = pos(value)
            body.append(f'<circle class="{cls} ring" cx="{cx:.1f}" cy="{ry + bar_h / 2:.1f}" r="5"/>')
            body.append(text(cx + 10, ry + bar_h - 2, fmt(value), "t1", 11))
        else:
            x1 = pos(value)
            body.append(bar(plot_x, max(plot_x + 1, x1), ry, bar_h, cls))
            body.append(text(x1 + 6, ry + bar_h - 2, fmt(value), "t1", 11))
    return body, bottom + 22 - y


def panels(name, title, desc, grid, width=1080, headline=None):
    """Panels laid out in rows of up to two; each entry is the keyword arguments of hbars."""
    body, y = [], 20
    if headline:
        body.append(text(20, 34, headline, "t1", 16, weight="600"))
        y = 52
    for row in grid:
        heights = []
        col_w = (width - 20 - 20 * len(row)) // len(row)
        for j, p in enumerate(row):
            b, h = hbars(20 + j * (col_w + 20), y, col_w, **p)
            body += b
            heights.append(h)
        y += max(heights) + 18
    (out_dir / name).write_text(svg(width, y, body, title, desc))


def items_for(scenario, metric):
    return [(label, med(scenario, key, metric), cls) for key, label, cls in CONTESTANTS]


# ---- one chart per scenario ---------------------------------------------------------------------------------------

def scenario_chart(scenario, in_flight, ceiling):
    native = f"{scenario}-native"
    panels(
        f"{scenario}.svg",
        f"{in_flight} calls in flight",
        f"Throughput, CPU and memory per contestant with {in_flight} 10 ms calls in flight",
        [
            [dict(title="Throughput, shared device", subtitle=f"ops/s, higher is better (ceiling {ceiling})",
                  items=items_for(scenario, "opsPerSec")),
             dict(title="Throughput, each model's own timer", subtitle="ops/s, higher is better",
                  items=items_for(native, "opsPerSec"))],
            [dict(title="CPU per call, shared device", subtitle="µs of process CPU per call, lower is better",
                  items=items_for(scenario, "cpuUsPerOp"), fmt=lambda v: f"{v:.2f} µs"),
             dict(title="Live heap per call in flight, shared device", subtitle="bytes, lower is better",
                  items=items_for(scenario, "liveBytesPerInFlight"), fmt=lambda v: f"{v:,.0f} B")],
        ],
        headline=f"{in_flight} calls in flight, 10 ms each",
    )


scenario_chart("fanout-1k", "1,000", "100k")
scenario_chart("fanout-10k", "10,000", "1M")
scenario_chart("inflight-100k", "100,000", "10M")

# Backpressure: what each one let pile up, and what it cost.
bp = CONTESTANTS + [("vt-naive", "Virtual threads, no bound", "vt")]
panels(
    "backpressure.svg",
    "Backpressure",
    "The most elements each contestant ever had in flight, and its throughput, with 16 calls allowed",
    [[dict(title="Most elements ever in flight", subtitle="log scale; 16 were allowed",
           items=[(label, most("backpressure", key, "maxOutstanding"), cls) for key, label, cls in bp],
           fmt=lambda v: f"{v:,.0f}", log=True),
      dict(title="Throughput", subtitle="ops/s; 1 ms calls, ceiling 16k",
           items=[(label, med("backpressure", key, "opsPerSec"), cls) for key, label, cls in bp[:-1]])]],
    headline="A fast producer, a slow consumer, at most 16 in flight",
)

# Overload: p99 before and after the dependency loses capacity, and how long the run took.
ov = [("reactor-1000", "Reactor flatMap(…, 1000)", "reactor"),
      ("reactor-tuned", "Reactor flatMap(…, 500)", "reactor"),
      ("lark-1000", "Lark mapPar(Limit.fixed(1000))", "lark"),
      ("lark-adaptive", "Lark mapPar(Limit.adaptive)", "lark")]
panels(
    "overload.svg",
    "A dependency that loses capacity",
    "p99 latency before and after a dependency's capacity drops from 500 to 100, and each run's length",
    [[dict(title="p99 latency before the drop", subtitle="ms, lower is better",
           items=[(label, med("overload", key, "p99BeforeMs"), cls) for key, label, cls in ov],
           fmt=lambda v: f"{v:.1f} ms"),
      dict(title="p99 latency after the drop", subtitle="ms, lower is better",
           items=[(label, med("overload", key, "p99AfterMs"), cls) for key, label, cls in ov],
           fmt=lambda v: f"{v:.1f} ms")],
     [dict(title="Time to finish 200k calls", subtitle="seconds, lower is better",
           items=[(label, med("overload", key, "seconds"), cls) for key, label, cls in ov],
           fmt=lambda v: f"{v:.2f} s")]],
    headline="Capacity drops from 500 to 100 halfway through",
)

# JMH: CPU-only work and hand-offs.
panels(
    "jmh.svg",
    "CPU-only work and hand-offs",
    "Milliseconds per million elements for a CPU-only chain and for a hand-off between threads",
    [[dict(title="Hand-off between two threads", subtitle="ms per million elements, log scale, lower is better",
           items=[("Lark buffer", jmh["HandOffBenchmark.lark"], "lark"),
                  ("Reactor publishOn", jmh["HandOffBenchmark.reactor"], "reactor"),
                  ("Pekko async", jmh["HandOffBenchmark.pekko"], "pekko"),
                  ("ArrayBlockingQueue", jmh["HandOffBenchmark.vtQueue"], "vt")],
           fmt=lambda v: f"{v:,.0f} ms", log=True),
      dict(title="map, filter, map, sum", subtitle="ms per million elements, log scale, lower is better",
           items=[("Lark", jmh["CpuChainBenchmark.lark"], "lark"),
                  ("Reactor", jmh["CpuChainBenchmark.reactor"], "reactor"),
                  ("Pekko", jmh["CpuChainBenchmark.pekko"], "pekko"),
                  ("Kotlin Sequence", jmh["CpuChainBenchmark.sequence"], "vt"),
                  ("for loop", jmh["CpuChainBenchmark.loop"], "vt")],
           fmt=lambda v: f"{v:,.1f} ms", log=True)]],
    headline="Where Reactor still wins: CPU-only work",
)

# Lines of code per contestant, from the region markers.
loc = {}
src = (root / "src/main/kotlin/bvr/IoScenario.kt").read_text()
for name, body in re.findall(r"// region (\S+)\n(.*?)// endregion", src, re.S):
    loc[name] = len([l for l in body.splitlines() if l.strip() and not l.strip().startswith("//")])
panels(
    "code.svg",
    "Lines of code",
    "Non-blank lines each contestant needs for the same bounded fan-out",
    [[dict(title="Lines for the same bounded fan-out", subtitle="non-blank lines, from IoScenario.kt",
           items=[(label, float(loc[key]), cls) for key, label, cls in CONTESTANTS],
           fmt=lambda v: f"{v:.0f}")]],
    width=560,
)

# Throughput as concurrency grows, the five contestants that matter most, shared device.
series = [("lark-async", "Lark mapAsync", "lark"), ("reactor", "Reactor", "reactor"), ("pekko", "Pekko", "pekko"),
          ("lark", "Lark mapPar", "alt"), ("vt-plain", "Virtual threads", "vt")]
levels = [("fanout-1k", 1_000), ("fanout-10k", 10_000), ("inflight-100k", 100_000)]
W, H, L, R, T, B = 760, 400, 70, 170, 64, 46
hi = nice_max(max(med(s, k, "opsPerSec") for k, _, _ in series for s, _ in levels))
body = [text(20, 30, "Throughput as calls in flight grow", "t1", 16, weight="600"),
        text(20, 48, "ops/s on the shared device, higher is better; 10 ms calls", "mu", 11)]
def px(i): return L + (W - L - R) * i / (len(levels) - 1)
def py(v): return H - B - (H - T - B) * v / hi
for i in range(5):
    v = hi * i / 4
    body.append(f'<line class="grid" x1="{L}" y1="{py(v):.1f}" x2="{W - R}" y2="{py(v):.1f}"/>')
    body.append(text(L - 8, py(v) + 4, compact(v) if v else "0", "mu", 10, "end"))
for i, (_, n) in enumerate(levels):
    body.append(text(px(i), H - B + 18, f"{n:,} in flight", "mu", 11, "middle"))
ends = []
for key, label, cls in series:
    pts = [(px(i), py(med(s, key, "opsPerSec"))) for i, (s, _) in enumerate(levels)]
    body.append(f'<polyline class="{cls}-s" fill="none" stroke-width="2" points="'
                + " ".join(f"{x:.1f},{y:.1f}" for x, y in pts) + '"/>')
    for x, y in pts:
        body.append(f'<circle class="{cls} ring" cx="{x:.1f}" cy="{y:.1f}" r="4.5"/>')
    ends.append([pts[-1][1], label, med(levels[-1][0], key, "opsPerSec")])
# Direct labels at the line ends, nudged apart so none overlap.
ends.sort()
for i in range(1, len(ends)):
    ends[i][0] = max(ends[i][0], ends[i - 1][0] + 15)
for y, label, v in ends:
    body.append(text(W - R + 12, y + 4, f"{label}  {compact(v)}", "t2", 11))
(out_dir / "throughput.svg").write_text(
    svg(W, H, body, "Throughput as calls in flight grow",
        "Ops per second at 1k, 10k and 100k calls in flight for Lark mapAsync, Reactor, Pekko, Lark mapPar and "
        "plain virtual threads"))

print("\n".join(sorted(p.name for p in out_dir.glob("*.svg"))))
