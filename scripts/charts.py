#!/usr/bin/env python3
"""Draws the README's charts as SVG into docs/charts/ from results/*/summary.json (run scripts/summarise.py first).

Plain SVG, no plotting library. Every chart is a strip per contestant: one small dot per fork (or per sample), a
line over their range, and a larger mark at the median, so the spread is as visible as the middle. Colour is the
library (JDK grey, Surefold blue, Reactor orange, Pekko green); a hollow mark is a blocking contestant, a filled one an
asynchronous or reactive one. Every row is labelled with its name and median, so colour is never needed to read it.
"""
import json
import math
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent.parent
out_dir = root / "docs/charts"
out_dir.mkdir(parents=True, exist_ok=True)

STYLE = """<style>
text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
.bg { fill: #fcfcfb; } .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .mu { fill: #898781; }
.grid { stroke: #e1e0d9; stroke-width: 1; }
.jdk { fill: #6b6a65; stroke: #6b6a65; } .surefold { fill: #2a78d6; stroke: #2a78d6; }
.reactor { fill: #eb6834; stroke: #eb6834; } .pekko { fill: #1baf7a; stroke: #1baf7a; }
.hollow { fill: #fcfcfb; stroke-width: 2; }
@media (prefers-color-scheme: dark) {
  .bg { fill: #1a1a19; } .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .grid { stroke: #2c2c2a; }
  .jdk { fill: #a8a79f; stroke: #a8a79f; } .surefold { fill: #3987e5; stroke: #3987e5; }
  .reactor { fill: #d95926; stroke: #d95926; } .pekko { fill: #199e70; stroke: #199e70; }
  .hollow { fill: #1a1a19; }
}
</style>"""

BLOCKING = {"vt-plain", "vt-ordered", "vt-structured", "surefold-mappar", "surefold-mappar-unordered", "vt-naive"}


def lib(name):
    for prefix, cls in (("surefold", "surefold"), ("reactor", "reactor"), ("pekko", "pekko")):
        if name.startswith(prefix):
            return cls
    return "jdk"


def nice(x):
    if x >= 1e6:
        return f"{x / 1e6:.2f}M"
    if x >= 1e4:
        return f"{x / 1e3:.0f}k"
    if x >= 100:
        return f"{x:,.0f}"
    return f"{x:.2f}" if x < 10 else f"{x:.1f}"


def strip(path, title, subtitle, rows, unit, log=False, panels=None, shared=False):
    """rows: [(label, [values])] for one panel, or panels: [(panel title, rows)] side by side."""
    panels = panels or [(None, rows)]
    label_w, panel_w, row_h, top = 210, 300, 22, 70
    n = max(len(r) for _, r in panels)
    w = label_w + panel_w * len(panels) + 40
    h = top + n * row_h + 40
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" '
           f'aria-label="{title}">', STYLE, f'<rect class="bg" width="{w}" height="{h}"/>',
           f'<text class="t1" x="16" y="24" font-size="15" font-weight="600">{title}</text>',
           f'<text class="t2" x="16" y="44" font-size="12">{subtitle}</text>']
    every = [v for _, prow in panels for _, vs in prow for v in vs if v is not None and v > 0]
    for k, (ptitle, prow) in enumerate(panels):
        vals = every if shared else [v for _, vs in prow for v in vs if v is not None and v > 0]
        if not vals:
            continue
        lo, hi = (min(vals) / 1.3, max(vals) * 1.3) if log else (0, max(vals) * 1.1)
        x0 = label_w + k * panel_w + 10
        def x(v):
            if log:
                return x0 + (math.log(v) - math.log(lo)) / (math.log(hi) - math.log(lo)) * (panel_w - 70)
            return x0 + (v - lo) / (hi - lo) * (panel_w - 70)
        if ptitle:
            svg.append(f'<text class="t1" x="{x0}" y="{top - 8}" font-size="12" font-weight="600">{ptitle}</text>')
        ticks = [10 ** e * m for e in range(int(math.floor(math.log10(lo))), int(math.ceil(math.log10(hi))) + 1) for m in (1, 3)] if log \
            else [hi / 1.1 * f for f in (0, 0.25, 0.5, 0.75, 1.0)]
        for t in ticks:
            if lo <= t <= hi:
                svg.append(f'<line class="grid" x1="{x(t):.1f}" y1="{top}" x2="{x(t):.1f}" y2="{top + n * row_h}"/>')
                svg.append(f'<text class="mu" x="{x(t):.1f}" y="{top + n * row_h + 14}" font-size="10" text-anchor="middle">{nice(t)}</text>')
        for i, (label, vs) in enumerate(prow):
            y = top + i * row_h + row_h / 2
            vs = [v for v in vs if v is not None and v > 0]
            if k == 0:
                svg.append(f'<text class="t1" x="{label_w - 8}" y="{y + 4:.1f}" font-size="12" text-anchor="end">{label}</text>')
            if not vs:
                continue
            c = lib(label)
            med = statistics.median(vs)
            svg.append(f'<line class="{c}" x1="{x(min(vs)):.1f}" y1="{y:.1f}" x2="{x(max(vs)):.1f}" y2="{y:.1f}" stroke-width="2" opacity="0.5"/>')
            for v in vs:
                svg.append(f'<circle class="{c}" cx="{x(v):.1f}" cy="{y:.1f}" r="2" opacity="0.7"/>')
            hollow = " hollow" if label in BLOCKING else ""
            svg.append(f'<circle class="{c}{hollow}" cx="{x(med):.1f}" cy="{y:.1f}" r="5"/>')
            svg.append(f'<text class="t2" x="{x(max(vs)) + 8:.1f}" y="{y + 4:.1f}" font-size="11">{nice(med)}</text>')
    svg.append(f'<text class="mu" x="{w - 16}" y="{h - 8}" font-size="10" text-anchor="end">{unit}. Small dots: forks; '
               f'large: median; hollow: blocking.</text>')
    svg.append("</svg>")
    (out_dir / path).write_text("\n".join(svg) + "\n")
    print(f"wrote docs/charts/{path}")


def load(suite):
    p = root / "results" / suite / "summary.json"
    return json.loads(p.read_text()) if p.exists() else None


def by_median(rows, key):
    return sorted(rows.items(), key=lambda kv: -(statistics.median(kv[1][key]) if kv[1].get(key) else 0))


io = load("io")
if io:
    sc = io["scenarios"]
    for shape, total in (("c10k", "1M"), ("c100k", "10M")):
        devices = [d for d in ("dispatched", "wheel", "shared", "native") if f"{shape}-{d}" in sc]
        order = [c for c, _ in by_median(sc[f"{shape}-dispatched"]["rows"], "opsPerSec")]
        strip(f"{shape}-devices.svg", f"{shape[1:]} calls of 10 ms in flight, on each device",
              f"Throughput, ops/s, log scale (ceiling {total}/s). The same contestants, four ways of completing the I/O.",
              None, "ops/s", log=True, shared=True,
              panels=[(d, [(c, sc[f"{shape}-{d}"]["rows"].get(c, {}).get("opsPerSec") or []) for c in order]) for d in devices])
    rows = sc["c10k-dispatched"]["rows"]
    order = sorted(rows, key=lambda c: statistics.median(rows[c]["cpuUsPerOp"]))
    strip("cpu-per-call.svg", "CPU per call, 10,000 calls of 10 ms in flight", "Process CPU µs per element on the dispatched device: "
          "contestant, device and sampler together.", [(c, rows[c]["cpuUsPerOp"]) for c in order], "CPU µs per element")

wl = load("workload")
if wl:
    sc = wl["scenarios"]
    names = sorted(sc)
    order = [c for c, _ in by_median(sc[names[0]]["rows"], "opsPerSec")]
    strip("workload.svg", "The workload matrix", "Throughput, ops/s, log scale; each panel changes one thing from 1,000 calls of 10 ms.",
          None, "ops/s", log=True, shared=True,
          panels=[(n.replace("-dispatched", "").replace("w-", ""), [(c, sc[n]["rows"].get(c, {}).get("opsPerSec") or []) for c in order]) for n in names])

mem = load("memory")
if mem:
    m = mem["memory"]
    for mode, title in (("pending", "Retained bytes per pending call"), ("hol", "Retained bytes per answered, unconsumed element")):
        if mode in m:
            order = sorted(m[mode], key=lambda c: statistics.median(m[mode][c]["slopes"]))
            strip(f"memory-{mode}.svg", title, "Slope of live heap between 10,000 and 100,000 held; dots are fork × repeat samples.",
                  [(c, m[mode][c]["slopes"]) for c in order], "bytes per element")

ov = load("overload")
if ov:
    o = ov["overload"]
    for p in ("sudden", "open-50ms"):
        if p not in o:
            continue
        order = sorted(o[p])
        e2e = [(c, [o[p][c]["phases"][-1]["e2eP99Ms"]]) for c in order]
        dep = [(c, [o[p][c]["phases"][-1]["depP99Ms"]]) for c in order]
        good = [(c, [o[p][c]["phases"][-1]["goodputPerSec"] or 0.001]) for c in order]
        strip(f"overload-{p}.svg", f"After the dependency loses capacity ({p})",
              "Median across forks, last phase. End-to-end counts from arrival; the dependency's own p99 hides time spent waiting in the caller.",
              None, "ms, or answers/s", log=True,
              panels=[("e2e p99 ms", e2e), ("dependency p99 ms", dep), ("good answers/s", good)])

def timeline(path, title, subtitle, panels, until_ms):
    """panels: [(panel title, t_ms, [(label, css class, values, dashed)])], every series in calls, one shared axis."""
    style = """<style>
.s1 { stroke: #2a78d6; } .s2 { stroke: #eb6834; } .s3 { stroke: #1baf7a; } .ref { stroke: #898781; }
.k1 { fill: #2a78d6; } .k2 { fill: #eb6834; } .k3 { fill: #1baf7a; } .kref { fill: #898781; }
@media (prefers-color-scheme: dark) {
  .s1 { stroke: #3987e5; } .s2 { stroke: #d95926; } .s3 { stroke: #199e70; }
  .k1 { fill: #3987e5; } .k2 { fill: #d95926; } .k3 { fill: #199e70; }
}
</style>"""
    left, right, plot_w, plot_h, top, gap = 56, 200, 560, 170, 108, 50
    w = left + plot_w + right
    h = top + len(panels) * (plot_h + gap) + 10
    hi = max(v for _, _, series in panels for _, _, vs, _ in series for v in vs if v is not None) * 1.08
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" '
           f'aria-label="{title}">', STYLE, style, f'<rect class="bg" width="{w}" height="{h}"/>',
           f'<text class="t1" x="16" y="24" font-size="15" font-weight="600">{title}</text>',
           f'<text class="t2" x="16" y="44" font-size="12">{subtitle}</text>']
    # The legend: every series once, in the order the panels draw them.
    legend, lx = {}, 16
    for _, _, series in panels:
        for label, cls, _, dashed in series:
            legend.setdefault(label, (cls, dashed))
    for label, (cls, dashed) in legend.items():
        dash = ' stroke-dasharray="5 4"' if dashed else ""
        svg.append(f'<line class="{cls}" x1="{lx}" y1="66" x2="{lx + 22}" y2="66" stroke-width="2"{dash}/>')
        svg.append(f'<text class="t2" x="{lx + 28}" y="70" font-size="11">{label}</text>')
        lx += 40 + 6.2 * len(label)
    for k, (ptitle, t_ms, series) in enumerate(panels):
        y0 = top + k * (plot_h + gap)
        def x(t):
            return left + t / until_ms * plot_w
        def y(v):
            return y0 + plot_h - v / hi * plot_h
        svg.append(f'<text class="t1" x="{left}" y="{y0 - 10}" font-size="12" font-weight="600">{ptitle}</text>')
        for v in range(0, int(hi) + 1, 250):
            svg.append(f'<line class="grid" x1="{left}" y1="{y(v):.1f}" x2="{left + plot_w}" y2="{y(v):.1f}"/>')
            svg.append(f'<text class="mu" x="{left - 6}" y="{y(v) + 4:.1f}" font-size="10" text-anchor="end">{v:,}</text>')
        for s in range(0, until_ms // 1000 + 1):
            svg.append(f'<text class="mu" x="{x(s * 1000):.1f}" y="{y0 + plot_h + 14}" font-size="10" text-anchor="middle">{s} s</text>')
        ends = []
        for label, cls, vs, dashed in series:
            pts = [(t, v) for t, v in zip(t_ms, vs) if v is not None and t <= until_ms]
            dash = ' stroke-dasharray="5 4"' if dashed else ""
            line = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in pts)
            svg.append(f'<polyline class="{cls}" fill="none" stroke-width="2" stroke-linejoin="round"{dash} points="{line}">'
                       f'<title>{label}</title></polyline>')
            tail = [v for t, v in pts if t >= until_ms - 900]
            ends.append([y(statistics.median(tail)), label, cls, statistics.median(tail)])
        # Direct labels at the right edge, pushed apart so none overlaps another.
        ends.sort()
        for i in range(1, len(ends)):
            ends[i][0] = max(ends[i][0], ends[i - 1][0] + 13)
        for ly, label, cls, v in ends:
            k_cls = "kref" if cls == "ref" else "k" + cls[1:]
            svg.append(f'<rect class="{k_cls}" x="{left + plot_w + 8}" y="{ly - 4:.1f}" width="8" height="8" rx="2"/>')
            svg.append(f'<text class="t2" x="{left + plot_w + 20}" y="{ly + 4:.1f}" font-size="11">{label} {v:,.0f}</text>')
    svg.append(f'<text class="mu" x="{w - 16}" y="{h - 8}" font-size="10" text-anchor="end">calls, sampled every 10 ms; '
               f'labels give the median of the last 0.9 s</text>')
    svg.append("</svg>")
    (out_dir / path).write_text("\n".join(svg) + "\n")
    print(f"wrote docs/charts/{path}")


def overload_record(pattern, contestant, fork=1):
    p = root / "results/overload/results.jsonl"
    for line in p.read_text().splitlines() if p.exists() else []:
        r = json.loads(line)
        if r.get("kind") == "overload" and r["pattern"] == pattern and r["contestant"] == contestant and r["fork"] == fork:
            return r
    return None


adaptive, fixed = overload_record("sudden", "surefold-adaptive"), overload_record("sudden", "surefold-fixed-500")
if adaptive and fixed:
    def series(r, limit_label):
        s = r["series"]
        waiting = [max(0, b - sv - q) for b, sv, q in zip(s["inBody"], s["serving"], s["depQueue"])]
        limit = s["limit"] if any(v is not None for v in s["limit"]) else [500] * len(s["tMs"])
        return (s["tMs"], [("capacity", "ref", s["capacity"], True), (limit_label, "s1", limit, False),
                           ("queued in the dependency", "s3", s["depQueue"], False),
                           ("waiting in the caller", "s2", waiting, False)])
    ta, sa = series(adaptive, "limit")
    tf, sf = series(fixed, "limit")
    timeline("vegas-sudden.svg", "The adaptive limit after a capacity drop, and where the queue goes",
             "Surefold mapParUnordered, window = the limit's max (1,000 or 500). Capacity 500, then 100 from 3 s; fork 1 of 4.",
             [("Surefold's adaptive Limit (Vegas)", ta, sa), ("A fixed limit of 500", tf, sf)], 6000)

jmh = load("jmh")
if jmh:
    j = jmh["jmh"]
    per = {k.split("[")[1].rstrip("]"): v for k, v in j.items() if k.startswith("PerElementBenchmark")}
    if per:
        order = sorted(per, key=lambda c: per[c]["score"])
        strip("per-element.svg", "Machinery per element, with I/O that is already done",
              "JMH, ns per element through each contestant's own code, bound 64, completed futures. Dots are forks.",
              [(c, per[c]["forks"]) for c in order], "ns per element", log=True)
