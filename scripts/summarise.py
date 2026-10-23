#!/usr/bin/env python3
"""Turns results/io.txt and results/jmh.json into the Markdown tables README.md quotes. Medians across every fork and
iteration; maxOutstanding is the largest seen, since a bound is broken by its worst moment, not its typical one."""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent.parent
rows = defaultdict(lambda: defaultdict(list))
for line in (root / "results/io.txt").read_text().splitlines():
    if not line.startswith("RESULT"):
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    key = (kv["scenario"], kv["contestant"])
    for k, v in kv.items():
        if k not in ("scenario", "contestant", "iteration", "fork"):
            rows[key][k].append(float(v))


def med(key, metric):
    values = rows[key].get(metric)
    return statistics.median(values) if values else None


def fmt(x, digits=0):
    return "–" if x is None else f"{x:,.{digits}f}"


out = []
scenarios = []
for s, _ in rows:
    if s not in scenarios:
        scenarios.append(s)
for s in [x for x in scenarios if x != "overload"]:
    out.append(f"\n#### `{s}`\n")
    out.append("| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |")
    out.append("|---|--:|--:|--:|--:|--:|--:|")
    for (s2, c), _ in rows.items():
        if s2 != s:
            continue
        key = (s2, c)
        mo = rows[key].get("maxOutstanding")
        out.append(
            f"| {c} | {fmt(med(key, 'opsPerSec'))} | {fmt(med(key, 'cpuUsPerOp'), 2)} | {fmt(med(key, 'allocBytesPerOp'))} "
            f"| {fmt(max(mo) if mo else None)} | {fmt(med(key, 'liveBytesPerInFlight'))} | {fmt(med(key, 'rssHighWaterMb'))} |"
        )

if "overload" in scenarios:
    out.append("\n#### `overload`: capacity drops from 500 to 100 halfway\n")
    out.append("| contestant | seconds | ops/s | p50 before ms | p99 before ms | p50 after ms | p99 after ms |")
    out.append("|---|--:|--:|--:|--:|--:|--:|")
    for (s2, c), _ in rows.items():
        if s2 == "overload":
            key = (s2, c)
            out.append(
                f"| {c} | {fmt(med(key, 'seconds'), 2)} | {fmt(med(key, 'opsPerSec'))} | {fmt(med(key, 'p50BeforeMs'), 1)} "
                f"| {fmt(med(key, 'p99BeforeMs'), 1)} | {fmt(med(key, 'p50AfterMs'), 1)} | {fmt(med(key, 'p99AfterMs'), 1)} |"
            )

jmh = root / "results/jmh.json"
if jmh.exists():
    out.append("\n#### JMH (ms per million elements, lower is better)\n")
    out.append("| benchmark | ms/op | ± | alloc B/op |")
    out.append("|---|--:|--:|--:|")
    for r in json.loads(jmh.read_text()):
        name = r["benchmark"].replace("bvr.", "")
        pm = r["primaryMetric"]
        alloc = r.get("secondaryMetrics", {}).get("·gc.alloc.rate.norm", {}).get("score")
        out.append(f"| {name} | {pm['score']:.2f} | {pm['scoreError']:.2f} | {fmt(alloc)} |")

sys.stdout.write("\n".join(out) + "\n")
