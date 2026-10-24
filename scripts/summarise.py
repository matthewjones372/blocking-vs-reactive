#!/usr/bin/env python3
"""Turns results/<suite>/ into results/<suite>/summary.md (tables) and summary.json (the same numbers, for charts).

    scripts/summarise.py io workload memory overload net jmh semantics
    BVR_RESULTS=/some/other/dir scripts/summarise.py smoke

Statistics. A JVM fork is the unit of replication: iterations within one JVM share its JIT decisions, heap layout
and placement on the machine, so they are not independent samples. Each cell's value for a fork is the median of
that fork's measured iterations; across forks the summary gives the median, the range, and a 95% percentile-
bootstrap interval of the median. A comparison is a ratio of medians with its own bootstrap interval (forks of each
contestant resampled independently), and is called resolved only when that interval excludes 1 by more than the
table's noise floor: the larger of 5% and twice the median coefficient of variation across forks in that table.
With five forks a bootstrap interval of a median is coarse; it is reported as it is rather than narrowed by
treating iterations as independent.

The script refuses to mix: every record in a suite must carry the commit, JDK, CPU count and JVM options in its
metadata, or it stops. Invalid runs (wrong answer, timeout, crash) are listed and left out, never averaged in.
"""
import json
import os
import random
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent.parent
results = Path(os.environ.get("BVR_RESULTS", root / "results"))
BOOT = 2000
rng = random.Random(7)


def load(suite):
    d = results / suite
    meta = json.loads((d / "metadata.json").read_text())
    records = [json.loads(l) for l in (d / "results.jsonl").read_text().splitlines() if l.strip()]
    runs = [json.loads(l) for l in (d / "runs.jsonl").read_text().splitlines() if l.strip()] if (d / "runs.jsonl").exists() else []
    check_consistent(suite, meta, records)
    return meta, records, runs


def check_consistent(suite, meta, records):
    problems = []
    for key in ("commit", "jdk", "cpus", "jvmArgs"):
        values = {str(r.get(key)) for r in records}
        if len(values) > 1:
            problems.append(f"{key} differs between records: {sorted(values)}")
    commits = {r.get("commit") for r in records}
    if commits != {meta["commit"]}:
        problems.append(f"records carry commit(s) {commits}, metadata says {meta['commit']}")
    if problems:
        sys.exit(f"{suite}: refusing to summarise mixed results:\n  " + "\n  ".join(problems))


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def boot_median(xs):
    if len(xs) < 3:
        return (None, None)
    meds = sorted(statistics.median(rng.choices(xs, k=len(xs))) for _ in range(BOOT))
    return meds[int(0.025 * BOOT)], meds[int(0.975 * BOOT) - 1]


def boot_ratio(a, b):
    """95% interval of median(a) / median(b), resampling each independently."""
    if len(a) < 3 or len(b) < 3:
        return (None, None)
    rs = sorted(statistics.median(rng.choices(a, k=len(a))) / statistics.median(rng.choices(b, k=len(b))) for _ in range(BOOT))
    return rs[int(0.025 * BOOT)], rs[int(0.975 * BOOT) - 1]


def cv(xs):
    return statistics.stdev(xs) / statistics.mean(xs) if len(xs) >= 2 and statistics.mean(xs) else None


def fmt(x, d=0):
    if x is None or (isinstance(x, float) and x != x):
        return "–"
    return f"{x:,.{d}f}"


def per_fork(records, key, metric):
    """{cell: [value per fork]}: the median of each fork's measured iterations."""
    by = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r.get(metric) is None:
            continue
        by[key(r)][r["round"]].append(r[metric])
    return {cell: [statistics.median(v) for _, v in sorted(forks.items())] for cell, forks in by.items()}


def header(meta):
    return (f"Commit `{meta['commit'][:10]}`{' (dirty tree)' if meta['dirty'] else ''}, Lark `{meta['larkCommit'][:10]}`, "
            f"{meta['java'].splitlines()[0]}, {meta['cpu']}, {meta['cpus']} CPUs, kernel {meta['kernel']}, "
            f"governor {meta['governor']}. JVM: `{meta['jvmOptions']}`. {meta['forks']} forks"
            + (f" × {meta['warmups']} warm-up + {meta['iterations']} measured iterations" if meta['suite'] in ('io', 'workload', 'smoke') or meta['suite'].startswith('net') else "")
            + f"; order shuffled per round (seed {meta['seed']}). {meta['started']} to {meta.get('finished', '(unfinished)')}.\n")


# ---- I/O, workload and net suites ---------------------------------------------------------------------------------

IO_METRICS = [
    ("opsPerSec", "ops/s", 0), ("cpuUsPerOp", "CPU µs/op", 2), ("allocBytesPerOp", "alloc B/op", 0),
    ("pendingMean", "pending", 0), ("bufferedMean", "answered, unconsumed", 0), ("impliedLatencyMs", "implied latency ms", 2),
    ("lagP99Us", "device lag p99 µs", 0), ("gcPauseMs", "GC pause ms", 0), ("jitMs", "JIT ms", 0), ("rssMeanMb", "RSS MB", 0),
]


def io_summary(suite, meta, records, runs, out, js):
    measured = [r for r in records if r.get("kind") in ("io", "net") and r.get("warmup") is False]
    key = lambda r: (r["scenario"], r["contestant"])
    bad = {key(r) for r in measured if r.get("valid") is False}
    failed = [r for r in runs if r.get("exit") not in (0, None)]
    good = [r for r in measured if key(r) not in bad]
    cols = list(IO_METRICS)
    if suite.startswith("net"):
        cols = [c for c in cols if c[0] != "lagP99Us"] + [("serverLagP99Us", "server lag p99 µs", 0), ("serverUtil", "server CPU", 2)]
        for r in good:
            s = json.loads(r["serverStats"]) if r.get("serverStats") else {}
            r["serverLagP99Us"] = s.get("lagP99Us")
            r["serverUtil"] = s.get("cpuUtilisation")
    values = {m: per_fork(good, key, m) for m, _, _ in cols + [("steadyOpsPerSec", "", 0)]}
    # Warm-up check: JIT time inside measured iterations, and how the last measured iteration compares with the first
    # in each fork. Both should be near zero and near 1 if two warm-up iterations were enough.
    jit = [r["jitMs"] for r in good if r.get("jitMs") is not None]
    drift = []
    by_fork = defaultdict(dict)
    for r in good:
        by_fork[(r["scenario"], r["contestant"], r["round"])][r["iteration"]] = r["opsPerSec"]
    for its in by_fork.values():
        if len(its) >= 2:
            drift.append(its[max(its)] / its[min(its)])
    if jit:
        line = (f"Warm-up check: median JIT time per measured iteration {fmt(median(jit))} ms (90th percentile "
                f"{fmt(sorted(jit)[int(0.9 * (len(jit) - 1))])} ms)")
        if drift:
            line += (f"; last measured iteration against first, median {fmt(median(drift), 3)}, 5th–95th percentile "
                     f"{fmt(sorted(drift)[int(0.05 * (len(drift) - 1))], 2)}–{fmt(sorted(drift)[int(0.95 * (len(drift) - 1))], 2)}")
        out.append(line + ".\n")
    calib = [r["nsPerKiloRounds"] for r in records if r.get("kind") == "calibration"]
    out.append(f"Machine drift: the single-threaded calibration loop took {fmt(median(calib))} ns per thousand rounds "
               f"(median of {len(calib)} JVMs), coefficient of variation {fmt((cv(calib) or 0) * 100, 1)}%.\n")
    if bad or failed:
        out.append("**Invalid or failed runs, left out:** " + ", ".join(sorted({f'{s}/{c}' for s, c in bad} |
                   {f"{r['scenario']}/{r['contestant']} (exit {r['exit']})" for r in failed})) + "\n")
    js["scenarios"] = {}
    for s in sorted({r["scenario"] for r in measured}):
        cells = {c: v for (sc, c), v in values["opsPerSec"].items() if sc == s}
        if not cells:
            continue
        cvs = [cv(v) for v in cells.values() if cv(v) is not None]
        noise = max(0.05, 2 * (median(cvs) or 0))
        # The unbounded control is not a contestant: it is never the best, and is shown for what it lets pile up.
        control = {c for c in cells if any(r.get("model") == "unbounded" for r in good if r["contestant"] == c)}
        best = max((c for c in cells if c not in control), key=lambda c: median(cells[c]))
        sample = next(r for r in good if r["scenario"] == s)
        if suite.startswith("net"):
            desc = (f"{sample['items']:,} requests, {sample['concurrency']:,} in flight, service {sample['serviceMicros']} µs, "
                    f"{sample['payload']} B body, `{sample['client']}` clients, `{sample.get('server', 'nio')}` server"
                    + (f", client JVM `{sample['jvmOpts']}`" if sample.get("jvmOpts") else ""))
        else:
            desc = (f"{sample['items']:,} elements, bound {sample['concurrency']:,}, latency {sample['latencyMicros']} µs "
                    f"{sample['dist']}, device `{sample['device']}`"
                    + (f", 1 in {sample['failEvery']} calls fails (recovered)" if sample.get("failEvery") else "")
                    + (f", CPU step {sample['cpuIters']} rounds" if sample.get("cpuIters") else "")
                    + (f", serial consumer {sample['sinkIters']} rounds" if sample.get("sinkIters") else ""))
        nforks = max(len(v) for v in cells.values())
        out.append(f"\n### `{s}`\n\n{desc}. {nforks} forks; noise floor {noise * 100:.0f}%.\n")
        out.append("| contestant | ops/s median [95% CI] | fork range | vs best [95% CI] | " + " | ".join(h for _, h, _ in cols[1:]) + " |")
        out.append("|---|--:|--:|--:|" + "--:|" * (len(cols) - 1))
        js["scenarios"][s] = {"noise": noise, "best": best, "desc": desc, "rows": {}}
        for c in sorted(cells, key=lambda c: -median(cells[c])):
            v = cells[c]
            lo, hi = boot_median(v)
            rlo, rhi = boot_ratio(v, cells[best]) if c != best else (None, None)
            ratio = median(v) / median(cells[best])
            if c in control:
                verdict = "control (unbounded)"
            elif c == best:
                verdict = "best"
            elif rhi is not None and rhi < 1 - noise:
                verdict = f"{ratio:.2f} [{rlo:.2f}–{rhi:.2f}]"
            else:
                verdict = f"tie: {ratio:.2f}" + (f" [{rlo:.2f}–{rhi:.2f}]" if rlo is not None else "")
            row = [c, f"{fmt(median(v))} [{fmt(lo)}–{fmt(hi)}]", f"{fmt(min(v))}–{fmt(max(v))}", verdict]
            for m, _, d in cols[1:]:
                row.append(fmt(median(values[m].get((s, c), [])), d))
            out.append("| " + " | ".join(row) + " |")
            r0 = next(r for r in good if r["scenario"] == s and r["contestant"] == c)
            js["scenarios"][s]["rows"][c] = {m: values[m].get((s, c)) for m, _, _ in cols + [("steadyOpsPerSec", "", 0)]}
            js["scenarios"][s]["rows"][c].update(verdict=verdict, ratio=ratio, ratioCi=[rlo, rhi], ci=[lo, hi],
                                                 model=r0.get("model"), ordered=r0.get("ordered"))
    if suite == "io":
        device_robustness(js, out)


def ranks(values):
    return {c: i + 1 for i, c in enumerate(sorted(values, key=lambda c: -values[c]))}


def spearman(a, b):
    common = [c for c in a if c in b and a[c] is not None and b[c] is not None]
    ra, rb = ranks({c: a[c] for c in common}), ranks({c: b[c] for c in common})
    n = len(common)
    return 1 - 6 * sum((ra[c] - rb[c]) ** 2 for c in common) / (n * (n * n - 1)) if n > 2 else None


def device_robustness(js, out):
    out.append("\n### Does the ranking survive a change of device?\n")
    out.append("Median ops/s for the same contestants on each device; Spearman's ρ compares each device's ranking with "
               "the dispatched device's. Device lag p99 is in each scenario's own table.\n")
    for shape in ("c1k", "c10k", "c100k"):
        devices = [d for d in ("dispatched", "wheel", "shared", "native") if f"{shape}-{d}" in js["scenarios"]]
        if len(devices) < 2 or "dispatched" not in devices:
            continue
        med = {d: {c: median(row["opsPerSec"]) for c, row in js["scenarios"][f"{shape}-{d}"]["rows"].items()} for d in devices}
        out.append(f"\n#### `{shape}`\n")
        out.append("| contestant | " + " | ".join(devices) + " |")
        out.append("|---|" + "--:|" * len(devices))
        for c in sorted(med["dispatched"], key=lambda c: -med["dispatched"][c]):
            out.append(f"| {c} | " + " | ".join(fmt(med[d].get(c)) for d in devices) + " |")
        out.append("| Spearman ρ vs dispatched | " + " | ".join(
            "1" if d == "dispatched" else fmt(spearman(med["dispatched"], med[d]), 2) for d in devices) + " |")
        js.setdefault("robustness", {})[shape] = {d: spearman(med["dispatched"], med[d]) for d in devices}


# ---- memory -------------------------------------------------------------------------------------------------------

def memory_summary(suite, meta, records, runs, out, js):
    mem = [r for r in records if r.get("kind") == "memory" and r.get("repeat", -1) >= 0]
    bad = [r for r in mem if not r.get("valid")]
    failed = [r for r in runs if r.get("exit") not in (0, None)]
    if bad or failed:
        out.append("**Not settled, wrong answer or failed, left out:** " + ", ".join(sorted(
            {f"{r['mode']}/{r['contestant']}" for r in bad} | {f"{r['scenario']}/{r['contestant']} (exit {r['exit']})" for r in failed})) + "\n")
    pairs = defaultdict(dict)
    for r in mem:
        if r.get("valid"):
            pairs[(r["mode"], r["contestant"], r["round"], r["repeat"])][r["concurrency"]] = r
    slopes, at_high, rss, thread_kb, check = (defaultdict(list) for _ in range(5))
    for (mode, c, _, _), by_n in pairs.items():
        if len(by_n) != 2:
            continue
        lo, hi = sorted(by_n)
        slopes[(mode, c)].append((by_n[hi]["retainedBytes"] - by_n[lo]["retainedBytes"]) / (hi - lo))
        at_high[(mode, c)].append(by_n[hi]["retainedBytes"] / hi)
        rss[(mode, c)].append(by_n[hi]["rssDeltaMb"])
        thread_kb[(mode, c)].append((by_n[hi].get("nmtCommittedKb") or {}).get("Thread"))
        check[(mode, c)].append((by_n[hi]["pending"], by_n[hi]["buffered"], by_n[hi]["queued"]))
    classes = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r.get("kind") == "memory-classes":
            for k in r["classes"]:
                # Hidden classes (lambdas) are named per JVM; their address is not part of what they are.
                name = re.sub(r"\$\$Lambda/0x[0-9a-f]+", "$$Lambda", k["class"])
                classes[(r["mode"], r["contestant"])][name].append(k["bytesPerOp"])
    js["memory"] = {}
    for mode, title in (("pending", "Calls pending at the device"), ("hol", "Answered but unconsumed: ordered, head of line held")):
        cells = {c: v for (m, c), v in slopes.items() if m == mode}
        if not cells:
            continue
        out.append(f"\n### {title}\n")
        out.append("Bytes retained per element: the slope of live bytes (class histogram after full GC) between bounds of "
                   "10,000 and 100,000, each fork × repeat one sample. `state at 100k` is pending / answered-unconsumed / "
                   "read-ahead at the moment of measurement.\n")
        out.append("| contestant | B/element median [95% CI] | range | n | state at 100k | live/n at 100k | RSS growth at 100k MB | NMT thread KB | largest growing classes, B/element |")
        out.append("|---|--:|--:|--:|--:|--:|--:|--:|---|")
        for c in sorted(cells, key=lambda c: median(cells[c])):
            v = cells[c]
            lo, hi = boot_median(v)
            top = sorted(((median(b), k) for k, b in classes[(mode, c)].items()), reverse=True)[:4]
            top_s = ", ".join(f"`{k.split('.')[-1]}` {b:.0f}" for b, k in top if b and b >= 4)
            state = "/".join(fmt(median([s[i] for s in check[(mode, c)]])) for i in range(3))
            out.append(f"| {c} | {fmt(median(v))} [{fmt(lo)}–{fmt(hi)}] | {fmt(min(v))}–{fmt(max(v))} | {len(v)} | {state} "
                       f"| {fmt(median(at_high[(mode, c)]))} | {fmt(median(rss[(mode, c)]))} | {fmt(median(thread_kb[(mode, c)]))} | {top_s} |")
            js["memory"].setdefault(mode, {})[c] = {"slopes": v, "ci": [lo, hi]}


# ---- overload -----------------------------------------------------------------------------------------------------

def overload_summary(suite, meta, records, runs, out, js):
    ov = [r for r in records if r.get("kind") == "overload"]
    bad = [r for r in ov if not r.get("valid")]
    failed = [r for r in runs if r.get("exit") not in (0, None)]
    if bad or failed:
        out.append("**Invalid or failed runs, left out:** " + ", ".join(sorted({f"{r['pattern']}/{r['contestant']}" for r in bad} |
                   {f"{r['scenario']}/{r['contestant']} (exit {r['exit']})" for r in failed})) + "\n")
    js["overload"] = {}
    by = defaultdict(list)
    for r in ov:
        if r.get("valid"):
            by[(r["pattern"], r["contestant"])].append(r)
    for p in sorted({r["pattern"] for r in ov}):
        sample = next(r for r in ov if r["pattern"] == p)
        out.append(f"\n### `{p}`\n")
        out.append(("Open loop at " + f"{sample['ratePerSec']:,}/s, deadline {sample['deadlineMs']} ms. " if sample.get("ratePerSec") else "Closed loop. ")
                   + f"*good/s* counts answers within {sample['sloMs']} ms of arrival."
                   + (f" The dependency rejects a call when {sample['queueLimit']} already wait." if sample.get("queueLimit") else "")
                   + " Medians across forks. Latency (ms) counts from each element's arrival; phases are by arrival time;"
                   " *dep* is the time inside the dependency, its queue included.\n")
        phases = [(ph["fromMs"], ph["untilMs"]) for ph in sample["phases"]]
        head = "| contestant | n |" + "".join(f" {a / 1000:g}–{b / 1000:g} s: ok/s · good/s · e2e p50/p99 · dep p99 |" for a, b in phases)
        out.append(head + " expired | rejected | relief / reclaim ms per change |")
        out.append("|---|--:|" + "---|" * len(phases) + "--:|--:|---|")
        js["overload"][p] = {}
        for c in sorted({k for (pp, k) in by if pp == p}):
            rs = by[(p, c)]
            row = f"| {c} | {len(rs)} |"
            cell = {"phases": []}
            for i in range(len(phases)):
                def m(k):
                    return median([r["phases"][i][k] for r in rs])
                row += (f" {fmt(m('throughputPerSec'))} · {fmt(m('goodputPerSec'))} · {fmt(m('e2eP50Ms'), 1)}/{fmt(m('e2eP99Ms'), 1)}"
                        f" · {fmt(m('depP99Ms'), 1)} |")
                cell["phases"].append({k: m(k) for k in ("throughputPerSec", "goodputPerSec", "e2eP50Ms", "e2eP99Ms", "depP99Ms")})
            rec = []
            for j in range(len(rs[0]["recovery"])):
                def show(xs):
                    got = [x for x in xs if x is not None]
                    if not got:
                        return "never"
                    return fmt(median(got)) + (f" ({len(xs) - len(got)}× never)" if len(got) < len(xs) else "")
                rec.append(f"{show([r['recovery'][j]['reliefMs'] for r in rs])} / {show([r['recovery'][j]['reclaimMs'] for r in rs])}")
            row += f" {fmt(median([r['expired'] for r in rs]))} | {fmt(median([r['rejected'] for r in rs]))} | {'; '.join(rec)} |"
            out.append(row)
            cell["series"] = rs[0]["series"]
            js["overload"][p][c] = cell


# ---- JMH ------------------------------------------------------------------------------------------------------------

def jmh_summary(out, js):
    d = results / "jmh"
    meta = json.loads((d / "metadata.json").read_text())
    rows = json.loads((d / "jmh.json").read_text())
    out.append(f"Commit `{meta['commit'][:10]}`{' (dirty tree)' if meta.get('dirty') else ''}, {meta['java']}, {meta['cpu']}, "
               f"{meta['cpus']} CPUs.\n")
    out.append("Score is JMH's mean with its 99.9% error. *Fork means* is each JVM's own mean, the spread between JVMs.\n")
    out.append("| benchmark | unit | score ± error | fork means, min–max | alloc B/op |")
    out.append("|---|---|--:|--:|--:|")
    js["jmh"] = {}
    for r in rows:
        name = r["benchmark"].replace("bvr.", "") + (f" [{','.join(r['params'].values())}]" if r.get("params") else "")
        pm = r["primaryMetric"]
        forks = [statistics.mean(f) for f in pm["rawData"]]
        sm = r.get("secondaryMetrics", {})
        alloc = (sm.get("gc.alloc.rate.norm") or sm.get("·gc.alloc.rate.norm") or {}).get("score")
        out.append(f"| {name} | {pm['scoreUnit']} | {pm['score']:,.2f} ± {pm['scoreError']:,.2f} | {min(forks):,.2f}–{max(forks):,.2f} | {fmt(alloc)} |")
        js["jmh"][name] = {"score": pm["score"], "error": pm["scoreError"], "forks": forks, "alloc": alloc, "unit": pm["scoreUnit"]}


# ---- semantics ------------------------------------------------------------------------------------------------------

def semantics_summary(out, js):
    rows = [json.loads(l) for l in (results / "semantics/semantics.jsonl").read_text().splitlines() if l.strip()]
    by = defaultdict(dict)
    for r in rows:
        by[r["contestant"]][r["check"]] = r
    yn = lambda x: "yes" if x else "no"
    out.append("| contestant | calls started, nothing answering | take(10): returns | take: calls still awaited | "
               "failure surfaces while earlier calls held | failure: calls still awaited | timed-out calls let go | "
               "slow consumer: most read ahead | paused consumer: source reads | caller interrupted: returns | interrupted: run abandoned |")
    out.append("|---|--:|---|--:|---|--:|---|--:|--:|---|---|")
    js["semantics"] = by
    for c, k in by.items():
        if "take" not in k:
            continue
        out.append(f"| {c} | {k['bound']['pending']} of 32 | {yn(k['take']['returned'])} | {k['take']['stillAwaited']} | "
                   f"{yn(k['failure']['returnedWhileHeld'])} | {k['failure']['stillAwaited']} | {yn(k['timeouts']['heldCallsReleased'])} | "
                   f"{k['slowConsumer']['mostAheadOfConsumer']} | {k['demandPause']['readDuringPause']} | "
                   f"{yn(k['interrupt']['callerReturned'])} | {yn(k['interrupt']['stillAwaited'] > 0)} |")


def operational_tables(out):
    d = results / "semantics"
    if (d / "threaddump.jsonl").exists():
        out.append("\n### Thread dump with 100 calls pending\n")
        out.append("| contestant | model | threads in dump | threads at the call site |")
        out.append("|---|---|--:|--:|")
        for r in (json.loads(l) for l in (d / "threaddump.jsonl").read_text().splitlines() if l.strip()):
            out.append(f"| {r['contestant']} | {r['model']} | {r['threads']} | {r['threadsAtCallSite']} |")
    if (d / "stacktraces.jsonl").exists():
        out.append("\n### Stack trace of a failed fetch, as the handler catches it\n")
        out.append("| contestant | exception caught | names `handleRequest` | names `fetchOrder` | lines |")
        out.append("|---|---|---|---|--:|")
        for r in (json.loads(l) for l in (d / "stacktraces.jsonl").read_text().splitlines() if l.strip()):
            yn = lambda x: "yes" if x else "no"
            out.append(f"| {r['contestant']} | `{r['exception']}` | {yn(r['namesHandleRequest'])} | {yn(r['namesFetchOrder'])} | {r['lines']} |")


def main():
    for suite in sys.argv[1:] or ["io"]:
        out = [f"# `{suite}` summary\n", "Generated by `scripts/summarise.py` from the raw results beside this file; "
               "do not edit by hand.\n"]
        js = {}
        if suite == "jmh":
            jmh_summary(out, js)
        elif suite == "semantics":
            semantics_summary(out, js)
            operational_tables(out)
        else:
            meta, records, runs = load(suite)
            out.append(header(meta))
            if suite.startswith("net"):
                out.append(f"Server pinned to CPU {meta['serverCpus']} (`{meta['serverJvmOptions']}`); clients to CPUs {meta['clientCpus']}.\n")
            {"memory": memory_summary, "overload": overload_summary}.get(suite, io_summary)(suite, meta, records, runs, out, js)
        target = results / suite
        (target / "summary.md").write_text("\n".join(out) + "\n")
        (target / "summary.json").write_text(json.dumps(js, indent=1, default=str))
        print(f"wrote {target / 'summary.md'}")


if __name__ == "__main__":
    main()
