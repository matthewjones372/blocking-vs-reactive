#!/usr/bin/env python3
"""Runs one suite: every cell (scenario x contestant) once per round, in a fresh JVM, in an order shuffled afresh
each round, for --forks rounds. Each round holds every cell once, so a drift in the machine (another tenant, heat,
a slow minute) lands on every contestant alike rather than on whichever ran last.

Writes into results/<suite>/ (or --out):
  results.jsonl   every RESULT line, with the run it came from
  runs.jsonl      one line per JVM: cell, round, order, start, end, exit code, load average, stderr tail
  metadata.json   the build, Lark, JDK, machine, JVM options, seed and schedule

    scripts/run-suite.py io --forks 5
    scripts/run-suite.py smoke --forks 1 --warmups 1 --iterations 1

Nothing else should run on the machine meanwhile; the script stops Gradle's daemons before it starts measuring.
"""
import argparse
import json
import os
import platform
import random
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

root = Path(__file__).resolve().parent.parent
binary = root / "build/install/blocking-vs-reactive/bin/blocking-vs-reactive"

# Heap fixed and touched up front, so no run pays for growing it and RSS starts from the same place.
JVM = {
    "io": "-Xms4g -Xmx4g -XX:+UseG1GC -XX:+AlwaysPreTouch",
    "workload": "-Xms4g -Xmx4g -XX:+UseG1GC -XX:+AlwaysPreTouch",
    "smoke": "-Xms1g -Xmx1g -XX:+UseG1GC",
    "memory": "-Xms4g -Xmx4g -XX:+UseG1GC -XX:NativeMemoryTracking=summary",
    "overload": "-Xms2g -Xmx2g -XX:+UseG1GC -XX:+AlwaysPreTouch",
    "net": "-Xms2g -Xmx2g -XX:+UseG1GC -XX:+AlwaysPreTouch",
    "net-smoke": "-Xms1g -Xmx1g -XX:+UseG1GC",
}
SERVER_JVM = "-Xms1g -Xmx1g -XX:+UseG1GC -XX:+AlwaysPreTouch"


def sh(*cmd, cwd=root):
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True).stdout.strip()


def metadata(args, cells, schedule):
    lark_commit = (root / "lark.commit").read_text().strip()
    cpu = next((l.split(":", 1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                if l.startswith("model name")), platform.processor()) if Path("/proc/cpuinfo").exists() else platform.processor()
    governor = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    java = Path(os.environ.get("JAVA_HOME", "")) / "bin/java"
    return {
        "suite": args.suite,
        "commit": sh("git", "rev-parse", "HEAD"),
        "dirty": bool(sh("git", "status", "--porcelain", "--untracked-files=no")),
        "larkCommit": lark_commit,
        "larkVersion": "0.0.0-cand-SNAPSHOT",
        "java": "\n".join(l for l in subprocess.run([str(java) if java.exists() else "java", "-version"], capture_output=True,
                                                    text=True).stderr.splitlines() if not l.startswith("Picked up")),
        "cpu": cpu,
        "cpus": os.cpu_count(),
        "memoryKb": next((int(l.split()[1]) for l in Path("/proc/meminfo").read_text().splitlines() if l.startswith("MemTotal")), None)
        if Path("/proc/meminfo").exists() else None,
        "kernel": platform.release(),
        "governor": governor.read_text().strip() if governor.exists() else None,
        "host": socket.gethostname(),
        "jvmOptions": JVM[args.suite],
        "serverJvmOptions": SERVER_JVM if args.suite.startswith("net") else None,
        "serverCpus": args.server_cpus if args.suite.startswith("net") else None,
        "clientCpus": args.client_cpus if args.suite.startswith("net") else None,
        "forks": args.forks, "warmups": args.warmups, "iterations": args.iterations, "seed": args.seed,
        "cells": len(cells),
        "schedule": schedule,
        "started": datetime.now(timezone.utc).isoformat(),
    }


def command(suite, scenario, contestant, args):
    if suite in ("io", "workload", "smoke"):
        return ["io", scenario, contestant, str(args.warmups), str(args.iterations)]
    if suite == "memory":
        return ["memory", contestant, scenario, "10000", "100000", "3"]
    if suite == "overload":
        return ["overload", scenario, contestant]
    if suite.startswith("net"):
        return ["net", scenario, contestant, str(args.warmups), str(args.iterations)]
    raise SystemExit(f"no command for suite {suite}")


def pinned(cpus, cmd):
    return (["taskset", "-c", cpus] + cmd) if cpus and shutil.which("taskset") else cmd


def start_server(args, env):
    server_env = dict(env, JAVA_OPTS=SERVER_JVM)
    log = open(args.out / "server.log", "a")
    p = subprocess.Popen(pinned(args.server_cpus, [str(binary), "net-server", str(args.port), "1"]),
                         env=server_env, stdout=log, stderr=log)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", args.port), timeout=0.2).close()
            return p
        except OSError:
            time.sleep(0.1)
    raise SystemExit("the net server did not start")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("suite")
    ap.add_argument("--forks", type=int, default=5)
    ap.add_argument("--warmups", type=int, default=2)
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--seed", type=int, default=20261023)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--timeout", type=int, default=900, help="seconds before one JVM is killed")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--only", help="run only cells whose 'scenario contestant' contains this")
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--server-cpus", default="3" if (os.cpu_count() or 1) >= 4 else None)
    ap.add_argument("--client-cpus", default=f"0-{(os.cpu_count() or 1) - 2}" if (os.cpu_count() or 1) >= 4 else None)
    args = ap.parse_args()
    args.out = args.out or root / "results" / args.suite
    args.out.mkdir(parents=True, exist_ok=True)

    if not args.no_build:
        subprocess.run(["./gradlew", "-q", "installDist"], cwd=root, check=True)
    # Gradle's and Kotlin's daemons hold memory and wake up now and then: none may run during a measurement.
    subprocess.run(["./gradlew", "-q", "--stop"], cwd=root, capture_output=True)
    subprocess.run(["pkill", "-f", "KotlinCompileDaemon"], capture_output=True)

    cells = [tuple(l.split()) for l in sh(str(binary), "cells", args.suite).splitlines() if l.strip()]
    if args.only:
        cells = [c for c in cells if args.only in " ".join(c)]
    schedule = []
    for r in range(1, args.forks + 1):
        order = list(cells)
        random.Random(args.seed * 1000 + r).shuffle(order)
        schedule += [{"round": r, "order": i, "scenario": s, "contestant": c} for i, (s, c) in enumerate(order)]

    meta = metadata(args, cells, schedule)
    if meta["dirty"]:
        print("warning: the working tree has uncommitted changes; results will not trace to a commit", file=sys.stderr)
    (args.out / "metadata.json").write_text(json.dumps(meta, indent=1))
    results = open(args.out / "results.jsonl", "w")
    runs = open(args.out / "runs.jsonl", "w")

    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    server = None
    current_round = None
    for n, cell in enumerate(schedule, 1):
        if args.suite.startswith("net") and cell["round"] != current_round:
            # A fresh server each round, so its JIT and heap are not a function of how far the suite has got.
            if server:
                server.terminate(); server.wait()
            server = start_server(args, env)
            current_round = cell["round"]
        props = (f"-Dbvr.commit={meta['commit']} -Dbvr.larkVersion={meta['larkCommit']} -Dbvr.run={run_id} "
                 f"-Dbvr.fork={cell['round']} -Dbvr.order={cell['order']} -Dbvr.netPort={args.port}")
        run_env = dict(env, JAVA_OPTS=f"{JVM[args.suite]} {props}")
        cmd = [str(binary)] + command(args.suite, cell["scenario"], cell["contestant"], args)
        if args.suite.startswith("net"):
            cmd = pinned(args.client_cpus, cmd)
        load = os.getloadavg()[0]
        t0 = time.time()
        print(f"[{n}/{len(schedule)}] round {cell['round']} {cell['scenario']} {cell['contestant']}", file=sys.stderr, flush=True)
        try:
            p = subprocess.run(cmd, env=run_env, capture_output=True, text=True, timeout=args.timeout)
            code, out, err = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            code, out, err = "timeout", (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or ""), ""
        for line in out.splitlines():
            if line.startswith("RESULT "):
                record = json.loads(line[7:])
                record.update(round=cell["round"], order=cell["order"], runId=run_id, suite=args.suite)
                results.write(json.dumps(record) + "\n")
        results.flush()
        runs.write(json.dumps(dict(cell, start=t0, end=time.time(), exit=code, loadBefore=load,
                                   stderrTail=[l for l in err.splitlines() if "WARNING" not in l][-8:])) + "\n")
        runs.flush()
        if code != 0:
            print(f"    exit {code}: {err.strip().splitlines()[-1:] if err.strip() else ''}", file=sys.stderr)
    if server:
        server.terminate(); server.wait()
    meta["finished"] = datetime.now(timezone.utc).isoformat()
    (args.out / "metadata.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
