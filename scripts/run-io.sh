#!/usr/bin/env bash
# Runs every I/O scenario for every contestant, each in FORKS fresh JVMs, one after another so no two compete for
# the CPU. Appends RESULT lines to results/io.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
FORKS=${FORKS:-2}
export JAVA_OPTS="${JAVA_OPTS:--Xms4g -Xmx4g -XX:+UseG1GC}"
./gradlew -q installDist
bin=build/install/blocking-vs-reactive/bin/blocking-vs-reactive
out=results/io.txt
: > "$out"
{ java -version 2>&1 | head -1; nproc; uname -r; } | sed 's/^/# /' >> "$out"
for scenario in backpressure fanout-1k fanout-10k inflight-100k; do
  for variant in "$scenario" "$scenario-native"; do
    contestants="vt-plain lark reactor pekko"
    [[ $scenario == backpressure ]] && contestants="$contestants vt-naive"
    for c in $contestants; do
      for fork in $(seq "$FORKS"); do
        echo "$variant $c fork $fork" >&2
        "$bin" "$variant" "$c" 2 4 | grep RESULT | sed "s/\$/ fork=$fork/" >> "$out"
      done
    done
  done
done
