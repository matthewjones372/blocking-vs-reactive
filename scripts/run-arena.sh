#!/usr/bin/env bash
# The server round (HYPOTHESES-SERVERS.md). Each server gets the first two cores and gcannon, HttpArena's load
# generator, the other two. Every (run, profile, contestant) is a fresh server JVM, warmed for WARM seconds and then
# measured for DURATION, with contestants alternating inside each run. Writes RESULT lines to results/arena.txt.
#
# Needs, from outside this repo:
#   GCANNON   the gcannon binary (github.com/MDA2AV/gcannon, built against liburing 2.9 as HttpArena's Dockerfile does)
#
# SKIP_BUILD=1 uses the installDist already built. RUNS, WARM, DURATION, CONNS, PROFILES, CONTESTANTS and OUT override the defaults, for smoke tests.
set -euo pipefail
cd "$(dirname "$0")/.."
RUNS=${RUNS:-3}
WARM=${WARM:-10}
DURATION=${DURATION:-10}
CONNS=${CONNS:-4096}
PROFILES=${PROFILES:-"baseline pipelined json"}
CONTESTANTS=${CONTESTANTS:-"vt-arena uring-arena jetty-arena"}
OUT=${OUT:-results/arena.txt}
PORT=8080
: "${GCANNON:?set GCANNON to the gcannon binary}"

[[ -n ${SKIP_BUILD:-} ]] || ./gradlew -q installDist ${LARK_VERSION:+-PlarkVersion=$LARK_VERSION}
lib="build/install/blocking-vs-reactive/lib/*"
java="${JAVA_HOME:+$JAVA_HOME/bin/}java"
server_cpus=0,1
client_cpus=2,3
heap="-Xms1g -Xmx1g"

: > "$OUT"
{ "$java" -version 2>&1 | grep -v JAVA_TOOL | head -1; nproc; uname -r; "$GCANNON" --version 2>&1 | head -1 || true
  echo "server cpus $server_cpus; gcannon cpus $client_cpus; conns $CONNS; warm ${WARM}s; measured ${DURATION}s"; } \
  | sed 's/^/# /' >> "$OUT"

start_server() {
  case $1 in
    vt-arena) exec taskset -c $server_cpus "$java" $heap -cp "$lib" bvr.arena.VtArenaKt $PORT arena/dataset.json ;;
    # Exploratory, after the pre-registered round: vt-arena on JDK 27 ($JAVA27), as is and with a poller per carrier.
    vt-arena-27) exec taskset -c $server_cpus "$JAVA27" $heap -cp "$lib" bvr.arena.VtArenaKt $PORT arena/dataset.json ;;
    vt-arena-27-pc) exec taskset -c $server_cpus "$JAVA27" $heap -Djdk.pollerMode=3 -cp "$lib" bvr.arena.VtArenaKt $PORT arena/dataset.json ;;
    # Exploratory: vt-arena's handler on per-core carriers, each with its own io_uring, connections as continuations.
    uring-arena) exec taskset -c $server_cpus "$java" $heap --enable-native-access=ALL-UNNAMED \
                   --add-exports java.base/jdk.internal.vm=ALL-UNNAMED -cp "$lib" bvr.arena.UringArenaKt $PORT arena/dataset.json ;;
    jetty-arena) exec taskset -c $server_cpus "$java" $heap -cp "$lib" bvr.arena.JettyArenaKt $PORT arena/dataset.json ;;
  esac
}

load() { # load <profile> <seconds>
  local r=arena/requests
  case $1 in
    baseline) taskset -c $client_cpus "$GCANNON" "http://127.0.0.1:$PORT/" --raw "$r/get.raw,$r/post_cl.raw,$r/post_chunked.raw" \
                -c "$CONNS" -t 2 -d "${2}s" -p 1 --json ;;
    pipelined) taskset -c $client_cpus "$GCANNON" "http://127.0.0.1:$PORT/pipeline" -c "$CONNS" -t 2 -d "${2}s" -p 16 --json ;;
    json) taskset -c $client_cpus "$GCANNON" "http://127.0.0.1:$PORT/" --raw "$r/json-get.raw" -c "$CONNS" -t 2 -d "${2}s" -p 1 --recv-buf 65536 --json ;;
  esac
}

hz=$(getconf CLK_TCK)
cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }

for run in $(seq "$RUNS"); do
  for profile in $PROFILES; do
    for c in $CONTESTANTS; do
      log=$(mktemp)
      start_server "$c" > "$log" 2>&1 &
      server=$!
      for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:$PORT/pipeline" && break; sleep 0.1; done
      echo "run $run $profile $c" >&2
      load "$profile" "$WARM" > /dev/null
      c0=$(cpu_ticks "$server"); t0=$(date +%s%N)
      json=$(load "$profile" "$DURATION")
      pct=$(( ($(cpu_ticks "$server") - c0) * 100 * 1000000000 / hz / ($(date +%s%N) - t0) ))
      echo "$json" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
l, s, e = d["latency_us"], d["status"], d["errors"]
print("RESULT run=%s profile=%s contestant=%s rps=%.0f p50us=%d p99us=%d p999us=%d ok=%d non2xx=%d errors=%d reconnects=%d serverCpuPct=%s" % (
    sys.argv[1], sys.argv[2], sys.argv[3], d["rps"], l["p50"], l["p99"], l["p999"], s["2xx"],
    s["3xx"] + s["4xx"] + s["5xx"] + s["other"], e["connect"] + e["read"] + e["timeout"], d["reconnects"], sys.argv[4]))
' "$run" "$profile" "$c" "$pct" | tee -a "$OUT"
      kill "$server"; wait "$server" 2> /dev/null || true
      rm -f "$log"
      sleep 2
    done
  done
done
