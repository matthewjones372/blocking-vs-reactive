#!/usr/bin/env bash
# The real-network scenario. Each fork gets a fresh server JVM (bvr.NetServerKt: HTTP/1.1 on loopback, every answer
# 10 ms after its request) pinned to the last core, and a client JVM pinned to the others, so the client's process
# CPU is the client's alone. Runs one fork at a time and writes RESULT lines to results/net.txt.
#
# Two passes:
#   1. every contestant at 16, 1,000 and 10,000 calls in flight, with the JVM's default settings;
#   2. the blocking contestants at 1,000 and 10,000 again, under other virtual-thread scheduler and poller settings.
#      The values `jdk.pollerMode` accepts on JDK 25 are 1 (SYSTEM_THREADS) and 2 (VTHREAD_POLLERS, Linux's default);
#      anything else throws (sun/nio/ch/Poller.java). Default parallelism is the client's core count.
#
# FORKS, WARMUPS, ITERATIONS, LEVELS, CONTESTANTS, PASSES and OUT override the defaults, for smoke tests.
set -euo pipefail
cd "$(dirname "$0")/.."
FORKS=${FORKS:-2}
WARMUPS=${WARMUPS:-2}
ITERATIONS=${ITERATIONS:-4}
LEVELS=${LEVELS:-"net-16 net-1k net-10k"}
CONTESTANTS=${CONTESTANTS:-"vt-plain lark lark-async reactor-netty reactor-jdk pekko vt-socket lark-socket"}
SCHED_LEVELS=${SCHED_LEVELS:-"net-1k net-10k"}
SCHED_CONTESTANTS=${SCHED_CONTESTANTS:-"vt-plain lark vt-socket lark-socket"}
PASSES=${PASSES:-"main sched"}
OUT=${OUT:-results/net.txt}
PORT=${PORT:-8099}
BASE_OPTS="-Xms4g -Xmx4g -XX:+UseG1GC -Dbvr.net.port=$PORT"

./gradlew -q installDist ${LARK_VERSION:+-PlarkVersion=$LARK_VERSION}
bin=build/install/blocking-vs-reactive/bin/blocking-vs-reactive
cp="build/install/blocking-vs-reactive/lib/*"

ncpu=$(nproc)
if command -v taskset > /dev/null && (( ncpu >= 2 )); then
  pin_server=(taskset -c $((ncpu - 1)))
  pin_client=(taskset -c 0-$((ncpu - 2)))
else
  pin_server=()
  pin_client=()
fi

: > "$OUT"
{ java -version 2>&1 | grep -v JAVA_TOOL | head -1; nproc; uname -r
  echo "server: ${pin_server[*]:-unpinned}; client: ${pin_client[*]:-unpinned}"; } | sed 's/^/# /' >> "$OUT"

hz=$(getconf CLK_TCK)
cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }

# fork <scenario> <contestant> <fork> <config label> [extra JVM options...]
fork() {
  local scenario=$1 contestant=$2 n=$3 config=$4
  shift 4
  local log
  log=$(mktemp)
  "${pin_server[@]}" "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Xms1g -Xmx1g -cp "$cp" bvr.NetServerKt "$PORT" 10000 1 > "$log" 2>&1 &
  local server=$!
  for _ in $(seq 100); do grep -q NETSERVER "$log" && break; sleep 0.1; done
  grep -q NETSERVER "$log" || { cat "$log" >&2; exit 1; }
  local t0 c0
  t0=$(date +%s%N)
  c0=$(cpu_ticks "$server")
  echo "$scenario $contestant fork $n${config:+ [$config]}" >&2
  JAVA_OPTS="$BASE_OPTS $*" "${pin_client[@]}" "$bin" "$scenario" "$contestant" "$WARMUPS" "$ITERATIONS" ${config:+"$config"} \
    | grep RESULT | sed "s/\$/ fork=$n/" >> "$OUT"
  # The server's average CPU over the whole fork, warm-ups included: near 100% means the server, not the client,
  # set the pace.
  local pct
  pct=$(( ($(cpu_ticks "$server") - c0) * 100 * 1000000000 / hz / ($(date +%s%N) - t0) ))
  echo "RESULT scenario=$scenario contestant=$contestant${config:+ config=$config} serverCpuPct=$pct fork=$n" >> "$OUT"
  kill "$server"
  wait "$server" 2> /dev/null || true
  rm -f "$log"
}

if [[ " $PASSES " == *" main "* ]]; then
  for scenario in $LEVELS; do
    for c in $CONTESTANTS; do
      for n in $(seq "$FORKS"); do fork "$scenario" "$c" "$n" ""; done
    done
  done
fi

if [[ " $PASSES " == *" sched "* ]]; then
  for config in parallelism-2 poller-1 poller-2; do
    case $config in
      parallelism-2) opts="-Djdk.virtualThreadScheduler.parallelism=2" ;;
      poller-1) opts="-Djdk.pollerMode=1" ;;
      poller-2) opts="-Djdk.pollerMode=2" ;;
    esac
    for scenario in $SCHED_LEVELS; do
      for c in $SCHED_CONTESTANTS; do
        for n in $(seq "$FORKS"); do fork "$scenario" "$c" "$n" "$config" "$opts"; done
      done
    done
  done
fi
