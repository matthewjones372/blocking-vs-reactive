#!/usr/bin/env bash
# The JMH benchmarks (CPU-only chain, hand-off, per-element machinery): five forks each, settings on each class.
# Writes results/jmh/jmh.json, the JMH log, and metadata. JMH_INCLUDES=<regex> runs a subset.
set -euo pipefail
cd "$(dirname "$0")/.."
out=results/jmh
mkdir -p "$out"
./gradlew -q jmhJar
./gradlew -q --stop || true
pkill -f KotlinCompileDaemon || true
{
  echo "{\"commit\": \"$(git rev-parse HEAD)\", \"dirty\": $([[ -n $(git status --porcelain --untracked-files=no) ]] && echo true || echo false),"
  echo " \"larkCommit\": \"$(cat lark.commit)\", \"cpus\": $(nproc), \"kernel\": \"$(uname -r)\","
  echo " \"cpu\": \"$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')\","
  echo " \"java\": \"$("$JAVA_HOME"/bin/java -version 2>&1 | head -1 | sed 's/"/\\"/g')\", \"started\": \"$(date -u +%FT%TZ)\"}"
} > "$out/metadata.json"
env -u JAVA_TOOL_OPTIONS "$JAVA_HOME"/bin/java -jar build/libs/blocking-vs-reactive-jmh.jar ${JMH_INCLUDES:-} \
  -f 5 -prof gc -rf json -rff "$out/jmh.json" -jvmArgs "-Xms2g -Xmx2g -XX:+UseG1GC --enable-preview" | tee "$out/jmh.txt"
