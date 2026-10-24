#!/usr/bin/env bash
# Every suite, one after another, as published in results/: about six hours on a 4-core machine. Run from a clean,
# committed tree with JAVA_HOME at JDK 25 and Lark published (scripts/build-lark.sh). Nothing else should run.
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew -q installDist jmhJar
bin=build/install/blocking-vs-reactive/bin/blocking-vs-reactive
scripts/run-suite.py memory --forks 3 --no-build
scripts/run-suite.py overload --forks 4 --no-build
scripts/run-suite.py net --forks 3 --no-build
scripts/run-suite.py workload --forks 5 --no-build
scripts/run-suite.py io --forks 5 --no-build
scripts/run-jmh.sh
mkdir -p results/semantics
env -u JAVA_TOOL_OPTIONS JAVA_OPTS="-Xmx2g" $bin semantics | sed -n 's/^RESULT //p' > results/semantics/semantics.jsonl
env -u JAVA_TOOL_OPTIONS JAVA_OPTS="-Xmx2g" $bin threaddump | sed -n 's/^RESULT //p' > results/semantics/threaddump.jsonl
env -u JAVA_TOOL_OPTIONS JAVA_OPTS="-Xmx2g" $bin stacktraces > build/stacktraces.out
grep -v '^RESULT' build/stacktraces.out > results/semantics/stacktraces.txt
sed -n 's/^RESULT //p' build/stacktraces.out > results/semantics/stacktraces.jsonl
scripts/summarise.py memory overload net workload io jmh semantics
