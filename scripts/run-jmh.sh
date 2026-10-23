#!/usr/bin/env bash
# H4 and H5 under JMH; the JSON lands in results/jmh.json and the table in results/jmh.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew -q jmh | tee results/jmh.txt
cp build/results/jmh/results.json results/jmh.json
