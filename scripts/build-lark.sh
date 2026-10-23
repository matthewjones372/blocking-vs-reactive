#!/usr/bin/env bash
# Builds Lark at the commit in lark.commit and publishes it to ~/.m2 as 0.0.0-cand-SNAPSHOT, the version
# build.gradle.kts uses. LARK_DIR points at an existing checkout to use instead of cloning one.
set -euo pipefail
cd "$(dirname "$0")/.."
commit=$(cat lark.commit)
dir=${LARK_DIR:-build/lark-src}
if [[ ! -d $dir/.git ]]; then
  git clone --filter=blob:none https://github.com/matthewjones372/lark "$dir"
fi
git -C "$dir" fetch --quiet origin "$commit" || true
git -C "$dir" checkout --quiet "$commit"
(cd "$dir" && ./gradlew -q :lark:publishToMavenLocal :lark-stream:publishToMavenLocal :lark-stream-forks:publishToMavenLocal \
  -Prelease.forceVersion=0.0.0-cand -x test -x javadoc)
echo "lark $commit published as 0.0.0-cand-SNAPSHOT"
