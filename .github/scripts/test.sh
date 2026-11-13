#!/usr/bin/env bash
# Estate test runner — runs EVERY suite the engine deploys: the parent
# .github/scripts/__tests__/*.test.js battery AND the consumer submodule
# suite (lib/__tests__/*.test.js). Single source of truth: the gate
# (gate.yml), the pre-push hook, AND package.json "test" all call this,
# so every door runs the same suite. AGG-TESTCHAIN-REGISTRATION-1.
#
# Failure modes this kills (class: unregistered test silently never runs in CI):
# 1. consumer submodule tests were registered NOWHERE — no runner, no npm
#    script, no CI step. Five suites (dead-list, dead-urls, readme-sanitize,
#    readme-reuse-notice, visa-eligibility) never ran in any CI, while the
#    engine consumes consumer/lib directly (generate-and-push-readmes.js).
# 2. The old runner globbed .github/scripts/__tests__/*.test.js
#    NON-recursively — a test file in a nested dir would never run.
# 3. No floor guard: if __tests__ moved/renamed, the loop found zero files
#    and exited 0 ("All tests pass" on a zero-test run). The aggregator
#    runner fixed this class with MIN_TESTS (AGG-DISPCOVER-TESTGLOB-1);
#    the parent never got the same guard.
# (aggregator submodule keeps its own runner + CI: job-board-aggregator
#  ci-gate.yml runs `npm test` -> lib/__tests__/run-all.js.)
#
# Usage: bash .github/scripts/test.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Per-suite floor guards: if discovery collapses below the floor, the suite
# moved/renamed or the glob broke — fail loudly instead of passing narrow.
# Floors sit far below current counts (62 / 5 at registration time); they
# catch catastrophic discovery loss, not legitimate attrition.
suites=(
  ".github/scripts/__tests__|20"
  ".github/scripts/consumer/lib/__tests__|3"
)

fail=0
total=0
for suite in "${suites[@]}"; do
  dir="${suite%%|*}"; floor="${suite##*|}"
  if [ ! -d "$dir" ]; then
    echo "✗ suite dir missing: $dir (submodule not initialized or layout moved — refusing to pass)"
    fail=1
    continue
  fi
  # Recursive on purpose: nested test dirs must register too (the flat
  # glob's blind spot — plant-test receipt AGG-TESTCHAIN-REGISTRATION-1).
  mapfile -t files < <(find "$dir" -type f -name '*.test.js' | sort)
  if [ "${#files[@]}" -lt "$floor" ]; then
    echo "✗ $dir: discovered ${#files[@]} test files (floor $floor) — discovery broken; refusing to pass"
    fail=1
    continue
  fi
  for f in "${files[@]}"; do
    total=$((total + 1))
    if node "$f" > /dev/null 2>&1; then
      echo "✓ $f"
    else
      echo "✗ $f"
      fail=1
    fi
  done
done

if [ "$fail" -ne 0 ]; then
  echo "FAIL: one or more tests failed. Fix before pushing."
  exit 1
fi
echo "✓ All $total tests pass (matches gate)."
