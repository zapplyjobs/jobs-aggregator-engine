#!/usr/bin/env bash
# AGG-PIN-ANCESTOR-GUARD-1 (2026-09-22) / AGG-PINANCESTOR-CONSUMER-1 (2026-11-13):
# block submodule-pin defects on landing, for BOTH submodule pins
# (.github/scripts/aggregator AND .github/scripts/consumer).
# Designed from a full-history dry run (862 gitlink transitions, 520
# SHA-claiming commits replayed); catches the observed defect classes:
#   (a) MESSAGE-TREE AGREEMENT: if the HEAD commit mentions hex SHA(s), the
#       tree's pin must match at least one mention that resolves in the pin's
#       store. A294 incident class - a "bump to X" commit whose tree still
#       pins the older SHA (09bfdd06: message claimed 8f42f67, tree kept
#       639abacd; the stale-index no-op bump that left the pin 62h behind
#       engine main). A mention that resolves NOWHERE is ignored (falls
#       through to rule b) - the original "orphan fail" wording oversold this;
#       the code fails only when >=1 mention resolves and none matches.
#   (b) ANCESTRY: the new pin must not be an ANCESTOR of the previous pin
#       (silent backward pins - >=10 historical instances). Escape:
#       [pin-rollback-ok] in the HEAD message (acknowledged rollback).
#       Extended to consumer pins (AGG-PINANCESTOR-CONSUMER-1): engine commits
#       223dd4e/250a3f0e bumped the consumer pin forward and 0830634/39f46c56
#       rolled it back with NO escape marker - the gate passed silently because
#       the loop only covered the aggregator pin.
#   PASS cases: pin hold; pin absent from the commit; forward advance;
#   amend/rebase pairs (neither is the other's ancestor); messages without any
#   hex SHA (non-bump commits).
# Per-pin verdicts are all reported ("PASS [job-board-consumer]: ..."); the
# script exits nonzero if ANY pin fails.
# Runs after checkout; HEAD~1 must exist (gate checkout uses fetch-depth: 2).
# SUBMODULE_DIR_AGGREGATOR / SUBMODULE_DIR_CONSUMER override the engine-objects
# location for local runs (defaults to the in-repo submodule paths; they
# replace the old single-pin SUBMODULE_DIR seam, which had no callers).
set -euo pipefail

fail() { echo "::error::$1"; return 1; }

check_pin() { # <pin-path> <label> <head-message>
  local pin_path="$1" label="$2" msg="$3"
  local store
  case "$pin_path" in
    .github/scripts/aggregator) store="${SUBMODULE_DIR_AGGREGATOR:-$PWD/$pin_path}" ;;
    .github/scripts/consumer)   store="${SUBMODULE_DIR_CONSUMER:-$PWD/$pin_path}" ;;
    *) echo "::error::check_pin: unknown pin path $pin_path"; return 1 ;;
  esac
  local egit="git --git-dir=$store/.git"

  local old new
  old=$(git ls-tree HEAD~1 -- "$pin_path" | awk '{print $3}')
  new=$(git ls-tree HEAD -- "$pin_path" | awk '{print $3}')
  if [ -z "$new" ]; then
    echo "PASS [$label]: pin absent from this commit (not a pin change)"
    return 0
  fi
  if [ "$old" = "$new" ]; then
    echo "PASS [$label]: pin hold at ${new:0:8}"
    return 0
  fi

  # (a) message-tree agreement: resolve every mentioned SHA in this pin's
  # store; the tree pin must match at least one (full-SHA mentions match by
  # prefix; 7-char claims are expanded on the store).
  if grep -qE '\b[0-9a-f]{7,40}\b' <<<"$msg"; then
    local match=0 resolved_any=0 claimed full
    for claimed in $(grep -oE '\b[0-9a-f]{40}\b' <<<"$msg" | sort -u); do
      resolved_any=1
      full=$($egit rev-parse --verify --quiet "$claimed" || true)
      if [ -n "$full" ] && [ "$new" = "$full" ]; then match=1; fi
    done
    for claimed in $(grep -oE '\b[0-9a-f]{7,39}\b' <<<"$msg" | sort -u); do
      full=$($egit rev-parse --verify --quiet "$claimed" || true)
      if [ -n "$full" ]; then
        resolved_any=1
        [ "$new" = "$full" ] && match=1
      fi
    done
    if [ "$resolved_any" = "1" ] && [ "$match" = "0" ]; then
      fail "[$label] Pin/message mismatch: commit mentions ${label} SHA(s) but the tree pins ${new:0:8}. Fix the pin (bump to the intended commit) or remove the stale SHA from the message." || return
    fi
  fi

  # (b) ancestry: block silent backward pins unless acknowledged.
  if [ -n "$old" ] && [ "$old" != "$new" ]; then
    $egit fetch origin "$old" "$new" >/dev/null 2>&1 || {
      fail "[$label] Cannot resolve pins on the ${label} remote: old=${old:0:8} new=${new:0:8}. Is the new pin merged to ${label} main?" || return
    }
    if $egit merge-base --is-ancestor "$new" "$old"; then
      if grep -q '\[pin-rollback-ok\]' <<<"$msg"; then
        echo "PASS [$label]: acknowledged pin rollback to ${new:0:8} ([pin-rollback-ok])"
        return 0
      fi
      fail "[$label] Submodule pin REGRESSION: ${new:0:8} is an ancestor of the current pin ${old:0:8}. Bump forward, or add [pin-rollback-ok] to acknowledge an intentional rollback." || return
    fi
  fi
  echo "PASS [$label]: pin ${new:0:8} (no defect detected)"
  return 0
}

git rev-parse --verify -q HEAD~1 >/dev/null || {
  echo "::error::HEAD~1 missing - the pin guard needs the previous commit (gate checkout uses fetch-depth: 2; local runs need a repo with >=2 commits)."
  exit 1
}

MSG=$(git log -1 --pretty=%B)
status=0
check_pin ".github/scripts/aggregator" "job-board-aggregator" "$MSG" || status=1
check_pin ".github/scripts/consumer"   "job-board-consumer"   "$MSG" || status=1
exit "$status"
