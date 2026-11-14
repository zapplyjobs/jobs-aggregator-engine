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
#       through to rule b) - this holds for BOTH the 40-hex and the short-SHA
#       loop (real bump commits cite 7-8-char SHAs: bump-submodule.yml uses
#       $SHORT_SHA; a 40-hex-only requirement would false-fail every legit
#       bump). A wrong-but-real mention still fails (it resolves, none match).
#       ESCAPE: with [pin-rollback-ok] in the message, a rule-(a) mismatch is
#       not fatal by itself - the guard runs the ancestry check inline and
#       passes ONLY if the pin change is actually BACKWARD. This is what
#       makes the escape hatch reachable for real `git revert` messages,
#       which quote the bump SHA (Revert "chore: bump consumer to X") - this
#       estate's actual rollbacks (0830634/39f46c5) are exactly that shape.
#       A forward bump with a mismatched SHA is NOT saved by the marker.
#   (b) ANCESTRY: the new pin must not be an ANCESTOR of the previous pin
#       (silent backward pins - >=10 historical instances). Escape:
#       [pin-rollback-ok] in the HEAD message (acknowledged rollback).
#       Extended to consumer pins (AGG-PINANCESTOR-CONSUMER-1): engine commits
#       223dd4e/250a3f0e bumped the consumer pin forward and 0830634/39f46c56
#       rolled it back with NO escape marker - the gate passed silently because
#       the loop only covered the aggregator pin.
#   PASS cases: pin hold; pin absent from the commit; forward advance;
#   amend/rebase pairs (neither is the other's ancestor); messages without any
#   hex SHA (non-bump commits); acknowledged backward rollbacks (marker +
#   actually-backward, by rule a or rule b).
# Per-pin verdicts are all reported ("PASS [job-board-consumer]: ..."); the
# script exits nonzero if ANY pin fails.
# Runs after checkout; HEAD~1 must exist (gate checkout uses fetch-depth: 2).
# SUBMODULE_DIR_AGGREGATOR / SUBMODULE_DIR_CONSUMER override the engine-objects
# location for local runs (defaults to the in-repo submodule paths; they
# replace the old single-pin SUBMODULE_DIR seam, which had no callers).
set -euo pipefail

fail() { echo "::error::$1"; return 1; }

# is_backward EGIT OLD NEW: 0 iff NEW is a strict ancestor of OLD (a real
# backward pin move). Unresolvable pins or identical pins are not backward.
is_backward() {
  local egit="$1" old="$2" new="$3"
  [ -n "$old" ] && [ "$old" != "$new" ] || return 1
  $egit fetch origin "$old" "$new" >/dev/null 2>&1 || return 1
  $egit merge-base --is-ancestor "$new" "$old"
}

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
  old=$(git ls-tree HEAD~1 -- "$pin_path" | awk '{print $3}') || {
    fail "[$label] Cannot read the previous pin (git ls-tree HEAD~1 $pin_path failed)."; return
  }
  new=$(git ls-tree HEAD -- "$pin_path" | awk '{print $3}') || {
    fail "[$label] Cannot read the current pin (git ls-tree HEAD $pin_path failed)."; return
  }
  if [ -z "$new" ]; then
    echo "PASS [$label]: pin absent from this commit (not a pin change)"
    return 0
  fi
  if [ "$old" = "$new" ]; then
    echo "PASS [$label]: pin hold at ${new:0:8}"
    return 0
  fi

  # (a) message-tree agreement: resolve every mentioned SHA in this pin's
  # store; the tree pin must match at least one (short claims are expanded on
  # the store - bump messages cite short SHAs). A mention that resolves
  # nowhere is ignored. On a mismatch, [pin-rollback-ok] upgrades the verdict
  # to "acknowledged rollback" ONLY if the pin move is actually backward
  # (real git revert messages quote the bump SHA; see header).
  if grep -qE '\b[0-9a-f]{7,40}\b' <<<"$msg"; then
    local match=0 resolved_any=0 claimed full
    for claimed in $(grep -oE '\b[0-9a-f]{40}\b' <<<"$msg" | sort -u); do
      # rev-parse --verify alone ECHOES any syntactically-valid full SHA with
      # exit 0 even when the object is absent from this store (verified:
      # rev-parse rc=0 on a garbage SHA that cat-file does not list). ^{object}
      # forces the store lookup, so a mention living only in the OTHER pin's
      # store does not count as resolved here.
      full=$($egit rev-parse --verify --quiet "${claimed}^{object}" || true)
      if [ -n "$full" ]; then
        resolved_any=1
        [ "$new" = "$full" ] && match=1
      fi
    done
    for claimed in $(grep -oE '\b[0-9a-f]{7,39}\b' <<<"$msg" | sort -u); do
      full=$($egit rev-parse --verify --quiet "$claimed" || true)
      if [ -n "$full" ]; then
        resolved_any=1
        [ "$new" = "$full" ] && match=1
      fi
    done
    if [ "$resolved_any" = "1" ] && [ "$match" = "0" ]; then
      if grep -q '\[pin-rollback-ok\]' <<<"$msg" && is_backward "$egit" "$old" "$new"; then
        echo "PASS [$label]: acknowledged pin rollback to ${new:0:8} ([pin-rollback-ok])"
        return 0
      fi
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
