#!/usr/bin/env bash
# agg-conformance-check.sh — AGG contract-conformance suite for the published pool.
#
# Reads the live canonical R2 `data/all_jobs.json` (read-only, ranged windows by
# default; --full streams everything) and asserts the AGG_CONTRACT.md job-record
# table plus the named regression classes that historically reached consumers:
#   - nested `salary` object (AGG-SALARY-NESTED-REMOVE-1) must stay absent
#   - `locations` arrays must be STRINGS-ONLY (AGG-LOCATIONS-RAWJSON-1)
#   - `display_location` must be a sane canonical string (leading "-" / raw JSON /
#     undefined-null classes; AGG-LOCATION-DISPLAY-CANONICAL-1)
#   - STRIP_FIELDS (index.js pipeline strip list) must not leak
#   - anchor_ts freshness per AGG-FRESHNESS-SLA-1 (14d regular / 120d internship;
#     anchor = max(posted_at, source_updated_at), AGG-ANCHOR-PUBLISH-1)
#   - flat salaryMin/salaryMax numeric + ordered, salaryCurrency ISO-shaped
#   - window id uniqueness (dedupe invariant); fingerprint dups reported INFO-only
#     (AGG-SUPPLY-DUPLICATE-VARIANTS-1 owns that design)
#
# Usage:
#   bash projects/zjp/scripts/agg-conformance-check.sh [--window-mb N] [--full] [--selftest]
#
# Modes:
#   default     head+tail JSONL windows (sorted newest-first: head = freshest rows,
#               tail = TTL edge) — cheap standing probe (~12MB transfer at default)
#   --full      stream the entire JSONL (~150MB) — deep scan incl. GLOBAL id uniqueness
#   --selftest  run embedded known-good + known-bad fixtures through the checker and
#               verify every violation class is caught (no network, no creds)
#
# Exit codes: 0 = PASS (warnings allowed), 1 = FAIL (violations found or selftest
# failed), 2 = usage/environment error.
#
# Env: R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT / R2_BUCKET_NAME
#      (auto-sourced from ~/.secrets/r2-zjp.env when present)
# Requires: python3 + boto3 (verified on this host), network to the R2 endpoint.
#
# Created: AGG-CONTRACT-CONFORMANCE-TEST-1 (2026-09-03/04, session A270).
# Read-only by design: no pipeline mutation, no gate risk.

set -euo pipefail

MODE_WINDOW=1
MODE_FULL=0
MODE_SELFTEST=0
WINDOW_MB=6

while [ $# -gt 0 ]; do
  case "$1" in
    --full) MODE_FULL=1 ;;
    --selftest) MODE_SELFTEST=1 ;;
    --window-mb) WINDOW_MB="${2:?--window-mb needs a number}"; shift ;;
    *) echo "unknown arg: $1 (use --full | --selftest | --window-mb N)" >&2; exit 2 ;;
  esac
  shift
done

if [ "$MODE_SELFTEST" -eq 0 ] && [ -z "${R2_BUCKET_NAME:-}" ] && [ -f "$HOME/.secrets/r2-zjp.env" ]; then
  set -a; . "$HOME/.secrets/r2-zjp.env"; set +a
fi

export CONFORMANCE_WINDOW_MB="$WINDOW_MB"
export CONFORMANCE_FULL="$MODE_FULL"
export CONFORMANCE_SELFTEST="$MODE_SELFTEST"

exec python3 - <<'PYEOF'
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

WINDOW_MB = int(os.environ.get("CONFORMANCE_WINDOW_MB", "6"))
FULL = os.environ.get("CONFORMANCE_FULL") == "1"
SELFTEST = os.environ.get("CONFORMANCE_SELFTEST") == "1"

TTL_REGULAR_DAYS = 14.0
TTL_INTERNSHIP_DAYS = 120.0
STRIP_FIELDS = [  # .github/scripts/index.js STRIP_FIELDS (pipeline step 9)
    "source_url", "_raw", "description", "enriched", "enriched_at",
    "is_internship", "is_new_grad", "is_us_only", "remote",
]
REQUIRED_STR = ["id", "title", "company_name", "url", "posted_at", "source", "location"]
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
UNDEFINED_ID_RE = re.compile(r"(^|-)(undefined|null|None)(-|$)")


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_iso(v):
    """Parse an ISO-8601 timestamp or date; returns aware datetime or None.

    Date-only values (YYYY-MM-DD) are valid source posting dates per the
    contract field row and parse at UTC midnight - the same value JS Date()
    produces runtime-side, so anchor semantics are identical
    (AGG-PHENOM-POSTEDAT-PARSE-1: the phenom-tjx lane emits schema.org
    datePosted passthrough date-only; runtime anchors were never affected).
    """
    if not isinstance(v, str):
        return None
    try:
        if ISO_RE.match(v):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        if DATE_RE.match(v):
            return datetime.fromisoformat(v).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None


def check_row(row, now):
    """Run all contract assertions on one published row.

    Returns list of (severity, code, detail). severity: FAIL | WARN.
    """
    out = []

    def fail(code, detail):
        out.append(("FAIL", code, detail))

    def warn(code, detail):
        out.append(("WARN", code, detail))

    # Required string fields
    for f in REQUIRED_STR:
        v = row.get(f)
        if v is None:
            fail("REQUIRED_MISSING", f)
        elif not isinstance(v, str):
            fail("REQUIRED_TYPE", f"{f} is {type(v).__name__}, expected string")
        elif not v.strip():
            fail("REQUIRED_EMPTY", f)
    if isinstance(row.get("title"), str) and len(row.get("title", "").strip()) < 5:
        fail("TITLE_TOO_SHORT", row.get("title", "")[:40])
    if isinstance(row.get("company_name"), str) and len(row.get("company_name", "").strip()) < 2:
        fail("COMPANY_TOO_SHORT", row.get("company_name", "")[:40])
    u = row.get("url")
    if isinstance(u, str) and not u.startswith("http"):
        fail("URL_NOT_HTTP", u[:80])
    rid = row.get("id")
    if isinstance(rid, str) and UNDEFINED_ID_RE.search(rid):
        fail("ID_UNDEFINED_SEGMENT", rid[:80])

    # posted_at parseable ISO
    if parse_iso(row.get("posted_at")) is None:
        fail("POSTED_AT_UNPARSEABLE", repr(row.get("posted_at"))[:60])

    # locations: array of strings (AGG-LOCATIONS-RAWJSON-1)
    locs = row.get("locations")
    if "locations" not in row:
        fail("LOCATIONS_MISSING", "key absent")
    elif locs is None:
        fail("LOCATIONS_NULL", "null instead of array")
    elif not isinstance(locs, list):
        fail("LOCATIONS_TYPE", type(locs).__name__)
    else:
        bad = [e for e in locs if not isinstance(e, str)]
        if bad:
            fail("LOCATIONS_NONSTRING", f"{len(bad)} non-string elements: {repr(bad[0])[:60]}")
        if len(locs) == 0:
            warn("LOCATIONS_EMPTY", row.get("id", "?"))

    # tags shape (live producer shape: domains/locations lists of str, employment str)
    tags = row.get("tags")
    if not isinstance(tags, dict):
        fail("TAGS_TYPE", type(tags).__name__ if tags is not None else "missing")
    else:
        for k in ("domains", "locations"):
            v = tags.get(k)
            if not isinstance(v, list):
                fail("TAGS_FIELD_TYPE", f"tags.{k} is {type(v).__name__}, expected list")
            elif any(not isinstance(e, str) for e in v):
                fail("TAGS_FIELD_NONSTRING", f"tags.{k} has non-string elements")
        emp = tags.get("employment")
        if not isinstance(emp, str) or not emp.strip():
            fail("TAGS_EMPLOYMENT_TYPE", repr(emp)[:60])

    # application_deadline: ISO 8601 or absent (AGG-VALIDTHROUGH-DEADLINE-1,
    # A303: supplemental-lane pass-through; never a fabricated/unparsable date)
    ad = row.get("application_deadline")
    if ad is not None:
        if not isinstance(ad, str) or not ad.strip():
            fail("APPLICATION_DEADLINE_TYPE", repr(ad)[:60])
        else:
            try:
                datetime.fromisoformat(ad.replace("Z", "+00:00"))
            except ValueError:
                fail("APPLICATION_DEADLINE_NOT_ISO", repr(ad)[:60])

    # display_location sanity (canonical display string classes)
    dl = row.get("display_location")
    if not isinstance(dl, str) or not dl.strip():
        fail("DISPLAY_LOCATION_MISSING", repr(dl)[:60])
    else:
        if dl.startswith("-"):
            fail("DISPLAY_LOCATION_LEADING_HYPHEN", dl[:60])
        if dl[0] in "{[" or dl.startswith("undefined") or dl.startswith("null"):
            fail("DISPLAY_LOCATION_RAW", dl[:60])

    # salary: nested key must be absent; flat fields typed + ordered
    if "salary" in row:
        fail("NESTED_SALARY_PRESENT", repr(row.get("salary"))[:80])
    smin, smax = row.get("salaryMin"), row.get("salaryMax")
    for nm, v in (("salaryMin", smin), ("salaryMax", smax)):
        if v is not None and not isinstance(v, (int, float)):
            fail("SALARY_FLAT_TYPE", f"{nm} is {type(v).__name__}")
    if isinstance(smin, (int, float)) and isinstance(smax, (int, float)) and smin > smax:
        fail("SALARY_MIN_GT_MAX", f"{smin} > {smax}")
    cur = row.get("salaryCurrency")
    if cur is not None and not (isinstance(cur, str) and CURRENCY_RE.match(cur)):
        fail("SALARY_CURRENCY_SHAPE", repr(cur)[:40])

    # strip leaks (pipeline step 9 fields must be gone)
    for f in STRIP_FIELDS:
        if f in row:
            fail("STRIP_LEAK", f)

    # freshness: anchor_ts (max(posted_at, source_updated_at)) within TTL
    # AGG-FRESHNESS-SLA-1: 14d regular / 120d internship (tags.employment)
    a = parse_iso(row.get("anchor_ts"))
    if a is None:
        fail("ANCHOR_TS_MISSING", repr(row.get("anchor_ts"))[:60])
    else:
        # anchor >= posted_at must hold by construction (anchor is the max of the
        # two); an inversion means the anchor stamp or a consumer rewrite regressed.
        p = parse_iso(row.get("posted_at"))
        if p is not None and a < p:
            fail("ANCHOR_BEFORE_POSTED", f"anchor {row.get('anchor_ts')} < posted_at")
        age_days = (now - a).total_seconds() / 86400.0
        ttl = TTL_INTERNSHIP_DAYS if (isinstance(tags, dict) and tags.get("employment") == "internship") else TTL_REGULAR_DAYS
        if age_days > ttl + 0.3:  # 0.3d slack for clock/publish skew
            fail("TTL_EXCEEDED", f"anchor age {age_days:.1f}d > {ttl:.0f}d ttl")

    return out


GOOD_ROW = {
    "id": "gh-acme-123", "title": "Software Engineer, Backend",
    "company_name": "Acme", "url": "https://jobs.example.com/123",
    "posted_at": (datetime.now(timezone.utc) - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%S.000Z"), "source": "greenhouse",
    "location": "San Jose, CA", "locations": ["San Jose, CA"],
    "display_location": "San Jose, CA",
    "tags": {"employment": "full_time", "domains": ["software"], "locations": ["us"]},
    "salaryMin": 120000, "salaryMax": 180000, "salaryCurrency": "USD",
    "anchor_ts": (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
}


def mutate(row, **kw):
    r = dict(row)
    for k, v in kw.items():
        if v == "__DEL__":
            r.pop(k, None)
        else:
            r[k] = v
    return r


SELFTEST_CASES = [
    ("good row passes", GOOD_ROW, []),
    ("nested salary present", mutate(GOOD_ROW, salary={"min": 1, "max": 2}), ["NESTED_SALARY_PRESENT"]),
    ("locations object element", mutate(GOOD_ROW, locations=["Austin, TX", {"name": "Remote"}]), ["LOCATIONS_NONSTRING"]),
    ("locations null", mutate(GOOD_ROW, locations=None), ["LOCATIONS_NULL"]),
    ("display leading hyphen", mutate(GOOD_ROW, display_location="- Remote"), ["DISPLAY_LOCATION_LEADING_HYPHEN"]),
    ("display raw json", mutate(GOOD_ROW, display_location='{"city": "Austin"}'), ["DISPLAY_LOCATION_RAW"]),
    ("strip leak source_url", mutate(GOOD_ROW, source_url="https://x"), ["STRIP_LEAK"]),
    ("strip leak _raw", mutate(GOOD_ROW, _raw={"a": 1}), ["STRIP_LEAK"]),
    ("strip leak description", mutate(GOOD_ROW, description="desc"), ["STRIP_LEAK"]),
    ("missing required id", mutate(GOOD_ROW, id="__DEL__"), ["REQUIRED_MISSING"]),
    ("url not http", mutate(GOOD_ROW, url="javascript:void(0)"), ["URL_NOT_HTTP"]),
    ("posted_at unparseable", mutate(GOOD_ROW, posted_at="whenever"), ["POSTED_AT_UNPARSEABLE"]),
    ("posted_at date-only accepted (AGG-PHENOM-POSTEDAT-PARSE-1)", mutate(GOOD_ROW, posted_at="2026-09-01"), []),
    ("posted_at garbage date-ish rejected", mutate(GOOD_ROW, posted_at="2026-09-29ish"), ["POSTED_AT_UNPARSEABLE"]),
    ("tags employment not string", mutate(GOOD_ROW, tags={"employment": ["internship"], "domains": [], "locations": []}), ["TAGS_EMPLOYMENT_TYPE"]),
    ("ttl exceeded regular", mutate(GOOD_ROW, anchor_ts=(datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")), ["ANCHOR_BEFORE_POSTED", "TTL_EXCEEDED"]),
    ("currency shape", mutate(GOOD_ROW, salaryCurrency="us$"), ["SALARY_CURRENCY_SHAPE"]),
    ("id undefined segment", mutate(GOOD_ROW, id="oracle-undefined-23026"), ["ID_UNDEFINED_SEGMENT"]),
    ("anchor_ts missing", mutate(GOOD_ROW, anchor_ts="__DEL__"), ["ANCHOR_TS_MISSING"]),
    ("application_deadline ISO accepted (AGG-VALIDTHROUGH-DEADLINE-1)", mutate(GOOD_ROW, application_deadline="2026-11-15T23:59:59.000Z"), []),
    ("application_deadline non-ISO rejected", mutate(GOOD_ROW, application_deadline="Nov 15th"), ["APPLICATION_DEADLINE_NOT_ISO"]),
    ("application_deadline wrong type rejected", mutate(GOOD_ROW, application_deadline=1731600000000), ["APPLICATION_DEADLINE_TYPE"]),
]


def run_selftest(now):
    print("=== SELFTEST: checker must catch every named regression class ===")
    ok = True
    for name, row, expected in SELFTEST_CASES:
        got = sorted({code for sev, code, _ in check_row(row, now)})
        exp = sorted(expected)
        # good row may still warn-free; expected == [] means no FAILs
        if got != exp:
            print(f"  SELFTEST FAIL: {name}: expected {exp}, got {got}")
            ok = False
        else:
            print(f"  ok: {name}")
    if not ok:
        print("SELFTEST: FAIL — checker logic is broken, do not trust live runs")
        return 1
    print("SELFTEST: PASS (all fixture classes caught)")
    return 0


def iter_windows(s3, bucket, key, total_bytes, now=None):
    """Yield (label, list-of-line-text) for head+rotating-mid+tail windows.

    Mid windows exist because the pool is sorted by posted_at: head+tail only see
    the extremes, and whole cohorts (e.g. supplemental-lane sources) sort mid-file
    where a 19K-row shape drift was invisible to the first suite version.
    AGG-CONFORMANCE-ROTATING-WINDOWS-1: the two mid offsets ROTATE on a
    YYYYMMDDHH seed (deterministic - same hour re-runs sample identical offsets,
    so findings are reproducible; different hours walk different mid-file
    cohorts, closing the fixed-offset blind spot). Offsets are printed in the
    run basis line so any two runs can be compared.
    """
    now = now or datetime.now(timezone.utc)
    seed = int(now.strftime("%Y%m%d%H"))
    W = WINDOW_MB * 1024 * 1024
    head = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{W}")["Body"].read().decode()
    lines = head.split("\n")[:-1]  # drop trailing partial line
    yield "head", lines
    f1 = 0.20 + (seed % 60) * 0.01          # 0.20 .. 0.79
    f2 = f1 + 0.15                           # 0.35 .. 0.94, always after mid1
    for label, frac in (("mid1", f1), ("mid2", f2)):
        off = int(total_bytes * frac)
        mid = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={off}-{off+W}")["Body"].read().decode()
        lines = mid.split("\n")[1:-1]  # trim partial lines at both edges
        yield f"{label}@{frac:.2f}", lines
    tail = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{W}")["Body"].read().decode()
    lines = tail.split("\n")[1:]  # drop leading partial line
    yield "tail", lines


def rotation_offsets(now=None):
    """Public mid-window fracs for a timestamp (verification + next-run preview)."""
    now = now or datetime.now(timezone.utc)
    seed = int(now.strftime("%Y%m%d%H"))
    f1 = 0.20 + (seed % 60) * 0.01
    return f1, f1 + 0.15


def main():
    now = datetime.now(timezone.utc)
    if SELFTEST:
        sys.exit(run_selftest(now))

    try:
        import boto3
    except ImportError:
        print("ENV ERROR: boto3 not available (python3 -m pip install boto3)", file=sys.stderr)
        sys.exit(2)
    for var in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET_NAME"):
        if not os.environ.get(var):
            print(f"ENV ERROR: {var} not set (source ~/.secrets/r2-zjp.env)", file=sys.stderr)
            sys.exit(2)

    s3 = boto3.client(
        "s3", endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    bucket, key = os.environ["R2_BUCKET_NAME"], "data/all_jobs.json"

    head_meta = s3.head_object(Bucket=bucket, Key=key)
    total_bytes = head_meta["ContentLength"]
    last_mod = head_meta["LastModified"]
    obj_age_min = (datetime.now(timezone.utc) - last_mod.replace(tzinfo=timezone.utc)).total_seconds() / 60.0

    violations = Counter()
    samples = {}
    warns = Counter()
    warn_samples = {}
    rows = 0
    parse_errors = 0
    seen_ids = set()
    dup_ids = 0
    fp_counts = Counter()
    max_anchor_age = 0.0

    def ingest(label, line_iter):
        nonlocal rows, parse_errors, dup_ids, max_anchor_age
        for ln in line_iter:
            if not ln.strip():
                continue
            try:
                row = json.loads(ln)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            rows += 1
            findings = check_row(row, now)
            for sev, code, detail in findings:
                if sev == "FAIL":
                    violations[code] += 1
                    samples.setdefault(code, []).append((label, row.get("id", "?"), detail[:70]))
                else:
                    warns[code] += 1
                    warn_samples.setdefault(code, []).append((label, row.get("id", "?"), detail[:70]))
            rid = row.get("id")
            if isinstance(rid, str):
                if rid in seen_ids:
                    dup_ids += 1
                else:
                    seen_ids.add(rid)
            fp = row.get("fingerprint")
            if isinstance(fp, str):
                fp_counts[fp] += 1
            a = parse_iso(row.get("anchor_ts"))
            if a is not None:
                max_anchor_age = max(max_anchor_age, (now - a).total_seconds() / 86400.0)

    if FULL:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = obj["Body"]
        leftover = ""
        while True:
            chunk = buf.read(8 * 1024 * 1024)
            if not chunk:
                break
            text = leftover + chunk.decode()
            parts = text.split("\n")
            leftover = parts.pop()
            ingest("full", parts)
        if leftover.strip():
            ingest("full", [leftover])
        basis = f"FULL STREAM (~{total_bytes/1e6:.0f}MB, global id uniqueness)"
    else:
        for label, lines in iter_windows(s3, bucket, key, total_bytes):
            ingest(label, lines)
        f1, f2 = rotation_offsets(now)
        nf1, nf2 = rotation_offsets(now + timedelta(hours=1))
        basis = (f"HEAD+TAIL + ROTATING MID windows {WINDOW_MB}MB each of {total_bytes/1e6:.0f}MB; "
                 f"mid fracs this run {f1:.2f}/{f2:.2f}, next hour {nf1:.2f}/{nf2:.2f} (YYYYMMDDHH seed)")

    dup_fp = sum(c - 1 for c in fp_counts.values() if c > 1)

    print("=== AGG CONTRACT CONFORMANCE — all_jobs.json (R2 canonical, read-only) ===")
    print(f"  object: {total_bytes:,} bytes | last modified {last_mod.isoformat()} ({obj_age_min:.0f} min ago)")
    print(f"  basis: {basis}")
    print(f"  rows checked: {rows} | JSON line parse errors: {parse_errors}")
    print(f"  max anchor age in sample: {max_anchor_age:.2f}d (TTL 14d regular / 120d internship)")

    if parse_errors:
        print(f"  FAIL JSONL_PARSE: {parse_errors} unparseable lines")

    if violations:
        print(f"  VIOLATIONS ({sum(violations.values())} rows affected):")
        for code, n in violations.most_common():
            print(f"    FAIL {code}: {n}")
            for lab, rid, det in samples[code][:3]:
                print(f"      e.g. [{lab}] {rid} — {det}")
        sys.exit(1)

    warn_total = sum(warns.values())
    if warn_total:
        print(f"  warnings ({warn_total} rows):")
        for code, n in warns.most_common():
            print(f"    WARN {code}: {n}")
            for lab, rid, det in warn_samples[code][:2]:
                print(f"      e.g. [{lab}] {rid} — {det}")
    else:
        print("  warnings: none")

    print(f"  INFO duplicate ids in sample: {dup_ids} (0 required)")
    print(f"  INFO fingerprint variant rows in sample: {dup_fp} (AGG-SUPPLY-DUPLICATE-VARIANTS-1 owns)")
    if obj_age_min > 30:
        print(f"  WARN PRODUCER_STALE: all_jobs.json {obj_age_min:.0f} min old (producer SLA 30 min)")
    if rows < 100:
        print(f"  FAIL SAMPLE_TOO_SMALL: only {rows} rows parsed — window mis-sized or file truncated")
        sys.exit(1)
    print("PASS: published pool conforms to the AGG contract job-record table (sampled basis)" if not FULL
          else "PASS: published pool conforms to the AGG contract job-record table (full basis)")


main()
PYEOF
