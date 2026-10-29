#!/usr/bin/env python3
"""
verify-agg-aspect-ci.py — CI version of AGG aspect-status verifier.

Runs the 6 signal-based checks (no fs-based checks — workspace files
not available in CI). Publishes to R2 data/agg-aspect-status.json.

Full 10-aspect verification: run verify-agg-aspect-status.py from the
workspace (projects/zjp/scripts/).

Usage: python3 .github/scripts/verify-agg-aspect-ci.py
Env: GH_TOKEN (or GITHUB_TOKEN), R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
     R2_ENDPOINT, R2_BUCKET_NAME
"""
import json, os, subprocess, sys, datetime

NOW = datetime.datetime.now(datetime.timezone.utc)
AGG_REPO = "zapplyjobs/jobs-aggregator-engine"
AGG_REPOS = ["zapplyjobs/jobs-aggregator-engine", "zapplyjobs/job-board-aggregator"]
PROXY = os.environ.get("ASPECT_PROXY", "https://zjp-data-proxy.wild-queen-069e.workers.dev/data")

def gh_json(args):
    try:
        out = subprocess.check_output(["gh"] + args, text=True, stderr=subprocess.DEVNULL, timeout=30)
        return json.loads(out) if out.strip() else None
    except Exception:
        return None

def gh_runconclusion(repo, workflow=None, limit=5):
    args = ["run", "list", "-R", repo, "-L", str(limit), "--json", "status,conclusion,createdAt"]
    if workflow:
        args += ["--workflow", workflow]
    rows = gh_json(args) or []
    return [r for r in rows if r.get("status") == "completed"]

def proxy_json(path):
    try:
        url = f"{PROXY}/{path}"
        out = subprocess.check_output(["curl", "-sf", "-H", f"X-Proxy-Token: {os.environ.get('DATA_PROXY_TOKEN', '')}", url], text=True, timeout=15)
        return json.loads(out) if out.strip() else None
    except Exception:
        return None

def bucket_high(v, g, y): return "GREEN" if v >= g else ("YELLOW" if v >= y else "RED")
def bucket_low(v, g, y): return "GREEN" if v <= g else ("YELLOW" if v <= y else "RED")
def green_if(c): return "GREEN" if c else "RED"

def c_verification():
    runs = gh_runconclusion(AGG_REPO, workflow="fetch-jobs.yml", limit=3)
    if not runs: return "RED", "no completed fetch-jobs runs", "gh-api:CI"
    latest = runs[0]
    fails = [r for r in runs if r.get("conclusion") != "success"]
    s = "GREEN" if latest.get("conclusion") == "success" else "RED"
    return s, f"last {latest.get('conclusion')} ({latest['createdAt'][:10]}), {len(fails)} fail/3", "gh-api:CI"

def _split_failures_by_ownership(alerts):
    # AGG-ALERTOWNER-CONFIRM-1 (2026-08-16): split failure_details into own/foreign
    # by owner membership — dual-owner comma-lists (e.g. 45 "ENR,AGG") count as OWN.
    # unattributed = failure strings with no matching detail: fail-safe, we cannot
    # claim GREEN over failures we cannot attribute (owner field live since INF
    # 1ff986a/3e53995; verified on R2 2026-08-16).
    details = alerts.get("failure_details") or []
    def _is_own(d):
        return "AGG" in [o.strip() for o in str(d.get("owner") or "").split(",")]
    # AGG-ASPECT-SELFALERT-DEADLOCK-1 (2026-08-19, TAG B98 advisory): check-40's owner
    # is DYNAMIC (modules whose aspects are RED right now), so counting an AGG-owned
    # check-40 row here makes monitoring self-perpetuating — any prior AGG aspect RED
    # fires check-40 owner=AGG, c_monitoring goes YELLOW over it, and the loop never
    # clears even after the underlying aspect heals. Same exclusion TAG ships (9ec7729,
    # mirroring ENR's isSelfAspectAlert): the meta-alarm about our own aspects is not
    # an independent monitoring failure; the underlying aspects gate themselves.
    def _is_self_aspect(d):
        return d.get("id") == 40 or d.get("name") == "aspect-status RED"
    own = [d for d in details if _is_own(d) and not _is_self_aspect(d)]
    foreign = [d for d in details if not _is_own(d)]
    self_excluded = sum(1 for d in details if _is_own(d) and _is_self_aspect(d))
    unattributed = max(0, len(alerts.get("failures", [])) - len(details))
    return own, foreign, unattributed, self_excluded

def c_monitoring():
    m = proxy_json("zjp-metrics.json")
    if not m: return "RED", "metrics unreadable", "proxy:metrics"
    alerts = m.get("alerts", {})
    gen = m.get("generated_at", "")
    age = 999
    if gen:
        try:
            age = (NOW - datetime.datetime.fromisoformat(gen.replace("Z","+00:00"))).total_seconds()/60
        except: pass
    nw = alerts.get("warning_count", len(alerts.get("warnings", [])))
    own, foreign, unattributed, self_excluded = _split_failures_by_ownership(alerts)
    self_note = f"; {self_excluded} self-aspect check-40 alert(s) excluded" if self_excluded else ""
    # AGG-ASPECT-MONITOR-ALERTGATE-1 (H-AGG-4 green-while-alerting fix) +
    # AGG-ALERTOWNER-CONFIRM-1: gate on OWN failures only. Foreign failures render
    # as evidence, never as AGG status — the A219 global counter made every foreign
    # alert AGG's permanent yellow (standing-noise-becomes-invisible class; the
    # desc-backfill 7.5h silent outage sat behind exactly this). YELLOW = monitoring
    # works but a real OWN issue is alerting; RED = monitoring broken.
    if own:
        listing = " | ".join(f"#{d.get('id')} {str(d.get('name'))[:40]} (owner {d.get('owner')})" for d in own[:3])
        return "YELLOW", f"{len(own)} OWN failure(s) — metrics {age:.0f}min old — {listing}", "zjp-metrics.alerts.owner"
    if unattributed:
        return "YELLOW", f"{unattributed} unattributed failure(s) (no owner detail) — metrics {age:.0f}min old", "zjp-metrics.alerts.owner"
    ev = f", {len(foreign)} foreign failure(s) (evidence)" if foreign else ""
    if age <= 60: return "GREEN", f"0 own failures — metrics {age:.0f}min old ({nw} warnings{ev}{self_note})", "zjp-metrics.alerts.owner"
    elif age <= 1440: return "YELLOW", f"metrics {age:.0f}min old (stale{ev})", "zjp-metrics.alerts.owner"
    else: return "RED", f"metrics {age:.0f}min old", "zjp-metrics.alerts.owner"

# AGG-DATAQUALITY-STRUCTURAL-EXCLUDE-1: demoted hot-path sources with permanent T0
# (API rate-limits / no description API). Excluded from desc coverage to distinguish
# structural floor from actionable regression in healthy sources.
STRUCTURAL_DESC_SOURCES = {'oracle', 'microsoft', 'bytedance', 'apple', 'google'}
def c_data_quality():
    m = proxy_json("zjp-metrics.json")
    if not m: return "RED", "metrics unreadable", "proxy:metrics"
    issues = []
    # 1. Description retrievability (existing check)
    dq = m.get("enrichment",{}).get("description_quality",{})
    rate = dq.get("retrievable_description_pct")
    if rate is not None:
        s_desc = bucket_high(rate, 90, 80)
        if s_desc != "GREEN":
            # AGG-DATAQUALITY-STRUCTURAL-EXCLUDE-1: compute adjusted rate excluding structural T0
            jm_dq = proxy_json("jobs-metadata.json") or {}
            bs = jm_dq.get("by_source", {})
            total = sum(bs.values()) or 1
            structural = sum(bs.get(s, 0) for s in STRUCTURAL_DESC_SOURCES)
            if structural > 0 and total > structural:
                adjusted = min(rate * total / (total - structural), 100.0)
                if bucket_high(adjusted, 90, 80) == "GREEN":
                    pass  # YELLOW is structural-only — don't flag as an issue
                else:
                    issues.append(f"desc {rate}% (excl. structural {structural:,} T0: ~{adjusted:.0f}%)")
            else:
                issues.append(f"desc {rate}%")
    # 2. Pool health: catastrophic FLOOR + day-over-day RATE anomaly (AGG-ASPECT-POOLRANGE-STALE-1).
    # The absolute ceiling (20K-50K -> 35K-65K) was recalibrated twice in 6 weeks and re-fired
    # RED on every legitimate growth event (WD recovery, tenant waves, preservation fixes —
    # pool 81K on 2026-08-15, verified clean: 0 dup IDs/URLs, dup-content <5%). Legitimate
    # growth is not a defect. What IS a defect: catastrophic loss (floor) and anomalous
    # single-day swings (rate, aligns with contract P-5 'drops >20% warrant investigation';
    # 40% chosen so routine growth + oscillation stays quiet while real anomalies surface).
    pool = m.get("pool",{}).get("total_jobs", 0)
    if pool and pool < 20000:
        issues.append(f"pool {pool:,} below catastrophic floor 20K")
    pool_trend = (m.get("trends", {}) or {}).get("pool_total") or []
    if pool and len(pool_trend) >= 2 and pool_trend[-2]:
        prev = pool_trend[-2]
        delta_pct = abs(pool - prev) / prev * 100
        if delta_pct > 40:
            issues.append(f"pool {pool:,} moved {delta_pct:.0f}% vs prior day ({prev:,}) — investigate")
    # 3. Data health from jobs-metadata (source contribution + freshness)
    jm = proxy_json("jobs-metadata.json")
    if jm:
        sj = jm.get("source_journey", {})
        carry_only = sum(v.get("final",0) for v in sj.values() if isinstance(v,dict) and v.get("fetched",0)==0)
        total = jm.get("total_jobs", 1)
        carry_pct = carry_only / total * 100 if total else 0
        if carry_pct > 70: issues.append(f"{carry_pct:.0f}% carry-forward (sources stalled)")
        fh = jm.get("fetcher_health", {})
        errors = sum(1 for v in fh.values() if isinstance(v,dict) and v.get("status")=="error")
        if errors > 200: issues.append(f"{errors} fetcher errors")
        lc = jm.get("lifecycle",{}).get("distribution",{})
        aging = lc.get("evergreen",0)
        aging_pct = aging / total * 100 if total else 0
        if aging_pct > 60: issues.append(f"{aging_pct:.0f}% aging (>10d)")
    # Determine status from issues
    pool_str = f"pool {pool:,}" if pool else "pool ?"
    if not issues:
        return "GREEN", f"{rate}% retrievable, {pool_str}, data healthy", "zjp-metrics+jobs-metadata"
    detail = "; ".join(issues[:3])
    severity = "RED" if any("out of range" in i or "stalled" in i for i in issues) else "YELLOW"
    return severity, f"{detail} — {pool_str}", "zjp-metrics+jobs-metadata"

def c_performance():
    m = proxy_json("zjp-metrics.json")
    if not m: return "RED", "metrics unreadable", "proxy:metrics"
    # AGG-PERF-METRIC-FIX-1: use pipeline_internal_minutes (sum of stage_timings from
    # index.js step instrumentation) instead of aggregator_runtime_minutes, which queries
    # ALL workflow runs (line 220 of generate-zjp-metrics.js) and picks up quick workflows
    # (gate checks ~10s, aspect refreshes ~20s) as the aggregator's runtime.
    rt = m.get("pipeline",{}).get("pipeline_internal_minutes")
    if rt is None: return "YELLOW", "runtime missing", "zjp-metrics.pipeline_internal"
    s = bucket_low(rt, 5, 8)
    return s, f"{rt:.1f} min (script internal) — target <5, alert <8", "zjp-metrics.pipeline_internal"

def c_infrastructure():
    m = proxy_json("zjp-metrics.json")
    if not m: return "RED", "metrics unreadable", "proxy:metrics"
    r2 = m.get("r2",{})
    st = r2.get("status","unknown"); age = r2.get("manifest_age_minutes",9999)
    if st == "healthy" and age <= 60: return "GREEN", f"R2 {st}, {age}min old", "zjp-metrics.r2"
    elif st == "healthy": return "YELLOW", f"R2 {st} but {age}min old", "zjp-metrics.r2"
    else: return "RED", f"R2 {st}", "zjp-metrics.r2"

def c_security():
    crit = 0; total = 0
    for repo in AGG_REPOS:
        alerts = gh_json(["api", f"repos/{repo}/dependabot/alerts?state=open&per_page=50"]) or []
        total += len(alerts)
        crit += sum(1 for a in alerts if a.get("security_advisory",{}).get("severity","") in ("critical","high"))
    if crit == 0: return "GREEN", f"{total} open, 0 critical/high", "gh-api:dependabot"
    elif crit <= 2: return "YELLOW", f"{total} open, {crit} critical/high", "gh-api:dependabot"
    else: return "RED", f"{total} open, {crit} critical/high", "gh-api:dependabot"

def c_configuration():
    # CI-native: core pipeline files present in repo (gh-api)
    # company-list.json is git-crypt encrypted → 404 on gh-api; index.js is sufficient proxy
    idx = gh_json(["api", f"repos/{AGG_REPO}/contents/.github/scripts/index.js"])
    return green_if(idx is not None), f"index.js {'present' if idx else 'MISSING'} (company-list.json git-crypt encrypted, not API-visible)", "gh-api:config"

def c_discoverability():
    # CI-native: repo structure discoverable (workflows + fetchers present)
    wf = gh_json(["api", f"repos/{AGG_REPO}/contents/.github/workflows"]) or []
    fetchers = gh_json(["api", "repos/zapplyjobs/job-board-aggregator/contents/lib/fetchers"]) or []
    wf_count = len(wf) if isinstance(wf, list) else 0
    fx_count = len(fetchers) if isinstance(fetchers, list) else 0
    ok = wf_count >= 5 and fx_count >= 10
    return green_if(ok), f"{wf_count} workflows, {fx_count} fetchers — structure proxy", "gh-api:catalog"

def c_documentation():
    # CI-native: recent development activity (commit freshness proxy)
    import datetime
    commits = gh_json(["api", f"repos/{AGG_REPO}/commits?per_page=1"]) or []
    if not commits: return "RED", "no commits found", "gh-api:commits"
    commit_date = commits[0].get("commit",{}).get("committer",{}).get("date","")
    if not commit_date: return "YELLOW", "commit date missing", "gh-api:commits"
    try:
        dt = datetime.datetime.fromisoformat(commit_date.replace("Z","+00:00"))
        age_d = (NOW - dt).total_seconds() / 86400
    except: return "YELLOW", "commit date parse error", "gh-api:commits"
    status = bucket_low(age_d, 14, 30)
    return status, f"last commit {age_d:.0f}d ago (freshness proxy)", "gh-api:doc-freshness"

def c_change_mgmt():
    # CI-native: CI gate enforces pre-merge checks (gate.yml exists)
    gate = gh_json(["api", f"repos/{AGG_REPO}/contents/.github/workflows/gate.yml"])
    return green_if(gate is not None), f"gate.yml {'present' if gate else 'MISSING'} — SDLC enforcement proxy", "gh-api:sdlc"

CHECKS = {
    "verification": c_verification,
    "monitoring": c_monitoring,
    "data_quality": c_data_quality,
    "performance": c_performance,
    "infrastructure": c_infrastructure,
    "security": c_security,
    "configuration": c_configuration,
    "discoverability": c_discoverability,
    "documentation": c_documentation,
    "change_mgmt": c_change_mgmt,
}

result = {"module": "AGG", "generated_at": NOW.isoformat(), "aspects": {}}
for name, fn in CHECKS.items():
    try:
        s, e, src = fn()
    except Exception as ex:
        s, e, src = "RED", f"check error: {ex}", "verifier-error"
    result["aspects"][name] = {"status": s, "evidence": e, "source": src}

data_str = json.dumps(result, indent=2)
print(data_str)
counts = {}
for a in result["aspects"].values():
    counts[a["status"]] = counts.get(a["status"], 0) + 1
print(f"\n=== AGG aspect-status (CI) === {counts.get('GREEN',0)}G / {counts.get('YELLOW',0)}Y / {counts.get('RED',0)}R ({len(result['aspects'])} signal checks; all 10 aspects CI-native)", file=sys.stderr)

if "--publish" in sys.argv:
    import boto3
    s3 = boto3.client("s3", region_name="auto",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    s3.put_object(Bucket=os.environ["R2_BUCKET_NAME"],
                  Key="data/agg-aspect-status.json", Body=data_str, ContentType="application/json")
    print("published R2: data/agg-aspect-status.json", file=sys.stderr)
