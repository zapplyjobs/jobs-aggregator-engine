#!/usr/bin/env python3
"""warden_queue.py - Warden kit queue builder + alarm filer (Desk 3: HEALTH). Reads the R2
yield-decay monitor (~/.secrets/r2-zjp.env, bucket zjp-data), merges the zero-output list
(flag company_missing_from_final_output), emits the ranked triage queue (oldest-first,
owner-tagged) as JSON artifact + readable table. Hard gates: monitor schema_version==2,
expected flag key present, summary count == ingested rows, monitor <=48h old (ages computed
against monitor generated_at), no null-age rows. Alarms: SUP-owned companies zero-output
>7d or queue>50 -> file/update todo task SUP-WARDEN-ALARM-1 (open rows only, dated note
carrying the ages, never duplicates). The kit files alarms; the lead adjudicates.
Usage: build | alarms [--dry-run] | --self-test (in-memory fixture, zero network, writes only /tmp)."""
import argparse, hashlib, json, os, re, statistics, subprocess, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
ARTIFACT = REPO / "projects/zjp/state/generated/warden_triage_queue.json"
TODO_CLI = REPO / "projects/aih/scripts/ops/todo-cli.py"
SECRETS = Path.home() / ".secrets/r2-zjp.env"
MONITOR_KEY = "data/sup_yield_decay_monitor.json"
ALARM_AGE, ALARM_QUEUE, ALARM_TASK = 7, 50, "SUP-WARDEN-ALARM-1"  # desk card triggers
ZERO_FLAG = "company_missing_from_final_output"
RUNBOOKS = {  # per-verdict-class action line, carried on alarm/queue rows (alert-action contract)
    "LIVE-UNFETCHED": "route AGG-SUPPLYROT-LAND-1",
    "MOVED": "re-point via supply-change-preflight",
    "EMPTY": "retire with board evidence",
    "GONE": "retire with board evidence + custom-door pass first",
    "UNPROBED-RETRY": "re-probe next build",
    "UNPROBED-FAMILY": "custom-door method (sup-source-research kit)",
}
OPEN_ROW_RE = re.compile(r"\[zjp\] (?:open|progress) \*\*(SUP-WARDEN-ALARM-\d+)\*\*")  # done rows never re-file
SILENT_STATE = Path(os.environ.get("WARDEN_SILENT_STATE", str(REPO / "projects/zjp/state/generated/warden_silent_state.json")))
LIST_PATH = Path(os.environ.get("WARDEN_LIST_PATH", "/home/ahd/zjp-repos/job-board-aggregator/lib/fetchers/company-list.json"))
SECRETS = Path(os.environ.get("WARDEN_SECRETS", str(Path.home() / ".secrets/r2-zjp.env")))
ARTIFACT = Path(os.environ.get("WARDEN_ARTIFACT", str(REPO / "projects/zjp/state/generated/warden_triage_queue.json")))
POOL_KEY = "data/all_jobs.json"


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _workday_tokens(entry):
    """Workday pool keys derive from the tenant URL (tenant label + site path), not
    company_slug - the 2026-10-28 audit's Boeing/Intel false-zero lesson."""
    s = str(entry.get("slug") or "").lower()
    toks = set()
    if "://" in s:
        host = s.split("://")[1].split("/")[0]
        toks.add(host.split(".")[0])
        rest = s.split("://")[1].split("/", 1)
        if len(rest) > 1 and rest[1]:
            toks.add(rest[1].split("/")[0].lower())
    elif ":" in s:
        toks |= {t for t in s.replace("|", " ").replace(":", " ").split()}
    else:
        toks.add(s)
    return {t for t in toks if t}


def entry_tokens(entry):
    """Pool-presence tokens for one company-list entry (exact + substring probes)."""
    fam = str(entry.get("ats") or "").lower()
    slug = str(entry.get("slug") or "").lower()
    toks = {slug}
    if fam == "workday":
        toks |= _workday_tokens(dict(entry, slug=slug))
    nm = _norm(entry.get("company"))
    if nm:
        toks.add(nm)
    toks.discard("")
    return toks


def list_entry_present(tokens, family, slug_sets, names):
    """(present, basis): any token hits the family's pool slug set (exact or substring)
    -> basis 'slug'; else the normalized company name appears under ANY source
    (custom/dedicated fetchers serve by company name - the Sift/Kohl's drift class)
    -> basis 'name'. basis lets the queue audit how each row was matched."""
    fam_slugs = slug_sets.get(family, set())
    for t in tokens:
        if t in fam_slugs or any(t in s for s in fam_slugs):
            return True, "slug"
    if any(t in names for t in tokens):
        return True, "name"
    return False, ""


def load_r2_env(path=SECRETS):
    env = dict((m.group(1), m.group(2).strip('"\'')) for m in
               (re.match(r"export (\w+)=(.*)", l.strip()) for l in path.read_text().splitlines()) if m)
    missing = {"R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET_NAME"} - set(env)
    if missing:
        sys.exit(f"ERROR: {path} missing {sorted(missing)}")
    return env


def monitor_time(monitor):
    return datetime.fromisoformat(str(monitor["generated_at"]).replace("Z", "+00:00"))


def check_monitor(monitor, now=None):
    """Hard-fail on schema drift or a stale monitor (false-green guard)."""
    if monitor.get("schema_version") != 2:
        sys.exit(f"ERROR: monitor schema_version={monitor.get('schema_version')!r}, kit expects 2 - "
                 f"inspect {MONITOR_KEY} before triage")
    if ZERO_FLAG not in (monitor.get("summary", {}).get("by_flag") or {}):
        sys.exit(f"ERROR: expected flag '{ZERO_FLAG}' missing from summary.by_flag - monitor schema drift")
    now = now or datetime.now(timezone.utc)
    h = (now - monitor_time(monitor)).total_seconds() / 3600
    if not -1 <= h <= 48:
        sys.exit(f"ERROR: monitor is {h:.0f}h old (generated {monitor.get('generated_at')}) - rebuild the "
                 f"monitor before triage; ages would inflate (now={now.isoformat()})")


def build_rows(monitor):
    """Zero-output companies, oldest-first (tie: bigger lost baseline), owner-tagged.
    Ages are computed against the monitor's generated_at, never local now."""
    now = monitor_time(monitor)
    rows = [{"key": c["key"], "company": c["company"], "ats": c["ats"], "slug": c["slug"],
             "owner": c.get("likely_owner", "?"), "age_days": None if not c.get("first_flagged") else
                 (now - datetime.fromisoformat(str(c["first_flagged"]).replace("Z", "+00:00"))).days,
             "first_flagged": c.get("first_flagged"), "flags": c.get("flags", []),
             "review_bucket": c.get("review_bucket"),
             "baseline_tech_us": (c.get("baseline") or {}).get("tech_us_count"),
             "current_final": (c.get("current") or {}).get("final_pool_total"),
             "review_reason": (c.get("review_reason") or "")[:140]}
            for c in monitor.get("flagged_companies", []) if ZERO_FLAG in c.get("flags", [])]
    rows.sort(key=lambda r: (-(r["age_days"] if r["age_days"] is not None else -1),
                             -(r["baseline_tech_us"] or 0), r["key"]))
    return rows


def require_ages(rows):
    unknown = [r["key"] for r in rows if r["age_days"] is None]
    if unknown:
        sys.exit(f"ERROR: {len(unknown)} zero-output rows carry no first_flagged (e.g. {unknown[:5]}) - "
                 "age unknown; re-baseline those monitor rows before triage (never rank silently)")


def render_table(rows):
    out = [f"{'#':>3}  {'age_d':>5}  {'owner':<8} {'ats':<14} {'key':<26} {'slug':<28} {'baseline':>8}", "-" * 100]
    for i, r in enumerate(rows, 1):
        out.append(f"{i:>3}  {r['age_days']:>5}  {r['owner']:<8} {r['ats']:<14} {r['key']:<26} "
                   f"{str(r['slug'])[:28]:<28} {str(r['baseline_tech_us']):>8}")
    return "\n".join(out)


def _atomic_write(path, text):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def write_artifact(rows, source, monitor_generated_at, path=None, sources=None):
    path = path or ARTIFACT
    over = [r for r in rows if r["age_days"] > ALARM_AGE]
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source": source,
           "monitor_generated_at": monitor_generated_at,
           "sources": sources or {"monitor": len(rows)},
           "definition": f"zero-output = flag {ZERO_FLAG} (monitor) + silent list zeros (list); ranked oldest-first; owner-tagged; ages vs monitor time / first-observation",
           "count": len(rows), "oldest_age_days": max((r["age_days"] for r in rows), default=0),
           "median_age_days": round(statistics.median([r["age_days"] for r in rows]), 1) if rows else 0,
           "median_note": "kill metric (desk card): median flag age at verdict <14d/quarter - emitted per build; the lead reads the trend",
           "alarm": {"sup_companies_over_7d": sum(1 for r in over if r["owner"].split("/")[0] == "SUP"),
                     "queue_over_50": len(rows) > ALARM_QUEUE}, "rows": rows}
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(path, json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    return doc


def read_back():
    data = ARTIFACT.read_bytes()
    doc = json.loads(data)
    print(f"READ-BACK {ARTIFACT.relative_to(REPO)}: {len(data)} bytes, sha256:{hashlib.sha256(data).hexdigest()[:12]}, "
          f"{doc['count']} rows, oldest {doc['oldest_age_days']}d, median {doc.get('median_age_days')}d, alarm={doc['alarm']}")
    return doc


def run_todo(*args):
    r = subprocess.run([sys.executable, str(TODO_CLI), *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"ERROR: todo-cli {' '.join(args[:2])} rc={r.returncode}\n{r.stderr[-800:]}")


def todo_search(query):
    return subprocess.run([sys.executable, str(TODO_CLI), "search", query], capture_output=True, text=True).stdout


def find_alarm_task():
    """Duplicate guard: only OPEN/PROGRESS rows count - done/archived rows never re-file."""
    m = [OPEN_ROW_RE.search(l) for l in todo_search("SUP-WARDEN-ALARM").splitlines()]
    m = [x for x in m if x]
    return m[0].group(1) if m else None


def alarm_note(doc):
    """Dated note with the ages; SUP-owned rows alarm, other owners are counted, not alarmed."""
    over_all = sorted((r for r in doc["rows"] if r["age_days"] > ALARM_AGE), key=lambda r: -r["age_days"])
    over = [r for r in over_all if r["owner"].split("/")[0] == "SUP"]
    others = len(over_all) - len(over)
    q = "queue>50 YES" if doc["alarm"]["queue_over_50"] else f"queue={doc['count']}"
    keys = [f"{r['key']} {r['age_days']}d" for r in over]
    while True:
        top = ", ".join(keys[:8])
        more = f" (+{len(keys) - 8} more in artifact)" if len(keys) > 8 else ""
        extra = f" [+{others} non-SUP (AGG-routed), not alarmed here]" if others else ""
        note = (f"[{doc['generated_at'][:10]}] Warden alarm: {len(over)} SUP-owned zero-output co >{ALARM_AGE}d "
                f"(oldest: {top}); {q}. Evidence basis: projects/zjp/state/generated/{ARTIFACT.name}. "
                f"Kit files alarms; lead judges.{more}{extra}")
        if len(note) <= 400 or len(keys) <= 1:
            return note, over, others
        keys = keys[:-1]  # NOTELEN advisory (B145): notes are read-queue, trim the key list not the pointer


def cmd_alarms(dry_run=False):
    doc = None
    if ARTIFACT.exists():
        doc = json.loads(ARTIFACT.read_text())
        if (datetime.now(timezone.utc) - datetime.fromisoformat(doc["generated_at"])).total_seconds() / 3600 > 48:
            doc = None  # stale artifact: rebuild (monitor gates re-run) rather than alarm on it
    if doc is None:
        doc = cmd_build(quiet=True)
    note, over, others = alarm_note(doc)
    suffix = " (DRY-RUN - nothing filed)" if dry_run else ""
    if others:
        print(f"NOTE: {others} non-SUP rotting row(s) reported, not alarmed (owner-routed)")
    if not over and not doc["alarm"]["queue_over_50"]:
        print(f"no alarm: oldest {doc['oldest_age_days']}d (trigger >{ALARM_AGE}d, SUP-owned), "
              f"{doc['count']} rows (trigger >{ALARM_QUEUE}){suffix}")
        return 0
    existing = find_alarm_task()
    if existing:
        print(f"ALARM update {existing}{suffix}")
        if not dry_run:
            run_todo("update", "zjp", existing, "--note", note)
    else:
        print(f"ALARM file {ALARM_TASK}{suffix}")
        if not dry_run:
            run_todo("add", "zjp", "SUP", ALARM_TASK, "--description",
                     "Warden triage alarm: zero-output companies rotting >7d / queue>50 - lead triage owed (desk card Desk 3: HEALTH)",
                     "--group", "1")
            run_todo("update", "zjp", ALARM_TASK, "--note", note)
    print(f"note: {note}")
    print("READ-BACK todo-cli search SUP-WARDEN-ALARM:")
    print("\n".join(l for l in todo_search("SUP-WARDEN-ALARM").splitlines() if OPEN_ROW_RE.search(l) or "[zjp] done" in l))
    return 0


def cmd_build(quiet=False):
    monitor, source = fetch_monitor()
    rows = build_rows(monitor)
    nb = monitor["summary"]["by_flag"][ZERO_FLAG]
    if nb != len(rows):
        sys.exit(f"ERROR: summary.by_flag['{ZERO_FLAG}']={nb} != {len(rows)} ingested zero-output rows - "
                 "schema drift; inspect the monitor before triage")
    require_ages(rows)
    env = load_r2_env()
    now = datetime.now(timezone.utc)
    list_rows, list_zero = fetch_list_universe(now, env)
    merged = merge_rows(rows, list_rows)
    collisions = len(rows) + len(list_rows) - len(merged)
    if collisions < 0 or list_zero != len(list_rows):
        sys.exit(f"ERROR: list-universe merge arithmetic drifted (computed {list_zero}, kept {len(list_rows)}, "
                 f"collisions {collisions}) - inspect warden_silent_state.json")
    sources = {"monitor": len(rows), "list_silent": len(list_rows) - collisions,
               "collisions_monitor_won": collisions}
    doc = write_artifact(merged, source, monitor.get("generated_at"), sources=sources)
    if not quiet:
        print(f"universes: monitor={len(rows)} list_silent={sources['list_silent']} "
              f"collisions={collisions} merged={len(merged)}")
        print(render_table(doc["rows"]))
        print()
        read_back()
        print("next: python3 projects/zjp/kits/warden/warden_packet.py packet <key>   |   "
              "python3 projects/zjp/kits/warden/warden_queue.py alarms --dry-run")
    return doc


def fetch_monitor():
    """R2 monitor read + gates. Returns (monitor_dict, source_label)."""
    import boto3
    env = load_r2_env()
    s3 = boto3.client("s3", endpoint_url=env["R2_ENDPOINT"], aws_access_key_id=env["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"])
    obj = s3.get_object(Bucket=env["R2_BUCKET_NAME"], Key=MONITOR_KEY)
    monitor = json.loads(obj["Body"].read())
    check_monitor(monitor)
    return monitor, f"r2:{env['R2_BUCKET_NAME']}/{MONITOR_KEY}"


def fetch_pool_index(env):
    """(slug_sets per source, normalized company-name set) streamed from all_jobs.json."""
    import boto3
    s3 = boto3.client("s3", region_name="auto", endpoint_url=env["R2_ENDPOINT"],
                      aws_access_key_id=env["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"])
    obj = s3.get_object(Bucket=env["R2_BUCKET_NAME"], Key=POOL_KEY)
    slugs, names = {}, set()
    for raw in obj["Body"].iter_lines():
        if not raw:
            continue
        try:
            r = json.loads(raw)
        except Exception:
            continue
        src = str(r.get("source") or "").lower()
        slugs.setdefault(src, set()).add(str(r.get("company_slug") or "").lower())
        nm = _norm(r.get("company_name"))
        if nm:
            names.add(nm)
    return slugs, names


def fetch_list_universe(now, env):
    """Second universe: every company-list entry (aggregator clone) with ZERO pool
    presence, whether or not the decay monitor ever flagged it (the silent class).
    first_seen persisted per key in warden_silent_state.json - first observation of a
    silent zero starts its age TODAY (honest: no older age is knowable without a
    re-baseline). Returns (rows, computed_zero_count)."""
    cl = json.loads(LIST_PATH.read_text(encoding="utf-8"))
    entries = []
    for fam, arr in cl.items():
        if fam.startswith("_") or not isinstance(arr, list):
            continue
        for e in arr:
            if isinstance(e, dict) and e.get("name"):
                entries.append({"ats": fam, "slug": str(e.get("slug") or fam), "company": e["name"]})
    slug_sets, names = fetch_pool_index(env)
    state = json.loads(SILENT_STATE.read_text()) if SILENT_STATE.exists() else {}
    rows = []
    for e in entries:
        toks = entry_tokens(e)
        present, basis = list_entry_present(toks, e["ats"], slug_sets, names)
        if present:
            state.pop(f"{e['ats']}/{e['slug']}", None)
            continue
        key = f"{e['ats']}/{e['slug']}"
        first = state.get(key)
        if not first:
            first = now.isoformat()
            state[key] = first
        age = max((now - datetime.fromisoformat(first)).days, 0)
        rows.append({"key": key, "company": e["company"], "ats": e["ats"], "slug": e["slug"],
                     "owner": "SUP", "source": "list", "flags": ["silent_zero_list"],
                     "match_basis": basis,
                     "age_days": age, "first_flagged": first, "review_bucket": "silent_zero_list",
                     "baseline_tech_us": None, "current_final": 0,
                     "review_reason": "list entry with zero pool rows (outside monitor universe)"[:140]})
    SILENT_STATE.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(SILENT_STATE, json.dumps(state, indent=1, sort_keys=True) + "\n")
    return rows, len(rows)


def merge_rows(monitor_rows, list_rows):
    """Monitor wins on exact KEY collision; list rows fill the silent universe.
    Company-name collisions no longer drop the list row (a rename or shared name
    must not delete a real zero - F320 adversary finding). Sort: oldest-first,
    tie bigger lost baseline (same as build_rows)."""
    seen_keys = {r["key"] for r in monitor_rows}
    merged = list(monitor_rows) + [r for r in list_rows if r["key"] not in seen_keys]
    merged.sort(key=lambda r: (-(r["age_days"] if r["age_days"] is not None else -1),
                               -(r["baseline_tech_us"] or 0), r["key"]))
    return merged


def self_test():
    """In-memory fixture: gates, ages-vs-monitor-time, ranking, SUP-owned alarm split,
    open-row-only regex, atomic write. Zero network (no R2/todo-cli), writes only /tmp."""
    mon_now = datetime(2026, 10, 28, 6, 23, 37, tzinfo=timezone.utc)

    def flag(key, days, base=100, owner="SUP"):
        ff = None if days is None else (mon_now - timedelta(days=days)).isoformat().replace("+00:00", "Z")
        return {"key": key, "company": key.title(), "ats": "lever", "slug": key, "flags": [ZERO_FLAG],
                "likely_owner": owner, "review_bucket": "final_output_missing_review", "first_flagged": ff,
                "baseline": {"tech_us_count": base}, "current": {"final_pool_total": 0}, "review_reason": "fixture"}

    monitor = {"schema_version": 2, "generated_at": "2026-10-28T06:23:37Z", "summary": {"by_flag": {ZERO_FLAG: 5}},
               "flagged_companies": [flag("selftest-old-a", 54), flag("selftest-mid-b", 3),
                                     flag("selftest-new-c", 0), flag("selftest-agg-d", 20, 50, owner="AGG"),
                                     flag("selftest-e", 10)]}
    tmp = Path("/tmp/warden_queue_selftest.json")
    check_monitor(monitor)
    for bad in [dict(monitor, schema_version=3), dict(monitor, generated_at="2026-10-01T00:00:00Z"),
                {**monitor, "summary": {"by_flag": {"other_flag": 1}}}]:
        try:
            check_monitor(bad)
            raise AssertionError(f"gate missed: {bad.get('schema_version')} {bad.get('generated_at')}")
        except SystemExit:
            pass
    rows = build_rows(monitor)
    assert [r["key"] for r in rows[:3]] == ["selftest-old-a", "selftest-agg-d", "selftest-e"], rows
    assert rows[0]["age_days"] == 54 and rows[2]["age_days"] == 10  # ages vs monitor time
    assert monitor["summary"]["by_flag"][ZERO_FLAG] == len(rows)
    try:
        require_ages(rows + [{"key": "x", "age_days": None}])
        raise AssertionError("null-age gate silent")
    except SystemExit:
        pass
    doc = write_artifact(rows, "fixture", monitor["generated_at"], path=tmp)
    assert doc["count"] == 5 and doc["oldest_age_days"] == 54 and doc["median_age_days"] == 10  # kill metric emitted
    assert doc["alarm"] == {"sup_companies_over_7d": 2, "queue_over_50": False}, doc["alarm"]  # AGG row not counted
    note, over, others = alarm_note(doc)
    assert len(over) == 2 and others == 1 and "selftest-old-a 54d" in note and "non-SUP" in note
    assert note.startswith(f"[{datetime.now(timezone.utc).date().isoformat()}]") and len(note) <= 400, note  # clock-independent: fixture dates differ from publish date
    big = write_artifact(rows + [{"key": f"f-{i}", "age_days": 1} for i in range(60)], "f", "x", path=tmp)
    assert big["alarm"]["queue_over_50"] is True
    assert json.loads(tmp.read_text())["source"] == "f"  # atomic write readable
    assert not ARTIFACT.exists() or json.loads(ARTIFACT.read_text())["source"] != "fixture", "self-test touched the real artifact"
    assert OPEN_ROW_RE.search("[zjp] open **SUP-WARDEN-ALARM-1**: x").group(1) == "SUP-WARDEN-ALARM-1"
    assert OPEN_ROW_RE.search("[zjp] progress **SUP-WARDEN-ALARM-2**: x")
    assert not OPEN_ROW_RE.search("[zjp] done (archived:sup_todo_x) **SUP-WARDEN-ALARM-1**: x")
    assert "selftest-old-a" in render_table(rows) and render_table(rows).count("\n") >= 6

    # F320 Unit D: monitor/list merge + silent-universe presence matching.
    list_rows = [
        {"key": "ashby/warp", "company": "Warp", "ats": "ashby", "slug": "warp", "owner": "SUP",
         "source": "list", "age_days": 0, "first_flagged": mon_now.isoformat(), "baseline_tech_us": None},
        {"key": "greenhouse/merge", "company": "Merge", "ats": "greenhouse", "slug": "merge", "owner": "SUP",
         "source": "list", "age_days": 2, "first_flagged": (mon_now - timedelta(days=2)).isoformat(),
         "baseline_tech_us": None},
    ]
    merged = merge_rows(rows, list_rows)
    assert len(merged) == 7 and sum(1 for r in merged if r.get("source") == "list") == 2  # no collision in fixture
    # F320 v2: dedupe is by KEY, not company name - a shared company name must NOT delete a real list zero.
    same_name = merge_rows(rows, [dict(list_rows[0], key="lever/selftest-old-a", company="Selftest-Old-A")])
    assert len(same_name) == 6 and sum(1 for r in same_name if r.get("source") == "list") == 1 and \
        any(r["company"] == "Selftest-Old-A" for r in same_name)  # name collision no longer drops the list row (key dedupe)
    same_key = merge_rows(rows, [dict(list_rows[0], key="lever/selftest-old-a"), dict(list_rows[1], key="selftest-old-a")])
    assert len(same_key) == 6 and not any(r.get("source") == "list" and r["key"] == "selftest-old-a" for r in same_key)  # exact key: monitor wins
    assert merged[0]["age_days"] == 54  # oldest-first preserved across sources
    slug_sets = {"ashby": {"warp-dev", "resend"}, "workday": {"nvidia.wd5"}}
    names = {"nvidia"}
    assert list_entry_present({"warp"}, "ashby", slug_sets, names) == (True, "slug")
    assert list_entry_present({"warp"}, "lever", slug_sets, names) == (False, "")
    assert list_entry_present({"confluent"}, "ashby", slug_sets, names) == (False, "")  # true zero stays
    assert list_entry_present(_workday_tokens({"slug": "https://nvidia.wd5.myworkdayjobs.com"}), "workday", slug_sets, names) == (True, "slug")
    assert list_entry_present({"nvidia"}, "workday", slug_sets, names) == (True, "slug")  # substring slug match
    assert list_entry_present({"zapply-labs"}, "lever", {}, {"zapply-labs"}) == (True, "name")  # name fallback is auditable
    assert RUNBOOKS and all(RUNBOOKS.get(v) for v in ("LIVE-UNFETCHED", "MOVED", "EMPTY", "GONE", "UNPROBED-RETRY"))  # alert-action contract

    tmp.unlink()
    print("warden_queue self-test: PASS (schema/freshness/flag-key gates refuse, ages vs monitor time, "
          "rank oldest-first, SUP-owned alarm split, null-age hard-fail, queue>50 trip, dated note <=400 "
          "chars, open-row-only dup guard, atomic write, monitor/list merge + collision monitor-wins, "
          "silent-universe presence matching; fixture-only, zero network, real artifact untouched)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", choices=["build", "alarms"])
    ap.add_argument("--dry-run", action="store_true", help="alarms: show what would file/update, write nothing")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    if a.cmd == "alarms":
        return cmd_alarms(dry_run=a.dry_run)
    if a.cmd == "build":
        cmd_build()
        return 0
    ap.print_usage()
    return 1


if __name__ == "__main__":
    sys.exit(main())
