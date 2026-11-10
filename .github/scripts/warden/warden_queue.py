#!/usr/bin/env python3
"""warden_queue.py - Warden kit queue builder + alarm filer (Desk 3: HEALTH). Reads the R2
yield-decay monitor (~/.secrets/r2-zjp.env, bucket zjp-data), merges the zero-output list
(flag company_missing_from_final_output), emits the ranked triage queue (oldest-first,
owner-tagged) as JSON artifact + readable table. Hard gates: monitor schema_version==2,
expected flag key present, summary count == ingested rows, monitor <=48h old (ages computed
against monitor generated_at), no null-age rows. Alarms: SUP-owned companies zero-output
>7d or queue>50 -> file/update todo task SUP-WARDEN-ALARM-1 (open rows only, dated note
carrying the ages, never duplicates). The kit files alarms; the lead adjudicates.
Usage: build | alarms [--dry-run] | disposition <key> <YYYY-MM-DD> [--class C] [--routed ROW] [--receipt ID] [--reopen] [--dry-run] | --self-test (in-memory fixture, zero network, writes only /tmp)."""
import argparse, collections, csv, hashlib, json, os, re, statistics, subprocess, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
TODO_CLI = REPO / "projects/aih/scripts/ops/todo-cli.py"
MONITOR_KEY = "data/sup_yield_decay_monitor.json"
ALARM_AGE, ALARM_QUEUE, ALARM_TASK = 7, 50, "SUP-WARDEN-ALARM-1"  # desk card triggers
ZERO_FLAG = "company_missing_from_final_output"
RUNBOOKS = {  # per-verdict-class action line, carried on alarm/queue rows (alert-action contract)
    "LIVE-UNFETCHED": "route AGG-SUPPLYROT-LAND-1",
    "MOVED": "re-point via supply-change-preflight",
    "MOVED-CANDIDATE": "lead judges - repoint via supply-change-preflight (variant-slug hit)",
    "STALE-DATED-CANDIDATE": "lead dated-probe vs the 14d anchor; pass negative -> stale-dated stands (reopen: fresh dated postings)",
    "NEW-TENANT-GRACE": "zero expected until first fresh posting; re-probe admission+30d; class is latency, never fetch-failure",
    "EMPTY": "retire with board evidence",
    "GONE": "retire with board evidence + custom-door pass first; if relocation_check.truncated, re-probe the unsearched families first",
    "UNPROBED-RETRY": "re-probe next build",
    "UNPROBED-FAMILY": "custom-door method (sup-source-research kit)",
}
OPEN_ROW_RE = re.compile(r"\[zjp\] (?:open|progress) \*\*(SUP-WARDEN-ALARM-\d+)\*\*")  # done rows never re-file
SILENT_STATE = Path(os.environ.get("WARDEN_SILENT_STATE", str(REPO / "projects/zjp/state/generated/warden_silent_state.json")))
LIST_PATH = Path(os.environ.get("WARDEN_LIST_PATH", "/home/ahd/zjp-repos/job-board-aggregator/lib/fetchers/company-list.json"))
SECRETS = Path(os.environ.get("WARDEN_SECRETS", str(Path.home() / ".secrets/r2-zjp.env")))
ARTIFACT = Path(os.environ.get("WARDEN_ARTIFACT", str(REPO / "projects/zjp/state/generated/warden_triage_queue.json")))
CSV_PATH = Path(os.environ.get("WARDEN_CSV_PATH", str(REPO / "projects/zjp/company-research-log.csv")))
POOL_KEY = "data/all_jobs.json"
NOTES = HERE / "notes.md"


def read_notes():
    """Read the kit's accumulated notes. Returns the file content as a string.
    Empty string if the file doesn't exist yet (first run)."""
    if NOTES.exists():
        return NOTES.read_text(encoding="utf-8")
    return ""


def append_note(text):
    """Append a dated learning to the kit's notes file.
    The kit agent calls this after each run with what it learned."""
    NOTES.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    with NOTES.open("a", encoding="utf-8") as f:
        f.write(f"\n## {stamp}\n{text}\n")


def retire_source(key, evidence):
    """Domain-internal judgment: retire a source confirmed empty or gone.
    Reversible: the row stays in the CSV (status=rejected), the company-list row
    stays (fetch:false), and the evidence is recorded. The lead can reverse this
    by re-enabling fetch and clearing the status. This is NOT an irreversible call."""
    return {"action": "retire", "key": key, "evidence": evidence,
            "reversible": True, "csv_status": "rejected", "company_list_fetch": False}


def repoint_source(key, new_ats, new_slug, evidence):
    """Domain-internal judgment: re-point a source that moved platforms.
    Reversible: the old config is kept in CSV notes, the new config is additive,
    and the evidence is recorded. The lead can reverse by restoring the old slug.
    This is NOT an irreversible call."""
    return {"action": "repoint", "key": key, "new_ats": new_ats, "new_slug": new_slug,
            "evidence": evidence, "reversible": True}





def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _today():
    return datetime.now(timezone.utc).date().isoformat()


def _carry_lookup(state):
    """Disposition lookup over the `@carry:` namespace (WardenMechanism2 design,
    F325; namespace form so legacy CI builders round-trip the state opaquely -
    object-valued entry keys would crash the vendored old builder). Exact
    resolved key, else unique normalized slug-suffix twin (monitor rows key
    bare `phia`; state keys `ashby/phia` - same company, two universes, one
    disposition). Ambiguous twins return None (visible, never silently suppressed)."""
    exact, suffix = {}, {}
    for k, v in state.items():
        if k.startswith("@carry:") and isinstance(v, dict) and v.get("carry_until"):
            rk = k[len("@carry:"):]
            exact[rk] = v
            suffix.setdefault(_norm(rk.rsplit("/", 1)[-1]), []).append(rk)

    def lookup(key):
        rec = exact.get(key)
        if rec:
            return rec
        hits = suffix.get(_norm(key), [])
        return state["@carry:" + hits[0]] if len(hits) == 1 else None
    return lookup


def apply_disposition(state, key, until, dclass=None, routed=None, receipt=None, reopen=False):
    """Pure state mutation for one disposition (F325). Resolves bare keys to
    list-universe twins; stores the record under the `@carry:<resolved>` key
    (legacy entry values untouched - CI-safe); `reopen` deletes the record
    (row re-fires when its age crosses the bar again). Returns
    (resolved_key, action)."""
    resolved = key if key in state else None
    if resolved is None:
        norm = _norm(key)
        twins = [k for k in state
                 if not k.startswith("@carry:") and _norm(k.rsplit("/", 1)[-1]) == norm]
        if len(twins) > 1:
            sys.exit(f"ERROR: disposition key '{key}' ambiguously matches state twins {twins} - "
                     "disambiguate with the full ats/slug key")
        resolved = twins[0] if twins else key  # bare key: monitor-only row (e.g. sandisk)
    ckey = "@carry:" + resolved
    if reopen:
        if ckey in state:
            del state[ckey]
            return resolved, "reopened"
        return resolved, "no-op"
    state[ckey] = {"carry_until": until,
                   "disposition": dclass or "adjudicate-carried",
                   "set_at": _today(),
                   **({"routed": routed} if routed else {}),
                   **({"receipt": receipt} if receipt else {})}
    return resolved, "carried"


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
             "review_bucket": c.get("review_bucket"), "match_basis": c.get("match_basis"),
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


def write_artifact(rows, source, monitor_generated_at, path=None, sources=None, by_marker=None):
    path = path or ARTIFACT
    today = _today()
    # carried = future-dated carry only; an expired carry_until is DUE - the row re-fires.
    carried = [r for r in rows if (r.get("carried_until") or "") >= today]
    eligible = [r for r in rows if not ((r.get("carried_until") or "") >= today)]
    # The >7d alarm leg counts the MONITOR universe only (EngChallenger P1-2, F325:
    # ~175 permanent list_silent rows bulk-cross 7d on 11-05/06 and would drown the
    # alarm forever - the habituation the queue split exists to kill). List-silent
    # age is inventory, counted + routed, never alarmed.
    over = [r for r in eligible if r["age_days"] > ALARM_AGE and r.get("source") != "list"]
    list_silent_7d = sum(1 for r in eligible if r["age_days"] > ALARM_AGE and r.get("source") == "list")
    # queue trigger counts the monitor universe too - same habituation logic.
    # standing inventory (dead supplemental families), not session triage debt -
    # the F325 sensitivity read: 187/200 list rows held queue>50 permanently.
    monitor_eligible = [r for r in eligible if r.get("source") != "list"]
    list_silent = sum(1 for r in eligible if r.get("source") == "list")
    fragile = [r for r in over if (r.get("match_basis") or "none") in ("company_name", "none")]
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source": source,
           "monitor_generated_at": monitor_generated_at,
           "sources": sources or {"monitor": len(rows)},
           "definition": f"zero-output = flag {ZERO_FLAG} (monitor) + silent list zeros (list); ranked oldest-first; owner-tagged; ages vs monitor time / first-observation; carried rows suppressed from alarms until carry_until",
           "count": len(rows), "oldest_age_days": max((r["age_days"] for r in rows), default=0),
           "median_age_days": round(statistics.median([r["age_days"] for r in rows]), 1) if rows else 0,
           "median_note": "kill metric (desk card): median flag age at verdict <14d/quarter - emitted per build; the lead reads the trend",
           "carried": {"count": len(carried),
                       "next_due": min((r["carried_until"] for r in carried), default=None)},
           "by_match_basis": dict(sorted(collections.Counter(
               (r.get("match_basis") if r.get("match_basis") is not None else "none")
               for r in eligible).items(), key=lambda x: -x[1])),
           "by_marker": by_marker if by_marker is not None else {"list_marked": 0,
                       "monitor_marked_excluded": 0, "monitor_marked_excluded_keys": [],
                       "monitor_marked_with_baseline": []},
           "alarm": {"sup_companies_over_7d": sum(1 for r in over if r["owner"].split("/")[0] == "SUP"),
                     "identity_fragile_over_7d": len(fragile),
                     "list_silent_over_7d": list_silent_7d,
                     "queue_over_50": len(monitor_eligible) > ALARM_QUEUE,
                     "queue_monitor": len(monitor_eligible), "queue_list_silent": list_silent}, "rows": rows}
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
    """Dated note with the ages; SUP-owned rows alarm, other owners are counted, not
    alarmed. Carried rows (future carry_until) are suppressed from the counts and
    reported as a split; identity-fragile rows (match_basis company_name/none - the
    F325 sensitivity read) are counted separately; list-silent inventory is routed,
    not alarmed (NOTELEN B145: trim the key list, never the pointer)."""
    a = doc["alarm"]
    over_all = sorted((r for r in doc["rows"]
                       if r["age_days"] > ALARM_AGE
                       and not ((r.get("carried_until") or "") >= _today())
                       and r.get("source") != "list"),
                      key=lambda r: -r["age_days"])
    over = [r for r in over_all if r["owner"].split("/")[0] == "SUP"]
    others = len(over_all) - len(over)
    carried_n = doc.get("carried", {}).get("count", 0)
    fragile = a.get("identity_fragile_over_7d", 0)
    bm = doc.get("by_marker") or {}
    stale_n = len(bm.get("monitor_marked_with_baseline") or [])
    head = (f"[{doc['generated_at'][:10]}] Warden alarm: {len(over)} SUP-owned zero-output co >{ALARM_AGE}d, "
            f"{carried_n} carried (next due {doc.get('carried', {}).get('next_due')}), {fragile} identity-fragile "
            f"(name-matched - cutover pending); list_silent >7d={a.get('list_silent_over_7d', 0)} "
            f"(inventory, routed AGG-SILENTZEROS-APPLECAP-1); queue monitor={a.get('queue_monitor')} "
            f"list_silent={a.get('queue_list_silent')}. "
            f"Evidence basis: projects/zjp/state/generated/{ARTIFACT.name}. Kit files alarms; lead judges.")
    stale_tail = ""
    if stale_n:
        stale_tail = (f" MARKER DUTY: {stale_n} marked row(s) carry baseline evidence (stale-marker suspects) - "
                      f"unmark or record deliberate-deserve: "
                      f"{', '.join(r['key'] for r in bm['monitor_marked_with_baseline'][:4])}"
                      f"{' et al' if stale_n > 4 else ''}; spec research/SUP_MARKER_SCHEMA_2026-11-04.md s4/s5.")
    if len(head) > 400:
        head = (f"[{doc['generated_at'][:10]}] Warden alarm: {len(over)} SUP-owned >{ALARM_AGE}d, "
                f"{carried_n} carried, {fragile} identity-fragile, ls7d={a.get('list_silent_over_7d', 0)}; "
                f"queue m={a.get('queue_monitor')} ls={a.get('queue_list_silent')}. "
                f"Evidence: state/generated/{ARTIFACT.name}.")
    keys = [f"{r['key']} {r['age_days']}d" for r in over]
    top, more = "", ""
    for k in keys[:8]:
        cand = f"{top}, {k}" if top else k
        if len(head) + len(cand) + 10 > 430:
            more = f" (+{len(keys) - len([x for x in keys[:8] if x != k])} more in artifact)" if len(keys) > 8 else ""
            break
        top = cand
    extra = f" [+{others} non-SUP (AGG-routed), not alarmed here]" if others else ""
    note = head + (f" Oldest: {top}{more}{extra}" if top else extra) + stale_tail
    return note, over, others


def cmd_alarms(dry_run=False):
    doc = None
    if ARTIFACT.exists():
        doc = json.loads(ARTIFACT.read_text())
        stale = (datetime.now(timezone.utc) - datetime.fromisoformat(doc["generated_at"])).total_seconds() / 3600 > 48
        old_shape = "carried" not in doc  # pre-F325 artifact: no carry suppression, rebuild
        if stale or old_shape:
            doc = None  # rebuild (monitor gates re-run) rather than alarm on stale or carry-blind data
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
    state = load_silent_state()
    list_rows, list_zero, state, list_marked = fetch_list_universe(now, env, state)
    lookup = _carry_lookup(state)
    for r in rows:  # monitor rows inherit dispositions via bare-key/twin match
        rec = lookup(r["key"])
        if rec:
            # normalize at stamp time (mirrors the list path): expired = DUE, not carried
            r["carried_until"] = rec["carry_until"] if rec["carry_until"] >= _today() else None
            r["disposition"] = rec.get("disposition")
    merged = merge_rows(rows, list_rows)
    collisions = len(rows) + len(list_rows) - len(merged)
    if collisions < 0 or list_zero != len(list_rows):
        sys.exit(f"ERROR: list-universe merge arithmetic drifted (computed {list_zero}, kept {len(list_rows)}, "
                 f"collisions {collisions}) - inspect warden_silent_state.json")
    sources = {"monitor": len(rows), "list_silent": len(list_rows) - collisions,
               "collisions_monitor_won": collisions}
    merged, by_marker = monitor_marker_pass(merged)
    by_marker["list_marked"] = list_marked
    doc = write_artifact(merged, source, monitor.get("generated_at"), sources=sources, by_marker=by_marker)
    if not quiet:
        carried_n = doc["carried"]["count"]
        bm = doc["by_marker"]
        print(f"universes: monitor={len(rows)} list_silent={sources['list_silent']} "
              f"collisions={collisions} merged={len(merged)} carried={carried_n} "
              f"(next due {doc['carried']['next_due']}) marked={bm}")
        a = doc["alarm"]
        print(f"alarm: sup_over_7d={a['sup_companies_over_7d']} identity_fragile={a['identity_fragile_over_7d']} "
              f"queue_monitor={a['queue_monitor']} list_silent={a['queue_list_silent']} "
              f"match_basis={doc['by_match_basis']} marker={bm.get('monitor_marked_excluded')}excluded/"
              f"{len(bm.get('monitor_marked_with_baseline', []))}stale-suspect")
        print(render_table(doc["rows"]))
        print()
        read_back()
        print("next: python3 projects/zjp/kits/warden/warden_packet.py packet <key>   |   "
              "python3 projects/zjp/kits/warden/warden_queue.py alarms --dry-run   |   "
              "python3 projects/zjp/kits/warden/warden_queue.py disposition <key> carry-until=YYYY-MM-DD")
    return doc


def load_marked_keys():
    """Indexed-not-served marker keys from the CSV ledger (AGG CANDIDATE B D1/D3;
    spec: research/SUP_MARKER_SCHEMA_2026-11-04.md s4). Full-string equality law
    (review P2-2): a marked ledger row yields (ats, slug) for the slug leg (empty
    slug = no slug leg - the jobvite class keys on name only) and its normalized
    company name for the name leg, where the name is kept ONLY when unique across
    the marked set (0 or 2+ hits = ambiguous = never matches; merge_rows F320 law).
    Returns (marked_slugs set, marked_names set). Missing/absent column = no marks."""
    csv_path = Path(os.environ.get("WARDEN_CSV_PATH", str(CSV_PATH)))
    marked_slugs, name_counts = set(), collections.Counter()
    try:
        with csv_path.open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                if (r.get("indexed_not_served") or "").strip().lower() != "true":
                    continue
                ats = (r.get("ats") or "").strip().lower()
                slug = (r.get("slug") or "").strip().lower()
                if ats and slug:
                    marked_slugs.add((ats, slug))
                nm = _norm(r.get("company"))
                if nm:
                    name_counts[nm] += 1
    except FileNotFoundError:
        return marked_slugs, set()
    return marked_slugs, {n for n, c in name_counts.items() if c == 1}


def _row_marked(row, marked_slugs, marked_names):
    """True when a monitor row matches the marked set. Full-string legs only:
    exact lowercase ats+slug, then a unique normalized-name hit. Deliberately NOT
    entry_tokens/list_entry_present (:157-182): their substring probes would pull
    unrelated rows into the marked set and suppress alarms for unmarked rot."""
    ats = (row.get("ats") or "").strip().lower()
    slug = str(row.get("slug") or "").strip().lower()
    if ats and slug and (ats, slug) in marked_slugs:
        return True
    return _norm(row.get("company")) in marked_names


def monitor_marker_pass(merged_rows):
    """F326 marker split (review P1-2 fold): a marked monitor row is excluded from
    the queue ONLY when it carries no baseline evidence (never delivered - the
    deliberate-deserve class). A marked row WITH baseline (a family once delivered)
    is a stale-marker suspect: it STAYS in the queue and is inventoried in
    by_marker.monitor_marked_with_baseline so the alarm note names the
    unmark-or-deserve duty. Returns (kept_rows, by_marker_doc)."""
    marked_slugs, marked_names = load_marked_keys()
    if not marked_slugs and not marked_names:
        return merged_rows, {}
    kept, excluded, with_baseline = [], [], []
    for r in merged_rows:
        if r.get("source") != "list" and _row_marked(r, marked_slugs, marked_names):
            if (r.get("baseline_tech_us") or 0) > 0:
                with_baseline.append({"key": r["key"], "age_days": r["age_days"],
                                      "baseline_tech_us": r["baseline_tech_us"]})
            else:
                excluded.append(r["key"])
                continue
        kept.append(r)
    return kept, {"list_marked": 0, "monitor_marked_excluded": len(excluded),
                  "monitor_marked_excluded_keys": excluded,
                  "monitor_marked_with_baseline": with_baseline}


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


def load_silent_state():
    """Load the silent-state map, LOUD on loss (ColdEyes finding): a prior
    artifact without a state file means the memory of the rot is gone - ages
    would re-baseline to 0 and the kit would read healthy while the silent
    class re-rots. Values may be legacy `first_seen` strings or disposition
    records (F325 carry-until upgrade); readers tolerate both shapes."""
    state = {}
    if SILENT_STATE.exists():
        try:
            state = json.loads(SILENT_STATE.read_text())
        except Exception as e:
            sys.exit(f"ERROR: warden_silent_state.json corrupt ({e}) - restore it from the backup or delete it "
                     "AND the triage artifact to acknowledge a loud re-baseline; never reset silently")
    elif ARTIFACT.exists():
        sys.exit("ERROR: warden_silent_state.json missing while the triage artifact exists - "
                 "restore data/warden_silent_state.json from R2, or delete the triage artifact "
                 "to acknowledge a loud re-baseline of every silent age.")
    return state


def _list_entries(cl):
    """(triage entries, marked_count) from raw company-list. Marked entries
    (indexed_not_served) are by-design zeros: skipped entry-level, counted, and NO
    state mutation - slug-absent families share the fam/fam pseudo-key, so a state
    drop here would reset sibling ages (research/SUP_MARKER_SCHEMA_2026-11-04.md s4,
    review P1-3)."""
    entries, list_marked = [], 0
    for fam, arr in cl.items():
        if fam.startswith("_") or not isinstance(arr, list):
            continue
        for e in arr:
            if isinstance(e, dict) and e.get("name"):
                if e.get("indexed_not_served"):
                    list_marked += 1
                    continue
                entries.append({"ats": fam, "slug": str(e.get("slug") or fam), "company": e["name"]})
    return entries, list_marked


def fetch_list_universe(now, env, state=None):
    """Second universe: every company-list entry (aggregator clone) with ZERO pool
    presence, whether or not the decay monitor ever flagged it (the silent class).
    first_seen persisted per key in warden_silent_state.json - first observation of a
    silent zero starts its age TODAY (honest: no older age is knowable without a
    re-baseline). Returns (rows, computed_zero_count, state, list_marked)."""
    cl = json.loads(LIST_PATH.read_text(encoding="utf-8"))
    entries, list_marked = _list_entries(cl)
    slug_sets, names = fetch_pool_index(env)
    if state is None:
        state = load_silent_state()
    rows = []
    for e in entries:
        toks = entry_tokens(e)
        present, basis = list_entry_present(toks, e["ats"], slug_sets, names)
        if present:
            state.pop(f"{e['ats']}/{e['slug']}", None)
            continue
        key = f"{e['ats']}/{e['slug']}"
        first = state.get(key)
        if isinstance(first, dict):  # defensive: legacy readers never wrote objects
            first = first.get("first_seen")
        if not first:
            first = now.isoformat()
            state[key] = first
        crec = state.get("@carry:" + key)
        carry = crec.get("carry_until") if isinstance(crec, dict) else None
        age = max((now - datetime.fromisoformat(first)).days, 0)
        rows.append({"key": key, "company": e["company"], "ats": e["ats"], "slug": e["slug"],
                     "owner": "SUP", "source": "list", "flags": ["silent_zero_list"],
                     "match_basis": basis,
                     "age_days": age, "first_flagged": first, "review_bucket": "silent_zero_list",
                     "carried_until": carry if (carry and carry >= _today()) else None,
                     "disposition": crec.get("disposition") if isinstance(crec, dict) else None,
                     "baseline_tech_us": None, "current_final": 0,
                     "review_reason": "list entry with zero pool rows (outside monitor universe)"[:140]})
    SILENT_STATE.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(SILENT_STATE, json.dumps(state, indent=1, sort_keys=True) + "\n")
    return rows, len(rows), state, list_marked


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


def cmd_disposition(key, until, dclass=None, routed=None, receipt=None, reopen=False, dry_run=False):
    """Record a disposition into data/warden_silent_state.json (R2 canonical + local
    mirror). R2 is authoritative (CI round-trips it daily); local-only keys are
    preserved on merge. Collision window is seconds against one daily CI upload -
    no locking (do-not-build per the verifier-audit precedent)."""
    from datetime import date as _date
    try:
        _date.fromisoformat(until)
    except ValueError:
        sys.exit(f"ERROR: carry-until '{until}' is not YYYY-MM-DD")
    env = load_r2_env()
    import boto3
    s3 = boto3.client("s3", endpoint_url=env["R2_ENDPOINT"], aws_access_key_id=env["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"])
    remote = {}
    try:
        obj = s3.get_object(Bucket=env["R2_BUCKET_NAME"], Key="data/warden_silent_state.json")
        remote = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        pass
    local = load_silent_state()
    state = dict(remote)
    for k, v in local.items():  # preserve local-only keys (mirror drift is real: sophos was absent locally 11-03)
        state.setdefault(k, v)
    resolved, action = apply_disposition(state, key, until, dclass=dclass, routed=routed,
                                         receipt=receipt, reopen=reopen)
    print(f"disposition: {key} -> {resolved}: {action} carry_until={until if not reopen else '-'} "
          f"class={dclass or 'adjudicate-carried'} routed={routed or '-'} receipt={receipt or '-'}")
    if dry_run:
        print("DRY-RUN - state written nowhere")
        return 0
    body = json.dumps(state, indent=1, sort_keys=True) + "\n"
    s3.put_object(Bucket=env["R2_BUCKET_NAME"], Key="data/warden_silent_state.json",
                  Body=body.encode("utf-8"), ContentType="application/json")
    SILENT_STATE.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(SILENT_STATE, body)
    print(f"READ-BACK: R2 data/warden_silent_state.json + local mirror updated ({len(state)} keys)")
    return 0



def self_test():
    """In-memory fixture: gates, ages-vs-monitor-time, ranking, SUP-owned alarm split,
    open-row-only regex, atomic write. Zero network (no R2/todo-cli), writes only /tmp."""
    mon_now = datetime(2026, 10, 28, 6, 23, 37, tzinfo=timezone.utc)

    def flag(key, days, base=100, owner="SUP", basis="ats_slug_identity"):
        ff = None if days is None else (mon_now - timedelta(days=days)).isoformat().replace("+00:00", "Z")
        return {"key": key, "company": key.title(), "ats": "lever", "slug": key, "flags": [ZERO_FLAG],
                "likely_owner": owner, "review_bucket": "final_output_missing_review", "first_flagged": ff,
                "match_basis": basis,
                "baseline": {"tech_us_count": base}, "current": {"final_pool_total": 0}, "review_reason": "fixture"}

    monitor = {"schema_version": 2, "generated_at": "2026-10-28T06:23:37Z", "summary": {"by_flag": {ZERO_FLAG: 5}},
               "flagged_companies": [flag("selftest-old-a", 54), flag("selftest-mid-b", 3),
                                     flag("selftest-new-c", 0), flag("selftest-agg-d", 20, 50, owner="AGG"),
                                     flag("selftest-e", 10, basis="company_name")]}
    tmp = Path("/tmp/warden_queue_selftest.json")
    check_monitor(monitor, now=mon_now)
    for bad in [dict(monitor, schema_version=3), dict(monitor, generated_at="2026-10-01T00:00:00Z"),
                {**monitor, "summary": {"by_flag": {"other_flag": 1}}}]:
        try:
            check_monitor(bad, now=mon_now)
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
    assert doc["alarm"]["sup_companies_over_7d"] == 2 and doc["alarm"]["queue_over_50"] is False, doc["alarm"]  # AGG row not counted
    assert doc["alarm"]["identity_fragile_over_7d"] == 1 and doc["by_match_basis"]["company_name"] == 1, doc["alarm"]  # F325: basis passthrough
    assert doc["carried"] == {"count": 0, "next_due": None}  # no dispositions in the base fixture
    note, over, others = alarm_note(doc)
    assert len(over) == 2 and others == 1 and "selftest-old-a 54d" in note and "non-SUP" in note
    assert "identity-fragile" in note and len(note) <= 430, note[:440]  # clock-independent: fixture dates differ from publish date
    # F325 carry-until: carried rows are suppressed from alarms, reported as a split.
    carried_rows = [dict(r, carried_until="2099-01-01", disposition="adjudicate-carried") for r in rows]
    cdoc = write_artifact(carried_rows, "fixture", monitor["generated_at"], path=tmp)
    assert cdoc["alarm"]["sup_companies_over_7d"] == 0 and cdoc["carried"]["count"] == 5, cdoc["alarm"]
    cnote, cover, _ = alarm_note(cdoc)
    assert len(cover) == 0 and "0 carried" not in cnote and "5 carried" in cnote, cnote[:300]
    # Expiry: a past carry_until no longer suppresses (the row is DUE - it re-fires).
    due = write_artifact([dict(rows[0], carried_until="2026-01-01")], "fixture", monitor["generated_at"], path=tmp)
    assert due["alarm"]["sup_companies_over_7d"] == 1, due["alarm"]
    # P1-1 (EngChallenger): expired carry through alarm_note - the row must PAGE, not vanish.
    dnote, dover, _ = alarm_note(due)
    assert len(dover) == 1 and "selftest-old-a 54d" in dnote, dnote[:300]
    # P1-2 (EngChallenger): list-universe rows never enter the >7d alarm leg (11-05/06 flood guard).
    list_old = [{"key": "ashby/oldlist", "company": "Oldlist", "ats": "ashby", "slug": "oldlist",
                 "owner": "SUP", "source": "list", "age_days": 60, "baseline_tech_us": None,
                 "match_basis": "slug", "review_bucket": "silent_zero_list"}]
    ldoc = write_artifact(rows + list_old, "fixture", monitor["generated_at"], path=tmp)
    assert ldoc["alarm"]["sup_companies_over_7d"] == 2 and ldoc["alarm"]["list_silent_over_7d"] == 1, ldoc["alarm"]
    lnote, lover, _ = alarm_note(ldoc)
    assert len(lover) == 2 and "oldlist" not in lnote, lnote[:300]
    # apply_disposition: bare-key twin resolution, reopen, ambiguity refusal (pure, no network).
    st = {"ashby/phia": "2026-10-28T00:00:00+00:00", "lever/wintermute-trading": "2026-10-29T00:00:00+00:00"}
    rkey, action = apply_disposition(st, "phia", "2026-11-15", receipt="F322-cont2")
    assert (rkey, action) == ("ashby/phia", "carried") and st["@carry:ashby/phia"]["carry_until"] == "2026-11-15"
    assert st["ashby/phia"] == "2026-10-28T00:00:00+00:00"  # legacy entry value untouched (CI-safe namespace)
    rk2, act2 = apply_disposition(st, "wintermutetrading", "2026-11-15")  # norm-suffix twin
    assert (rk2, act2) == ("lever/wintermute-trading", "carried") and st["lever/wintermute-trading"].startswith("2026-10-29")
    rk3, act3 = apply_disposition(st, "sandisk", "2026-12-03", dclass="routed", routed="AGG-CUSTOMSITE-DOOR-BUILDS-1")
    assert (rk3, act3) == ("sandisk", "carried") and st["@carry:sandisk"]["routed"].startswith("AGG-")  # bare key: monitor-only row
    rk4, act4 = apply_disposition(st, "phia", "2099-01-01", reopen=True)
    assert (rk4, act4) == ("ashby/phia", "reopened") and "@carry:ashby/phia" not in st
    st["lever/aa"], st["ashby/aa"] = "2026-10-01T00:00:00+00:00", "2026-10-02T00:00:00+00:00"
    try:
        apply_disposition(st, "aa", "2099-01-01")  # ambiguous suffix twins must refuse loudly
        raise AssertionError("ambiguous twin match was silent")
    except SystemExit:
        pass
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

    # F326 marker (research/SUP_MARKER_SCHEMA_2026-11-04.md s4): full-string marked-set
    # matcher; baseline-less marked monitor rows excluded; baseline-bearing ones retained
    # as stale-marker suspects; ambiguous or substring names never match; by_marker is
    # top-level sibling of by_match_basis; list-marked skip is entry-level, state untouched.
    mcsv = Path(os.environ.get("WARDEN_CSV_PATH", "/tmp/warden_marker_selftest.csv"))
    with mcsv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["company", "ats", "slug", "indexed_not_served"])
        w.writeheader()
        w.writerow({"company": "Never Delivered Co", "ats": "lever", "slug": "selftest-marked-a", "indexed_not_served": "TRUE"})
        w.writerow({"company": "Once Delivered Co", "ats": "lever", "slug": "selftest-marked-b", "indexed_not_served": "true"})
        w.writerow({"company": "Ambig Co", "ats": "lever", "slug": "", "indexed_not_served": "true"})
        w.writerow({"company": "Ambig Co", "ats": "ashby", "slug": "", "indexed_not_served": "true"})
    os.environ["WARDEN_CSV_PATH"] = str(mcsv)
    rA = flag("selftest-marked-a", 20, base=0); rA["baseline_tech_us"] = 0; rA["age_days"] = 20   # slug leg, no baseline -> excluded
    rB = dict(flag("selftest-marked-b", 3, base=100), company="Once Delivered Co")
    rB["baseline_tech_us"] = 100; rB["age_days"] = 3                        # slug leg + baseline -> kept, stale suspect
    rC = dict(flag("selftest-byname", 20, base=0), company="Never Delivered Co"); rC["baseline_tech_us"] = 0; rC["age_days"] = 20  # unique-name leg -> excluded
    rD = dict(flag("selftest-ambig", 3, base=0), company="Ambig Co"); rD["baseline_tech_us"] = 0; rD["age_days"] = 3            # ambiguous name -> kept, not inventoried
    rE = flag("selftest-marked-a-extended", 3, base=0); rE["baseline_tech_us"] = 0; rE["age_days"] = 3                          # substring of marked slug -> NOT excluded
    kept, bm = monitor_marker_pass(rows + [rA, rB, rC, rD, rE])
    assert len(kept) == 8, len(kept)                                            # 10 in, rA + rC excluded
    assert bm["monitor_marked_excluded"] == 2 and sorted(bm["monitor_marked_excluded_keys"]) == \
        ["selftest-byname", "selftest-marked-a"], bm                # slug leg AND unique-name leg both fired
    assert bm["monitor_marked_with_baseline"] == [
        {"key": "selftest-marked-b", "age_days": 3, "baseline_tech_us": 100}], bm
    mdoc = write_artifact(kept, "fixture", monitor["generated_at"], path=tmp, by_marker=bm)
    assert mdoc["count"] == 8 and mdoc["alarm"]["sup_companies_over_7d"] == 2, (mdoc["count"], mdoc["alarm"])
    assert mdoc["by_marker"]["monitor_marked_excluded"] == 2 and mdoc["by_match_basis"], mdoc["by_marker"]
    mnote, mover, _ = alarm_note(mdoc)
    assert "MARKER DUTY" in mnote and "selftest-marked-b" in mnote and "unmark or record deliberate-deserve" in mnote, mnote[:400]
    assert len(mover) == 2  # the stale suspect stays a triage row (age 3d, not in over) but is never silenced
    entries, marked_n = _list_entries({"jobvite": [{"name": "MarkedVite", "url": "x", "indexed_not_served": True},
                                                   {"name": "PlainVite", "url": "y"}],
                                       "_meta": {"x": 1}, "lever": "not-a-list"})
    assert len(entries) == 1 and marked_n == 1 and entries[0]["slug"] == "jobvite"  # slugless -> fam pseudo-slug, key would be jobvite/jobvite: why no state mutation
    os.environ.pop("WARDEN_CSV_PATH", None)
    mcsv.unlink()

    tmp.unlink()
    print("warden_queue self-test: PASS (schema/freshness/flag-key gates refuse, ages vs monitor time, "
          "rank oldest-first, SUP-owned alarm split, null-age hard-fail, queue>50 trip, dated note <=400 "
          "chars, open-row-only dup guard, atomic write, monitor/list merge + collision monitor-wins, "
          "silent-universe presence matching, marker split: baseline-less excluded/baseline kept as stale "
          "suspect/ambiguous refused/list-marked entry-level no state mutation; fixture-only, zero network, real artifact untouched)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", choices=["build", "alarms", "disposition"])
    ap.add_argument("--dry-run", action="store_true", help="alarms: show what would file/update; disposition: no writes")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--class", dest="dclass", default=None, help="disposition: adjudicate-carried | move | retire | retry | routed")
    ap.add_argument("--routed", default=None, help="disposition: owner row the fix is routed to")
    ap.add_argument("--receipt", default=None, help="disposition: session/receipt id")
    ap.add_argument("--reopen", action="store_true", help="disposition: strip the carry, row re-fires")
    ap.add_argument("key", nargs="?", default=None)
    ap.add_argument("until", nargs="?", default=None, help="disposition: carry-until YYYY-MM-DD")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    if a.cmd == "alarms":
        return cmd_alarms(dry_run=a.dry_run)
    if a.cmd == "build":
        cmd_build()
        return 0
    if a.cmd == "disposition":
        if not a.key or not a.until:
            ap.error("disposition requires <key> carry-until=YYYY-MM-DD (positional: <key> <until>)")
        return cmd_disposition(a.key, a.until, dclass=a.dclass, routed=a.routed,
                               receipt=a.receipt, reopen=a.reopen, dry_run=a.dry_run)
    ap.print_usage()
    return 1


if __name__ == "__main__":
    sys.exit(main())
