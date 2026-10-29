#!/usr/bin/env python3
"""warden_packet.py - Warden kit evidence-packet generator (Desk 3: HEALTH). Probes one zero-output
company's named board family + the other canned families (greenhouse/lever/ashby/workday/workable/
smartrecruiters; status-code method: 200 live, 404 dead, 422 wrong workday datacenter; GET 404/429
retried once per F218 single-404 fallacy). Emits counts + a verdict-class RECOMMENDATION - the kit
never issues verdicts, the lead judges. Blocked probes (network error / 403 / 5xx / parse) map to
UNPROBED-RETRY, never GONE/MOVED (403 is a known fake-death class here). Unsupported families map
to UNPROBED-FAMILY + custom-door pointer. Usage: packet <key> [--artifact PATH] | --self-test."""
import argparse, json, re, sys, threading, time, urllib.request, urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARTIFACT_DEFAULT = HERE.parents[3] / "projects/zjp/state/generated/warden_triage_queue.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
      "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9"}
BASES = {"greenhouse": "https://boards-api.greenhouse.io", "lever": "https://api.lever.co",
         "ashby": "https://api.ashbyhq.com", "smartrecruiters": "https://api.smartrecruiters.com",
         "workable": "https://www.workable.com", "workday": "https://{host}"}
US_RE = re.compile(r"\bUnited States\b|\bU\.S\.A?\b|\bUSA\b|,?\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b(?![-A-Za-z])")
INTERN_RE = re.compile(r"\bintern", re.I)
LEVER_CC = {"US": "United States", "CA": "Canada", "GB": "United Kingdom", "DE": "Germany", "FR": "France", "IN": "India", "SG": "Singapore", "AU": "Australia"}
ROUTING = {"LIVE-UNFETCHED": "AGG (fetcher work - our gap)", "MOVED": "Intake (new board mapping)",
           "EMPTY": "Registry (lead judges died vs seasonal)", "GONE": "Registry (lead judges)",
           "UNPROBED-RETRY": "re-run the sweep; blocked probes are a fake-death class - never file died from one",
           "UNPROBED-FAMILY": "custom-door method (sup-source-research kit); a lead session probes the door"}
DEAD, BLOCKED = (404, 410), (0, 403, 500, 502, 503, 504)


def _summarize(rows, url):
    """[(title, location)] -> board/US/intern counts + evidence quote."""
    return {"status": 200, "total": len(rows), "us": sum(1 for r in rows if US_RE.search(r[1] or "")),
            "intern": sum(1 for r in rows if INTERN_RE.search(r[0] or "")),
            "quote": f'"{rows[0][0]}" ({rows[0][1]})' if rows else "(empty listing)", "url": url}


def _dead(url, code, extra=None):
    return {**{"status": code, "error": f"HTTP {code}", "url": url}, **(extra or {})}


def _get(url, attempts=2):
    for i in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=20) as r:
                return r.status, json.loads(r.read(4_000_000))
        except urllib.error.HTTPError as e:
            if e.code in (404, 429) and i < attempts - 1:
                time.sleep(0.6)
                continue
            return e.code, None
        except Exception as e:
            return 0, str(e)


def _post(url, body):
    try:
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={**UA, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, json.loads(r.read(2_000_000))
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def _rows_gh(d):
    return [(j.get("title", ""), (j.get("location") or {}).get("name", "")) for j in d.get("jobs", [])]


def _rows_lv(d):
    cc = lambda j: LEVER_CC.get(str(j.get("country") or "").upper(), str(j.get("country") or ""))
    return [(j.get("text", ""), (cc(j) + " " + " ".join(j.get("tags", {}).get("location", []))).strip())
            for j in (d if isinstance(d, list) else [])]


def _rows_as(d):
    out = []
    for j in d.get("jobs", []):
        a = (j.get("address") or {}).get("postalAddress") or {}
        out.append((j.get("title", ""), " ".join(str(x) for x in [j.get("location", "")] +
                   [a.get(k) for k in ("addressLocality", "addressRegion", "addressCountry") if a.get(k)])))
    return out


def _rows_sr(d):
    loc = lambda j: ", ".join(str((j.get("location") or {}).get(k)) for k in ("city", "region", "country")
                              if (j.get("location") or {}).get(k))
    return [(j.get("name", ""), loc(j)) for j in d.get("content", [])]


def _rows_wk(d):
    return [(j.get("title", ""), ", ".join(str(x) for x in ((j.get("location") or {}).get("city"),
            (j.get("location") or {}).get("country")) if x)) for j in d.get("jobs", [])]


def _page_note(r, rows):
    if r["total"] > len(rows):
        r["count_basis"] = f"us/intern counted on first {len(rows)} postings of {r['total']}"
    return r


def _get_probe(path_fn, rows_fn, total_fn=None):
    """GET family probe: 200 -> summarize rows; otherwise dead/blocked with the status code."""
    def probe(slug):
        url = path_fn(slug)
        code, d = _get(url)
        if code != 200:
            return _dead(url, code)
        rows = rows_fn(d)
        r = _summarize(rows, url)
        if total_fn:
            r["total"] = total_fn(d, r)
        return _page_note(r, rows)
    return probe


def p_workday(slug):
    """CXS status-code method. slug = tenant URL; wrong site 404, wrong datacenter 422."""
    m = re.match(r"https?://([a-z0-9.-]+)", (slug or "").strip().lower())
    if not m:
        return {"status": None, "error": "not a workday tenant URL - family not applicable for this slug"}
    host = m.group(1)
    tenant = host.split(".")[0]
    code, tried = None, []
    for site in (tenant, tenant + "careers", "careers", "jobs", "external"):
        url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
        code, d = _post(url, {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""})
        tried.append(f"{site}:{code}")
        if code == 200:
            rows = [(p.get("title", ""), p.get("locationsText", "")) for p in d.get("jobPostings", [])]
            r = _summarize(rows, url)
            r["total"] = d.get("total", r["total"])
            return {**_page_note(r, rows), "sites_tried": tried}
        if code == 422:
            break  # tenant/datacenter dead - site variants cannot fix it
    return _dead(url, code, {"sites_tried": tried})


PROBES = {"greenhouse": _get_probe(lambda s: f"{BASES['greenhouse']}/v1/boards/{s}/jobs", _rows_gh),
          "lever": _get_probe(lambda s: f"{BASES['lever']}/v0/postings/{s}?mode=json", _rows_lv),
          "ashby": _get_probe(lambda s: f"{BASES['ashby']}/posting-api/job-board/{s}", _rows_as),
          "smartrecruiters": _get_probe(lambda s: f"{BASES['smartrecruiters']}/v1/companies/{s}/postings?limit=100",
                                        _rows_sr, lambda d, r: d.get("totalFound", r["total"])),
          "workable": _get_probe(lambda s: f"{BASES['workable']}/api/accounts/{s.lower()}?details=true", _rows_wk),
          "gem": _get_probe(lambda s: f"https://api.gem.com/job_board/v0/{s}/job_posts/",
                            lambda d: [(j.get("title", ""), (j.get("location") or {}).get("name", ""))
                                       for j in (d if isinstance(d, list) else [])],
                            lambda d, r: len(d) if isinstance(d, list) else r["total"]),
          "workday": p_workday}


def packet(entry, now=None):
    now = now or datetime.now(timezone.utc)
    ats, slug = entry["ats"], entry["slug"]
    named = (PROBES[ats](slug) if ats in PROBES else
             {"status": None, "note": f"no canned probe for ats={ats}; custom-door method is a session step"})
    discovery, blocked = [], False
    st = named.get("status")
    if named.get("total"):
        recommendation = "LIVE-UNFETCHED"
    elif st == 200:
        recommendation = "EMPTY"
    elif st in BLOCKED:
        recommendation = "UNPROBED-RETRY"  # 403/network/5xx: fake-death class - never GONE/MOVED off these
    else:  # named dead (404/410) or unsupported family: cross-family same-slug discovery
        for fam in PROBES:
            if fam == ats:
                continue
            for v in dict.fromkeys([slug, slug.replace("-", "")]):
                r = PROBES[fam](v)
                if r.get("total"):
                    discovery.append({"family": fam, "slug": v, **r})
                    break
                if r.get("status") in BLOCKED:
                    blocked = True
            if discovery:
                break
        if discovery:
            recommendation = "MOVED"
        elif blocked:
            recommendation = "UNPROBED-RETRY"
        elif st in DEAD:
            recommendation = "GONE"
        else:
            recommendation = "UNPROBED-FAMILY"
    ff = entry.get("first_flagged")
    return {"company": entry["company"], "key": entry["key"], "ats": ats, "slug": slug,
            "finding": {"flags": entry.get("flags", []), "review_bucket": entry.get("review_bucket"),
                        "last_good_baseline": entry.get("baseline") or {"tech_us_count": entry.get("baseline_tech_us")},
                        "current": entry.get("current") or {"final_pool_total": entry.get("current_final")},
                        "first_flagged": ff,
                        "age_days": (now - datetime.fromisoformat(ff.replace("Z", "+00:00"))).days if ff else None},
            "named_board_probe": named, "discovery": discovery, "verdict_recommendation": recommendation,
            "routing_hint": ROUTING.get(recommendation), "law": "recommendation only - the lead issues the verdict (kits never judge)",
            "evidence_quote": discovery[0]["quote"] if discovery else named.get("quote"),
            "issued_at": now.isoformat(timespec="seconds")}


def _serve(fixtures, port):
    """Minimal JSON fixture server for the self-test (localhost only); fixtures: path->(code,obj)."""
    class H(BaseHTTPRequestHandler):
        log_message = lambda self, *a: None
        def _hit(self):
            code, obj = fixtures.get(self.path.split("?")[0], (404, {}))
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b)
        do_GET = do_POST = _hit
    srv = HTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def self_test():
    """Planted live/dead/moved/blocked fixtures on 127.0.0.1, fake slugs - zero real-company network."""
    fixtures = {"/v1/boards/selftest-live-co/jobs": (200, {"jobs": [
                    {"title": "Senior Software Engineer", "location": {"name": "Austin, TX"}},
                    {"title": "Data Engineer Intern", "location": {"name": "Austin, TX, United States"}},
                    {"title": "RF Engineer", "location": {"name": "Toronto, Canada"}}]}),
                "/posting-api/job-board/selftest-moved-co": (404, {}),
                "/v0/postings/selftest-moved-co": (200, [
                    {"text": "Hardware Intern", "country": "US", "tags": {"location": ["San Jose"]}},
                    {"text": "QA Engineer", "country": "US", "tags": {"location": []}}]),
                "/v1/boards/selftest-blocked-co/jobs": (403, {})}
    srv = _serve(fixtures, 28471)
    saved = dict(BASES)
    BASES.update((k, "http://127.0.0.1:28471") for k in BASES)
    assert all(v.startswith("http://127.0.0.1") for v in BASES.values()), "self-test must stay on localhost"
    now = datetime(2026, 10, 28, tzinfo=timezone.utc)
    ent = lambda key, ats, slug: {"company": key.title(), "key": key, "ats": ats, "slug": slug,
                                  "flags": ["company_missing_from_final_output"],
                                  "first_flagged": "2026-09-04T00:00:00Z", "baseline_tech_us": 210}
    cases = [("selftest-live", "greenhouse", "selftest-live-co", "LIVE-UNFETCHED", (3, 2, 1, 54), None),
             ("selftest-dead", "greenhouse", "selftest-dead-co", "GONE", None, None),
             ("selftest-moved", "ashby", "selftest-moved-co", "MOVED", None, "lever"),
             ("selftest-blocked", "greenhouse", "selftest-blocked-co", "UNPROBED-RETRY", None, None)]
    failures = []
    try:
        for key, ats, slug, verdict, nb_expect, d_fam in cases:
            p = packet(ent(key, ats, slug), now)
            nb, ok = p["named_board_probe"], p["verdict_recommendation"] == verdict
            if nb_expect:  # (total, us, intern, age_days)
                ok = ok and (nb["total"], nb["us"], nb["intern"], p["finding"]["age_days"]) == nb_expect
                ok = ok and p["finding"]["last_good_baseline"] == {"tech_us_count": 210}
            if d_fam:  # moved: named board 404, discovery found the new family
                d0 = p["discovery"][0] if p["discovery"] else {}
                ok = ok and nb["status"] == 404 and d0.get("family") == d_fam and d0.get("intern") == 1 and p["evidence_quote"]
            if verdict in ("GONE", "UNPROBED-RETRY"):
                ok = ok and not p["discovery"]  # blocked probe never spawns discovery or a GONE
            if not ok:
                failures.append(f"{key}: {p}")
    finally:
        BASES.clear(); BASES.update(saved)
        srv.shutdown()
    if failures:
        for f in failures:
            print("FAIL:", f)
        return 1
    print("warden_packet self-test: PASS (live=LIVE-UNFETCHED 3/2us/1intern; dead=GONE; moved=MOVED via lever "
          "discovery; blocked-403=UNPROBED-RETRY no discovery; localhost-only, fake slugs, zero real-company network)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", choices=["packet"])
    ap.add_argument("key", nargs="?")
    ap.add_argument("--artifact", default=str(ARTIFACT_DEFAULT))
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    if a.cmd != "packet" or not a.key:
        ap.print_usage()
        return 1
    rows = json.loads(Path(a.artifact).read_text())["rows"]
    entry = next((r for r in rows if r["key"] == a.key), None)
    if not entry:
        sys.exit(f"key '{a.key}' not in {a.artifact} ({len(rows)} rows)")
    print(json.dumps(packet(entry), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
