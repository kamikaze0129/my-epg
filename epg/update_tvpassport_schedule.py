#!/usr/bin/env python3
"""TV Passport schedule updater for Chris's EPG pipeline.

Fetches upcoming listings from tvpassport.com per-station pages, maps them
to Chris's empty USA-local channels, and pushes an updated
tvpassport_schedule.json to kamikaze0129/my-epg.

Phase 1 scope: USA locals only (TV Passport's strength). The station mapping
lives in tvpassport_mapping.json (staged, reviewed); stations that could not
be matched unambiguously are NOT scraped.

Timezones: tvpassport.com renders data-st in the anonymous-default "Your
Time Zone", which is Europe/London (verified 2026-09-29 via the selected
<option> on the page). data-st is therefore interpreted with
zoneinfo("Europe/London") -- this handles BST/GMT transitions automatically.
The earlier stub stamped +0000 on these London times; that bug is NOT
repeated here.

Polite: ~1 req/s, 3 days per station (community grabber depth), retries.

Safe to run any time: skips gracefully when the mapping is empty, the
sitemap fetch fails, or a station page errors.
"""
import base64
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
from dynamic_credentials import add_surrogate_to_request, read_json_response

OWNER = "kamikaze0129"
REPO = "my-epg"
BRANCH = "main"
API = f"https://api.github.com/repos/{OWNER}/{REPO}"
CRED = "custom.github"

TVP = "https://www.tvpassport.com"
LONDON = ZoneInfo("Europe/London")
UA = "muse-tvpassport-updater"

BUILD_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_FILE = os.path.join(BUILD_DIR, "tvpassport_mapping.json")
LOCAL_JSON = os.path.expanduser("~/workspace/epg_build/tvpassport_schedule.json")
CI_JSON = os.path.expanduser("~/workspace/epg_actions/epg/tvpassport_schedule.json")

DAYS_AHEAD = 3
REQ_PAUSE = 1.0  # polite: ~1 request/second


def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def fetch(url, timeout=60, retries=3):
    last = None
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def parse_station_day(html):
    """Extract programmes from one station-day page.

    Returns list of dicts with UTC datetimes. data-st is Europe/London wall
    time (site default timezone for anonymous users).
    """
    progs = []
    for m in re.finditer(r'<div[^>]*data-st="([^"]+)"[^>]*>', html):
        el = m.group(0)

        def attr(name):
            mm = re.search(r'%s="([^"]*)"' % re.escape(name), el)
            return (mm.group(1).strip() if mm else "")

        st = attr("data-st")
        try:
            local = datetime.strptime(st, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=LONDON)
        except ValueError:
            continue
        try:
            dur_min = int(attr("data-duration") or "0")
        except ValueError:
            dur_min = 0
        if dur_min <= 0:
            continue
        start_utc = local.astimezone(timezone.utc)
        progs.append({
            "start_utc": start_utc,
            "stop_utc": start_utc + timedelta(minutes=dur_min),
            "title": attr("data-showName"),
            "episode": attr("data-episodeTitle"),
            "desc": attr("data-description"),
            "category": attr("data-showType"),
            "rating": attr("data-rating"),
            "cast": attr("data-cast"),
            "year": attr("data-year"),
            "live": attr("data-live"),
            "new": attr("data-new_show"),
        })
    # drop empties, sort
    progs = [p for p in progs if p["title"]]
    progs.sort(key=lambda p: p["start_utc"])
    return progs


def esc(s):
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def norm_rating(r):
    r = (r or "").upper().replace("-", "").replace(" ", "")
    return {"TVY": "TV-Y", "TVY7": "TV-Y7", "TVG": "TV-G", "TVPG": "TV-PG",
            "TV14": "TV-14", "TVMA": "TV-MA"}.get(r, r or None)


def build_programme_xml(cid, p):
    fmt = lambda d: d.strftime("%Y%m%d%H%M%S") + " +0000"
    out = [f'  <programme start="{fmt(p["start_utc"])}" '
           f'stop="{fmt(p["stop_utc"])}" channel="{esc(cid)}">',
           f'    <title>{esc(p["title"])}</title>']
    if p["episode"]:
        out.append(f'    <sub-title>{esc(p["episode"])}</sub-title>')
    desc_bits = []
    if p["desc"]:
        desc_bits.append(p["desc"])
    if p["cast"]:
        desc_bits.append("Cast: " + p["cast"])
    if p["year"]:
        desc_bits.append(f"({p['year']})")
    if desc_bits:
        out.append(f'    <desc>{" ".join(esc(b) for b in desc_bits)}</desc>')
    if p["category"]:
        for cat in p["category"].split(","):
            cat = cat.strip()
            if cat:
                out.append(f'    <category>{esc(cat)}</category>')
    if p["live"]:
        out.append('    <live />')
    if p["new"]:
        out.append('    <previously-shown />')
    rating = norm_rating(p["rating"])
    if rating:
        out.append(f'    <rating system="VCHIP"><value>{esc(rating)}</value></rating>')
    out.append('  </programme>')
    return "\n".join(out) + "\n"


def gh_api(method, path, data=None):
    url = f"{API}{path}" if path.startswith("/") else path
    body = json.dumps(data).encode() if data is not None else None
    r = urllib.request.Request(url, data=body, method=method)
    r.add_header("Accept", "application/vnd.github+json")
    r.add_header("X-GitHub-Api-Version", "2022-11-28")
    r.add_header("User-Agent", UA)
    if body:
        r.add_header("Content-Type", "application/json")
    add_surrogate_to_request(r, CRED, allowed_hosts=("api.github.com",))
    with urllib.request.urlopen(r, timeout=120) as resp:
        return resp.status, read_json_response(resp)


def push_json(content):
    s, ref = gh_api("GET", f"/git/ref/heads/{BRANCH}")
    assert s == 200, (s, ref)
    base_sha = ref["object"]["sha"]
    s, base_commit = gh_api("GET", f"/git/commits/{base_sha}")
    base_tree = base_commit["tree"]["sha"]
    blob_body = base64.b64encode(content.encode()).decode()
    s, blob = gh_api("POST", "/git/blobs",
                     {"content": blob_body, "encoding": "base64"})
    assert s == 201, (s, blob)
    s, new_tree = gh_api(
        "POST", "/git/trees",
        {"base_tree": base_tree,
         "tree": [{"path": "epg/tvpassport_schedule.json", "mode": "100644",
                   "type": "blob", "sha": blob["sha"]}]})
    assert s == 201, (s, new_tree)
    s, commit = gh_api(
        "POST", "/git/commits",
        {"message": "TV Passport: refresh USA-locals schedule\n\n"
                    "Auto-updated from tvpassport.com station listings.",
         "tree": new_tree["sha"], "parents": [base_sha]})
    assert s == 201, (s, commit)
    s, _ = gh_api("PATCH", f"/git/refs/heads/{BRANCH}",
                  {"sha": commit["sha"]})
    assert s == 200, (s,)
    log(f"pushed TV Passport schedule (commit {commit['sha'][:7]})")


def main():
    dry_run = "--dry-run" in sys.argv
    limit = None
    for a in sys.argv:
        if a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])

    try:
        with open(MAPPING_FILE, encoding="utf-8") as f:
            mapping = json.load(f)
    except (OSError, ValueError) as e:
        log(f"mapping file missing/unreadable ({e}); skipping")
        return 1
    if limit:
        mapping = mapping[:limit]
    if not mapping:
        log("mapping is empty; skipping")
        return 0
    log(f"{len(mapping)} mapped stations")

    now = datetime.now(timezone.utc)
    data = {"_meta": {
        "generated_utc": now.isoformat(),
        "source": "tvpassport.com station listings (auto)",
        "days_ahead": DAYS_AHEAD,
        "station_tz_note": ("data-st rendered in site default Europe/London; "
                            "converted to UTC via zoneinfo"),
        "note": ("USA-locals listings -> provider channel IDs. "
                 "Channel->station mapping is reviewed, not guessed."),
    }}
    display_names = {}
    last_end = now
    ok, failed = 0, 0

    for entry in mapping:
        cid = entry["chris_id"]
        slug, site_id = entry["tvp_slug"], entry["tvp_id"]
        progs = []
        try:
            for d in range(DAYS_AHEAD):
                day = (now + timedelta(days=d)).date().isoformat()
                url = f"{TVP}/tv-listings/stations/{slug}/{site_id}/{day}"
                html = fetch(url)
                day_progs = parse_station_day(html)
                # keep programmes that start within our window
                cutoff = now + timedelta(days=DAYS_AHEAD)
                progs.extend(p for p in day_progs
                             if now - timedelta(hours=6) <= p["start_utc"] <= cutoff)
                time.sleep(REQ_PAUSE)
        except Exception as e:
            log(f"  {cid}: fetch failed ({e}); skipping station")
            failed += 1
            continue
        if not progs:
            log(f"  {cid}: no programmes parsed; skipping")
            failed += 1
            continue
        # de-dupe by start time (day pages can overlap at midnight)
        seen, uniq = set(), []
        for p in progs:
            k = p["start_utc"]
            if k not in seen:
                seen.add(k)
                uniq.append(p)
        for p in uniq:
            data.setdefault(cid, []).append(build_programme_xml(cid, p))
            if p["stop_utc"] > last_end:
                last_end = p["stop_utc"]
        # display-name suggestion: keep provider convention
        if entry.get("chris_name"):
            display_names[cid] = entry["chris_name"]
        ok += 1
        log(f"  {cid}: {len(uniq)} programmes <- {slug}/{site_id}")

    if ok == 0:
        log("no stations produced data; skipping")
        return 1
    data["_meta"]["valid_until_utc"] = last_end.isoformat()
    data["_display_names"] = display_names
    content = json.dumps(data, indent=1)
    for path in (LOCAL_JSON, CI_JSON):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
    log(f"wrote {LOCAL_JSON} and {CI_JSON} ({ok} stations, {failed} failed)")
    if dry_run:
        log("dry run -- not pushing to GitHub")
        return 0
    push_json(content)
    return 0


if __name__ == "__main__":
    sys.exit(main())
