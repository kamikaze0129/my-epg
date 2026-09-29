#!/usr/bin/env python3
"""Cable-network schedule updater for Chris's EPG pipeline.

Fetches listings from the non-TVP cable sources Chris approved 2026-09-29:
  - TV Insider (server-rendered schedule pages, 2-week depth)
  - OnTVTonight (server-rendered channel listings)
  - Pluto TV public API (timelines endpoint, iptv-org pattern)
  - Official sites (starz.com / hbo.com / cinemax.com / ms.now /
    foxbusiness.com) -- wired when extractors land

Writes cable_schedule.json (local + CI copy) and pushes to kamikaze0129/my-epg.
Builder stage 3d6 consumes it with fill-only/append-only semantics.

Timezones: TV Insider and OnTVTonight render US Eastern times
(America/New_York). Pluto timelines are UTC ISO. All output is UTC ISO.

Polite: ~1 req/s per host, retries. Safe to run any time.
"""
import gzip
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
from dynamic_credentials import add_surrogate_to_request, read_json_response

OWNER = "kamikaze0129"
REPO = "my-epg"
BRANCH = "main"
API = f"https://api.github.com/repos/{OWNER}/{REPO}"
CRED = "custom.github"

ET = ZoneInfo("America/New_York")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

BUILD_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_JSON = os.path.expanduser("~/workspace/epg_build/cable_schedule.json")
CI_JSON = os.path.expanduser("~/workspace/epg_actions/epg/cable_schedule.json")

DAYS_AHEAD = 7
REQ_PAUSE = 1.0


def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def fetch(url, timeout=60, retries=3):
    last = None
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                       "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return raw.decode("utf-8", "replace")
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def fetch_json(url, timeout=60, retries=3):
    last = None
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


# ---------------------------------------------------------------- TV Insider
def parse_tvinsider(html):
    """Parse a tvinsider.com/network/<slug>/schedule/ page.

    Structure: <h2 id="MM-DD-YYYY" class="date"> then
    <a class="show-upcoming"><div class="top-element"><time>8:00 AM</time></div>
    <h3>Title</h3><h4>meta</h4><h6></h6><p>desc</p></a>
    Times are US Eastern. Stop = next programme's start (last of day +30m).
    """
    progs = []
    # split by date headers
    parts = re.split(r'<h2 id="(\d{2})-(\d{2})-(\d{4})" class="date">', html)
    # parts[0] = pre, then groups of (mm, dd, yyyy, body)
    for i in range(1, len(parts), 4):
        mm, dd, yyyy, body = parts[i], parts[i+1], parts[i+2], parts[i+3]
        day = datetime(int(yyyy), int(mm), int(dd))
        blocks = re.findall(
            r'<a class="show-upcoming">.*?<time>([^<]+)</time>.*?<h3>([^<]*)</h3>'
            r'(?:.*?<h4>([^<]*)</h4>)?.*?<p>([^<]*)</p>',
            body, re.S)
        day_progs = []
        for tstr, title, meta, desc in blocks:
            tstr = tstr.strip()
            m = re.match(r"(\d{1,2}):(\d{2})\s*([AP]M)", tstr, re.I)
            if not m:
                continue
            hr, mi, ap = int(m.group(1)), int(m.group(2)), m.group(3).upper()
            if ap == "PM" and hr != 12:
                hr += 12
            if ap == "AM" and hr == 12:
                hr = 0
            start = day.replace(hour=hr, minute=mi, tzinfo=ET).astimezone(timezone.utc)
            day_progs.append({
                "start": start.isoformat(),
                "title": re.sub(r"\s+", " ", title).strip(),
                "episode": re.sub(r"\s+", " ", meta or "").strip(),
                "desc": re.sub(r"\s+", " ", desc or "").strip(),
            })
        # stops: next start, or +30m for the last
        for j, p in enumerate(day_progs):
            if j + 1 < len(day_progs):
                p["stop"] = day_progs[j+1]["start"]
            else:
                p["stop"] = (datetime.fromisoformat(p["start"]) +
                             timedelta(minutes=30)).isoformat()
            progs.append(p)
    return progs


def fetch_tvinsider(slug):
    url = f"https://www.tvinsider.com/network/{slug}/schedule/"
    return parse_tvinsider(fetch(url)), url


# ------------------------------------------------------------- OnTVTonight
def parse_ontvtonight(html, page_date):
    """Parse an ontvtonight.com channel listings page.

    Rows: <td class="ott-channel-time-cell"><h5 ...ott-channel-time>01:00 am</h5>
    and <td class="ott-channel-show-cell">... <h5 ...ott-channel-show__title><a>Title</a>
    with optional <h6 class="ott-channel-show__meta">episode</h6>.
    Page covers "tonight" => page_date (a date). Times are US Eastern.
    """
    progs = []
    rows = re.findall(
        r'ott-channel-time">([^<]+)</h5>.*?ott-channel-show__title">\s*<a[^>]*>([^<]+)</a>'
        r'(?:.*?<h6 class="ott-channel-show__meta">(.*?)</h6>)?',
        html, re.S)
    items = []
    for tstr, title, meta in rows:
        m = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)", tstr.strip(), re.I)
        if not m:
            continue
        hr, mi, ap = int(m.group(1)), int(m.group(2)), m.group(3).lower()
        if ap == "pm" and hr != 12:
            hr += 12
        if ap == "am" and hr == 12:
            hr = 0
        start = datetime(page_date.year, page_date.month, page_date.day,
                         hr, mi, tzinfo=ET).astimezone(timezone.utc)
        meta = re.sub(r"<[^>]+>", "", meta or "")
        items.append({
            "start": start.isoformat(),
            "title": re.sub(r"\s+", " ", title).strip(),
            "episode": re.sub(r"\s+", " ", meta).strip(),
            "desc": "",
        })
    for j, p in enumerate(items):
        if j + 1 < len(items):
            p["stop"] = items[j+1]["start"]
        else:
            p["stop"] = (datetime.fromisoformat(p["start"]) +
                         timedelta(minutes=30)).isoformat()
        progs.append(p)
    return progs


def fetch_ontvtonight(channel_id, slug):
    url = f"https://www.ontvtonight.com/guide/listings/channel/{channel_id}/{slug}.html"
    return parse_ontvtonight(fetch(url), datetime.now(ET).date()), url


# ------------------------------------------------------------------- Pluto
def fetch_pluto(channel_api_id):
    """Pluto TV public API (iptv-org endpoint pattern). Timelines are UTC."""
    start = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00.000Z")
    stop = (datetime.now(timezone.utc) +
            timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%dT00:00:00.000Z")
    url = (f"https://api.pluto.tv/v2/channels/{channel_api_id}"
           f"?start={urllib.parse.quote(start)}&stop={urllib.parse.quote(stop)}")
    data = fetch_json(url)
    progs = []
    for e in data.get("timelines", []):
        ep = e.get("episode") or {}
        progs.append({
            "start": e.get("start"),
            "stop": e.get("stop"),
            "title": e.get("title", ""),
            "episode": ep.get("name", ""),
            "desc": ep.get("description", ""),
        })
    return progs, url


# ------------------------------------------------------------------- Starz
# (existing Starz code above)

# ------------------------------------------------------- HBO / Cinemax
# Warner Bros. Discovery GEP GraphQL endpoint. One endpoint serves both
# brands; no auth required. Feed codes extracted 2026-09-29 from each
# site's embedded feedsConfig. Full details in
# epg_build/hbo_cinemax_api_research.md.
# NOTE: no OuterMax feed exists on cinemax.com -- outermax.us keeps its
# honest placeholder until a source is found.
WBD_GRAPHQL = "https://wme-gep-graphql-prod.wme-digital.com/graphql"
WBD_QUERY = ("query getListings($brand:[String],$feed:[String],$startDate:String,"
             "$endDate:String,$count:Int){getScheduleEntries(filters:{brand:$brand,"
             "feed:$feed,scheduleRange:{startDate:$startDate,endDate:$endDate}},"
             "count:$count){feed scheduledTimestamp scheduledDuration "
             "title{en_US{full}}}}")

HBO_CINEMAX_FEEDS = {
    # chris_id: (brand, feed)
    "hbo.us": ("hbo", "hbo-east"),
    "epg-usa-hbo-east-bad286d2": ("hbo", "hbo-east"),
    "hbo2.us": ("hbo", "hbo2-east"),
    "hbocomedy.us": ("hbo", "hbo-comedy-east"),
    "hbosignature.us": ("hbo", "hbo-signature-east"),
    "hbozone.us": ("hbo", "hbo-zone-east"),
    "hbowest.us": ("hbo", "hbo-west"),
    "hbo2pacific.us": ("hbo", "hbo2-west"),
    "hbocomedypacific.us": ("hbo", "hbo-comedy-west"),
    "m3u-usa-hbo-drama-west": ("hbo", "hbo-signature-west"),
    "hbozonepacific.us": ("hbo", "hbo-zone-west"),
    "cinemax.us": ("cinemax", "max-east"),
    "epg-usa-cinemax-east-816867a4": ("cinemax", "max-east"),
    "epg-usa-cinemax-east-uhd-fe03eeee": ("cinemax", "max-east"),
    "actionmax.us": ("cinemax", "max-action-east"),
    "epg-usa-cinemax-action-max-east-91c7973f": ("cinemax", "max-action-east"),
    "epg-usa-cinemax-action-07dc64d9": ("cinemax", "max-action-east"),
    "moremax.us": ("cinemax", "max-more-east"),
    "epg-usa-cinemax-hits-east-2779670c": ("cinemax", "max-more-east"),
    "epg-usa-cinemax-hits-fhd-4f225dbf": ("cinemax", "max-more-east"),
    "5starmax.us": ("cinemax", "max-5star-east"),
    "cinemaxpasific.us": ("cinemax", "max-west"),
    "actionmaxpacific.us": ("cinemax", "max-action-west"),
    "moremaxpacific.us": ("cinemax", "max-more-west"),
}


def fetch_wbd(brand, feed):
    """Fetch 7 days of listings from the WBD GraphQL endpoint.

    scheduledTimestamp is UTC ISO; scheduledDuration is seconds (string).
    No programme descriptions on this endpoint (titles/times only).
    """
    now = datetime.now(timezone.utc)
    payload = {
        "query": WBD_QUERY,
        "variables": {
            "brand": [brand],
            "feed": [feed],
            "startDate": now.isoformat(),
            "endDate": (now + timedelta(days=DAYS_AHEAD)).isoformat(),
            "count": 500,
        },
    }
    last = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(
                WBD_GRAPHQL,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json", "User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            data = json.loads(raw.decode("utf-8", "replace"))
            break
        except Exception as e:
            last = e
            time.sleep(5 * (attempt + 1))
    else:
        raise last
    progs = []
    for e in data.get("data", {}).get("getScheduleEntries", []):
        try:
            start = datetime.fromisoformat(
                e["scheduledTimestamp"].replace("Z", "+00:00"))
            dur = int(e.get("scheduledDuration") or 0)
            if dur <= 0:
                continue
            stop = start + timedelta(seconds=dur)
        except (ValueError, KeyError):
            continue
        title = ((e.get("title") or {}).get("en_US") or {}).get("full", "")
        if not title:
            continue
        progs.append({
            "start": start.isoformat(),
            "stop": stop.isoformat(),
            "title": title,
            "episode": "",
            "desc": "",
        })
    return progs, f"{WBD_GRAPHQL}#{brand}/{feed}"
# Official starz.com schedule. Codes extracted 2026-09-29 from Starz's own
# Next.js dropdown data (_app chunk). CRITICAL: InBlack = STZ3, NOT IND1
# (IND1 is IndiePlex). Full table in epg_build/starz_selector_research.md.
STARZ_CODES = {
    "STZ1": "STARZ",
    "STZ8": "STARZ Edge",
    "STZ3": "STARZ In Black",
    "STZ7": "STARZ Comedy",
    "STZ5": "STARZ Cinema",
    "STZ4": "STARZ Kids & Family",
    "ENC1": "STARZ Encore",
    "ACT1": "STARZ Encore Action",
    "LST1": "STARZ Encore Classic",
    "TST1": "STARZ Encore Black",
    "WAM1": "STARZ Encore Family",
    "MYS1": "STARZ Encore Suspense",
    "WST1": "STARZ Encore Westerns",
    "E2SP": "STARZ Encore Español",
    "PLX2": "MoviePlex",
    "IND1": "IndiePlex",
    "CLA1": "RetroPlex",
}


def fetch_starz_official(code):
    """Fetch one day's schedule from starz.com for a selector code.

    Data is embedded __NEXT_DATA__ -> props.pageProps.scheduleData with
    explicit ISO+offset times. The page's serviceId echoes the requested
    code -- if it doesn't match, the code fell back to STZ1 (reject).
    """
    today = datetime.now(ET)
    url = (f"https://www.starz.com/us/en/schedule/{code}/"
           f"{today:%Y/%m/%d}")
    html = fetch(url)
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                  html, re.S)
    if not m:
        raise ValueError("no __NEXT_DATA__ found")
    data = json.loads(m.group(1))
    pp = data.get("props", {}).get("pageProps", {})
    if pp.get("serviceId") != code:
        raise ValueError(f"serviceId mismatch: got {pp.get('serviceId')}, want {code}")
    progs = []
    for e in pp.get("scheduleData", []):
        progs.append({
            "start": e.get("start"),  # ISO with offset, e.g. 2026-09-28T23:59:00.000-04:00
            "stop": e.get("end"),
            "title": e.get("title", ""),
            "episode": "",
            "desc": " ".join(x for x in [
                e.get("logLine", ""),
                f"({e.get('releaseYear')})" if e.get("releaseYear") else "",
                e.get("ratingCode", ""),
            ] if x).strip(),
        })
    return progs, url


# ----------------------------------------------------------------- mapping
# chris_id -> (source_type, source_arg). Populated from the staged review.
# TVP-sourced cable feeds stay in update_tvpassport_schedule.py's domain;
# official-site fetchers get wired here when their extractors land.
SOURCES = {
    # TV Insider
    "betgospel.us": ("tvinsider", "bet-gospel"),
    "betsoul.us": ("tvinsider", "bet-soul"),
    # OnTVTonight
    "bether.us": ("ontvtonight", ("69022320", "bet-her")),
    # Pluto
    "m3u-usa-bet-classics": ("pluto", "60f85644a9493e0007a1f035"),
    "m3u-usa-bet-x-tyler-perry-drama": ("pluto", "666b38a5efa2a10008b15b3a"),
    "m3u-usa-bet-x-tyler-perry-comedy": ("pluto", "666b265722acab000885c6aa"),
    "m3u-usa-bet-throwbacks": ("pluto", "61326275b3c86a00078e4833"),
    "m3u-usa-bet-visionaries": ("pluto", "663946c1b18d700008d9c168"),
    # Starz official (codes verified 2026-09-29; InBlack = STZ3)
    "starz.us": ("starz", "STZ1"),
    "starzinblack.us": ("starz", "STZ3"),
}
# HBO/Cinemax via WBD GraphQL (feed codes verified 2026-09-29)
for _cid, _feed in HBO_CINEMAX_FEEDS.items():
    SOURCES[_cid] = ("wbd", _feed)


def load_review_extras():
    """Merge staged review mappings for tvinsider/ontvtonight/pluto sources.

    Every merged mapping runs through mapping_guards first -- a staged
    mapping that trips a guard is rejected with a logged reason, never
    silently applied.
    """
    from mapping_guards import check_mapping
    review_path = os.path.expanduser("~/workspace/tvp_cable_mapping_review.json")
    try:
        review = json.load(open(review_path))
    except Exception:
        return
    for net in review:
        for m in net.get("mappings", []):
            src = (m.get("source") or "").lower()
            cid = m.get("chris_id")
            if not cid or cid in SOURCES:
                continue
            url = m.get("source_url") or ""
            label = m.get("tvp_feed") or url
            ok, reason = check_mapping(cid, m.get("chris_name", ""), label,
                                      feed_network=net.get("network", ""))
            if not ok:
                log(f"GUARD REJECT {cid}: {reason}")
                continue
    for net in review:
        for m in net.get("mappings", []):
            src = (m.get("source") or "").lower()
            cid = m.get("chris_id")
            if not cid or cid in SOURCES:
                continue
            url = m.get("source_url") or ""
            if src == "tvinsider":
                slug = url.rstrip("/").split("/network/")[-1].split("/schedule")[0]
                SOURCES[cid] = ("tvinsider", slug)
            elif src == "ontvtonight":
                mm = re.search(r"/channel/(\d+)/([^/.]+)", url)
                if mm:
                    SOURCES[cid] = ("ontvtonight", (mm.group(1), mm.group(2)))
            elif src == "pluto":
                mm = re.search(r"/channels/([0-9a-f]+)", url)
                if mm:
                    SOURCES[cid] = ("pluto", mm.group(1))


def main():
    load_review_extras()
    log(f"{len(SOURCES)} cable sources configured")
    out = {"updated_utc": datetime.now(timezone.utc).isoformat(),
           "sources": {}}
    for cid, (stype, arg) in SOURCES.items():
        try:
            if stype == "tvinsider":
                progs, url = fetch_tvinsider(arg)
            elif stype == "ontvtonight":
                progs, url = fetch_ontvtonight(*arg)
            elif stype == "pluto":
                progs, url = fetch_pluto(arg)
            elif stype == "starz":
                progs, url = fetch_starz_official(arg)
            elif stype == "wbd":
                progs, url = fetch_wbd(*arg)
            else:
                continue
            # sanity: keep only future-ish programmes with real times
            progs = [p for p in progs if p.get("start") and p.get("stop") and p.get("title")]
            out["sources"][cid] = {"source": stype, "source_url": url,
                                   "programmes": progs}
            log(f"{cid}: {len(progs)} programmes via {stype}")
        except Exception as e:
            log(f"{cid}: FAILED ({e})")
        time.sleep(REQ_PAUSE)

    for path in (LOCAL_JSON, CI_JSON):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        json.dump(out, open(path, "w"), indent=1)
    log(f"wrote {LOCAL_JSON} + CI copy")

    # push to repo
    with open(LOCAL_JSON, "rb") as f:
        content_b64 = __import__("base64").b64encode(f.read()).decode()
    req = urllib.request.Request(f"{API}/contents/epg/cable_schedule.json")
    add_surrogate_to_request(req, CRED)
    try:
        with urllib.request.urlopen(req) as r:
            sha = json.loads(r.read())["sha"]
    except Exception:
        sha = None
    body = {"message": "Cable schedule update",
            "content": content_b64, "branch": BRANCH}
    if sha:
        body["sha"] = sha
    req = urllib.request.Request(
        f"{API}/contents/epg/cable_schedule.json",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="PUT")
    add_surrogate_to_request(req, CRED)
    resp = read_json_response(urllib.request.urlopen(req))
    log(f"pushed: {resp.get('commit', {}).get('sha', '')[:8]}")


if __name__ == "__main__":
    main()
