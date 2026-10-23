#!/usr/bin/env python3
"""Pull fresh provider EPG via the per-stream Xtream endpoint and write XMLTV.

CI-safe: credentials come ONLY from IPTV_HOST / IPTV_USER / IPTV_PASS env vars
(GitHub Actions secrets). Nothing is logged that contains credentials.

Resilient by design: the provider's JSON randomly truncates, so complete
objects are salvaged from every attempt and merged across attempts. If the
pull yields nothing usable, the script still exits 0 (printing
PROVIDER_EPG_OK=false) so the workflow can fall back to carried data.

Usage: python3 pull_provider_epg.py --out provider_epg_fresh.xml
"""
import base64
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from xml.sax.saxutils import escape


def q(s):
    return escape(s or "", {'"': "&quot;"})

HOST = os.environ.get("IPTV_HOST", "").strip().rstrip("/")
USER = os.environ.get("IPTV_USER", "").strip()
PASS = os.environ.get("IPTV_PASS", "").strip()
BUDGET_S = int(os.environ.get("PROVIDER_PULL_BUDGET_S", "18000"))  # 5h default
WORKERS = int(os.environ.get("PROVIDER_PULL_WORKERS", "6"))

OUT = "provider_epg_fresh.xml"
if "--out" in sys.argv:
    OUT = sys.argv[sys.argv.index("--out") + 1]

t0 = time.time()


def log(msg):
    print(f"[provider-pull] {msg}", flush=True)


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.read()
    except Exception as e:
        log(f"fetch error: {type(e).__name__}")
        return None


def salvage_stream_list(data):
    """Extract complete stream objects from a possibly-truncated top-level JSON array."""
    text = data.decode("utf-8", errors="ignore")
    try:
        j = json.loads(text)
        if isinstance(j, list):
            return [o for o in j if isinstance(o, dict) and "stream_id" in o]
    except json.JSONDecodeError:
        pass
    dec = json.JSONDecoder()
    objs = []
    idx, n = 0, len(text)
    while idx < n:
        if text[idx] != "{":
            idx += 1
            continue
        try:
            obj, end = dec.raw_decode(text, idx)
            if isinstance(obj, dict) and "stream_id" in obj:
                objs.append(obj)
            idx = end
        except json.JSONDecodeError:
            break  # truncation: the rest is garbage
    return objs


def salvage_listings(data):
    """Extract complete epg_listing objects from a possibly-truncated response."""
    text = data.decode("utf-8", errors="ignore")
    try:
        j = json.loads(text)
        if isinstance(j, dict):
            l = j.get("epg_listings", [])
            if isinstance(l, list):
                return [o for o in l if isinstance(o, dict) and "start" in o]
    except json.JSONDecodeError:
        pass
    # truncated: seek to the listings array, then decode complete objects
    dec = json.JSONDecoder()
    objs = []
    m = text.find('"epg_listings"')
    if m == -1:
        return objs
    idx = text.find("[", m)
    if idx == -1:
        return objs
    idx += 1
    n = len(text)
    while idx < n:
        while idx < n and text[idx] in " \t\r\n,":
            idx += 1
        if idx >= n or text[idx] == "]":
            break
        if text[idx] != "{":
            idx += 1
            continue
        try:
            obj, end = dec.raw_decode(text, idx)
            if isinstance(obj, dict) and "start" in obj:
                objs.append(obj)
            idx = end
        except json.JSONDecodeError:
            break  # truncation: the rest is garbage
    return objs


def b64d(s):
    if not s:
        return ""
    try:
        return base64.b64decode(s).decode("utf-8", errors="ignore")
    except Exception:
        return str(s)


def to_xmltv_ts(s):
    # "2026-09-23 07:45:00" -> "20260923074500 +0000" (provider wall-clock, v11 convention)
    s = (s or "").strip()
    if len(s) < 19:
        return ""
    d = s[:10].replace("-", "")
    t = s[11:19].replace(":", "")
    if len(d) != 8 or len(t) != 6 or not (d + t).isdigit():
        return ""
    return f"{d}{t} +0000"


def main():
    if not (HOST and USER and PASS):
        log("IPTV_HOST/IPTV_USER/IPTV_PASS not all set; skipping provider pull.")
        print("PROVIDER_EPG_OK=false")
        return 0
    base = f"{HOST}/player_api.php?username={USER}&password={PASS}"

    # --- phase 1: stream list (truncates; salvage across attempts) ---
    streams = {}
    no_new_rounds = 0
    for attempt in range(12):
        data = fetch(base + "&action=get_live_streams")
        if data:
            new = 0
            for o in salvage_stream_list(data):
                sid = o.get("stream_id")
                if sid is not None and sid not in streams:
                    streams[sid] = o
                    new += 1
            log(f"stream list attempt {attempt + 1}: +{new} new ({len(streams)} total)")
            no_new_rounds = 0 if new else no_new_rounds + 1
            if no_new_rounds >= 2 and streams:
                break
        time.sleep(2)
    if not streams:
        log("no streams recovered; nothing to pull.")
        print("PROVIDER_EPG_OK=false")
        return 0
    log(f"stream list complete: {len(streams)} streams")

    # --- phase 2: per-stream EPG ---
    sids = sorted(streams.keys())
    results = {}
    done = 0
    stop_submit = False

    def fetch_one(sid):
        url = base + f"&action=get_simple_data_table&stream_id={sid}"
        seen = {}
        for attempt in range(6):
            data = fetch(url)
            if data:
                listings = salvage_listings(data)
                for L in listings:
                    key = (L.get("start"), L.get("end"), L.get("title"))
                    if key not in seen:
                        seen[key] = L
                if isinstance(data, bytes) and data.rstrip().endswith(b"}"):
                    break
            time.sleep(1.5)
        out = []
        for L in seen.values():
            s, e = to_xmltv_ts(L.get("start")), to_xmltv_ts(L.get("end"))
            title = b64d(L.get("title")).strip()
            if not (s and e and title and s < e):
                continue
            out.append((s, e, title, b64d(L.get("description")).strip()))
        out.sort()
        return sid, out

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        it = iter(sids)
        # prime the pool
        for _ in range(WORKERS * 2):
            try:
                sid = next(it)
                futs[ex.submit(fetch_one, sid)] = sid
            except StopIteration:
                break
        while futs:
            for fut in list(futs):
                if fut.done():
                    sid, listings = fut.result()
                    meta = streams[sid]
                    eid = (meta.get("epg_channel_id") or "").strip() or f"stream-{sid}"
                    if eid not in results:
                        results[eid] = {
                            "name": (meta.get("name") or eid).strip(),
                            "icon": (meta.get("stream_icon") or "").strip(),
                            "listings": listings,
                        }
                    done += 1
                    del futs[fut]
                    if done % 100 == 0 or done == len(sids):
                        nprog = sum(len(r["listings"]) for r in results.values())
                        el = int(time.time() - t0)
                        log(f"{done}/{len(sids)} streams, {nprog} programmes, {el}s elapsed")
                    if time.time() - t0 > BUDGET_S:
                        stop_submit = True
                    if not stop_submit:
                        try:
                            nsid = next(it)
                            futs[ex.submit(fetch_one, nsid)] = nsid
                        except StopIteration:
                            pass
            time.sleep(0.2)

    with_data = sum(1 for r in results.values() if r["listings"])
    nprog = sum(len(r["listings"]) for r in results.values())
    log(f"pull done: {with_data}/{len(results)} channels with listings, {nprog} programmes")
    if nprog == 0:
        print("PROVIDER_EPG_OK=false")
        return 0

    # --- phase 3: write XMLTV ---
    with open(OUT, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')
        for eid in sorted(results):
            r = results[eid]
            f.write(f'  <channel id="{q(eid)}">\n')
            f.write(f'    <display-name>{escape(r["name"])}</display-name>\n')
            if r["icon"]:
                f.write(f'    <icon src="{q(r["icon"])}" />\n')
            f.write("  </channel>\n")
        for eid in sorted(results):
            for s, e, title, desc in results[eid]["listings"]:
                f.write(f'  <programme start="{s}" stop="{e}" channel="{q(eid)}">\n')
                f.write(f'    <title lang="en">{escape(title)}</title>\n')
                if desc:
                    f.write(f'    <desc lang="en">{escape(desc)}</desc>\n')
                f.write("  </programme>\n")
        f.write("</tv>\n")
    size = os.path.getsize(OUT)
    log(f"wrote {OUT} ({size} bytes)")
    print("PROVIDER_EPG_OK=true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
