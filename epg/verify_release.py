#!/usr/bin/env python3
"""verify_release.py — pass/fail proof sheet for a released EPG.

Downloads the live release assets (epg.xml + epg_247.xml) from
kamikaze0129/my-epg and audits them independently of the builder:
structural gates, NFL Sunday Ticket games, USA timezone-shift pairs,
real-vs-placeholder coverage, and spot checks (KGMB, KBOI, HBO West).

This is the receipt Chris asked for: never "trust me, it's in the
release" again — every claim below is measured from the actual bytes
TiviMate downloads.

Usage:
    python3 verify_release.py [--tag v2026.09.21-nfl1] [--out proof.md]
    Defaults to the latest release and writes
    ~/workspace/your_files/epg_proof_sheet.md.

Exit code 0 = every check passed, 1 = at least one FAILED.
"""

import argparse
import json
import os
import re
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import iterparse

REPO = "kamikaze0129/my-epg"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) EPGProofSheet/1.0"}

# ---- gates mirrored from the CI builder (epg/weekly_build.py as pushed) ----
# NOTE 2026-09-22: canonical and CI builders have DIVERGED. Canonical says
# KNOWN_PROGRAMMES=400000 (post fossil-drop); CI still has 561796 (+-5%).
# The release is built by CI, so the proof sheet mirrors CI's gates.
# Reconciling the builders is a Sunday audit item.
PLACEHOLDER_MARK = "No programme schedule was supplied"
CH_MIN, CH_MAX = int(10940 * 0.98), int(10940 * 1.02)
PR_MIN, PR_MAX = int(561796 * 0.95), int(561796 * 1.05)
ICON_MIN = 10400
STALE_MAX_FRAC = 0.02
TZ_OFFSETS = {"west": -3, "mountain": -2, "central": -1,
              "alaska": -4, "hawaii": -6}
TZ_MIN_RATIO = 0.8
ZONE_RE = re.compile(
    r"^USA\s+(.+?)\s+(East|West|Mountain|Central|Alaska|Hawaii)\s*\*?\s*$", re.I)
NFL_RE = re.compile(r"nfl-sunday-7\d\d")
EXPECTED_247_CHANNELS = 2757


def parse_ts(s):
    s = (s or "").strip()
    for fmt in ("%Y%m%d%H%M%S %z", "%Y%m%d%H%M%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def download(url, dest):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=180) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(4 * 1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
    return dest


def resolve_tag(tag):
    if tag != "latest":
        return tag
    url = f"https://api.github.com/repos/{REPO}/releases/latest"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)["tag_name"]


def scan(path):
    """One iterparse pass. Returns dict of everything the checks need."""
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=12)
    d = {
        "well_formed": True, "fatal": "",
        "channels": 0, "programmes": 0, "icons": 0,
        "dup_ids": 0, "orphans": 0, "missing_tt": 0,
        "bad_dur": 0, "bad_ts": 0, "stale": 0,
        "names": {},            # cid -> display name
        "max_stop": {},         # cid -> latest programme stop
        "progs": {},            # cid -> list of (start, stop, title, is_ph)
        "future_real": {},      # cid -> count of future real programmes
    }
    seen, prog_cids, dups = set(), set(), set()
    try:
        for event, elem in iterparse(path, events=("end",)):
            if elem.tag == "channel":
                d["channels"] += 1
                cid = elem.get("id")
                if cid in seen:
                    dups.add(cid)
                seen.add(cid)
                dn = elem.findtext("display-name") or ""
                d["names"][cid] = dn.strip()
                ie = elem.find("icon")
                if ie is not None and ie.get("src"):
                    d["icons"] += 1
                elem.clear()
            elif elem.tag == "programme":
                d["programmes"] += 1
                cid = elem.get("channel")
                prog_cids.add(cid)
                s, e = elem.get("start"), elem.get("stop")
                t = (elem.findtext("title") or "").strip()
                desc = elem.findtext("desc") or ""
                is_ph = PLACEHOLDER_MARK in desc
                if not (s and e and t):
                    d["missing_tt"] += 1
                else:
                    ds, de = parse_ts(s), parse_ts(e)
                    if not (ds and de):
                        d["bad_ts"] += 1
                    elif de <= ds:
                        d["bad_dur"] += 1
                    else:
                        prev = d["max_stop"].get(cid)
                        if prev is None or de > prev:
                            d["max_stop"][cid] = de
                        lst = d["progs"].setdefault(cid, [])
                        lst.append((ds, de, t, is_ph))
                        if de > now and not is_ph:
                            d["future_real"][cid] = d["future_real"].get(cid, 0) + 1
                elem.clear()
    except Exception as ex:  # noqa: BLE001
        d["well_formed"] = False
        d["fatal"] = f"XML not well-formed: {ex}"
        return d
    d["dup_ids"] = len(dups)
    d["orphans"] = len(prog_cids - seen)
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    d["stale"] = sum(1 for cid in seen
                     if d["max_stop"].get(cid, epoch) <= horizon)
    d["seen"] = seen
    return d


def check(name, ok, detail):
    return {"name": name, "ok": bool(ok), "detail": str(detail)}


def verify_main(path, nfl_expect=None):
    d = scan(path)
    out = []
    out.append(check("MAIN-01 XML well-formed", d["well_formed"], d["fatal"] or "ok"))
    if not d["well_formed"]:
        return out, d
    out.append(check("MAIN-02 channel count", CH_MIN <= d["channels"] <= CH_MAX,
                     f'{d["channels"]} (gate {CH_MIN}-{CH_MAX})'))
    out.append(check("MAIN-03 programme count", PR_MIN <= d["programmes"] <= PR_MAX,
                     f'{d["programmes"]} (gate {PR_MIN}-{PR_MAX})'))
    out.append(check("MAIN-04 duplicate channel ids", d["dup_ids"] == 0,
                     f'{d["dup_ids"]} dups'))
    out.append(check("MAIN-05 orphan programmes", d["orphans"] == 0,
                     f'{d["orphans"]} orphans'))
    out.append(check("MAIN-06 title/time/duration sanity",
                     d["missing_tt"] == 0 and d["bad_dur"] == 0 and d["bad_ts"] == 0,
                     f'missing={d["missing_tt"]} bad_dur={d["bad_dur"]} bad_ts={d["bad_ts"]}'))
    out.append(check("MAIN-07 icon coverage", d["icons"] >= ICON_MIN,
                     f'{d["icons"]} icons (min {ICON_MIN})'))
    stale_frac = d["stale"] / max(d["channels"], 1)
    out.append(check("MAIN-08 freshness (<=2% stale)", stale_frac <= STALE_MAX_FRAC,
                     f'{d["stale"]} stale / {d["channels"]} '
                     f'({stale_frac:.2%})'))
    n_real = sum(1 for c in d["seen"] if d["future_real"].get(c, 0) > 0)
    n_ph_only = sum(1 for c in d["seen"]
                    if c in d["progs"] and d["future_real"].get(c, 0) == 0)
    out.append(check("MAIN-09 real future coverage (report)",
                     True,
                     f'{n_real} channels with future real programmes, '
                     f'{n_ph_only} placeholder-only'))

    # ---- NFL Sunday Ticket ----
    nfl_cids = sorted(c for c in d["seen"] if NFL_RE.search(c))
    out.append(check("NFL-01 Sunday Ticket channels present", len(nfl_cids) == 13,
                     f'{len(nfl_cids)}/13: {", ".join(c.split("nfl-sunday-")[1][:3] for c in nfl_cids)}'))
    now = datetime.now(timezone.utc)
    nfl_games = 0
    nfl_detail = []
    for cid in nfl_cids:
        fut = [(s, t) for s, e, t, ph in d["progs"].get(cid, [])
               if e > now and not ph and "nfl" in t.lower()]
        if fut:
            nfl_games += 1
        else:
            num = cid.split("nfl-sunday-")[1][:3]
            nfl_detail.append(num)
    out.append(check("NFL-02 future NFL game on each channel", nfl_games == 13,
                     "all 13 have a game" if nfl_games == 13
                     else f"missing on {nfl_detail}"))
    if nfl_expect:
        mism = []
        for cid in nfl_cids:
            num = "nfl-sunday-" + cid.split("nfl-sunday-")[1][:3]
            exp = nfl_expect.get(num, "")
            fut = [t for _s, e, t, ph in d["progs"].get(cid, [])
                   if e > now and not ph]
            if exp and not any(exp.lower() in t.lower() for t in fut):
                mism.append(f"{num}: expected '{exp}'")
        out.append(check("NFL-03 games match expected schedule", not mism,
                         "all match" if not mism else "; ".join(mism)))

    # ---- USA timezone-shift pairs ----
    # Check every east/variant group; a group only counts as "ok" when the
    # east has future real data AND the variant matches >=80% shifted.
    # Groups whose east is currently empty are listed separately (stale east,
    # not a shift failure).
    groups = {}
    for cid, name in d["names"].items():
        m = ZONE_RE.match(name or "")
        if m:
            base = re.sub(r"\s+", " ", m.group(1)).strip().lower()
            groups.setdefault(base, {}).setdefault(m.group(2).lower(), []).append(cid)
    broken, ok_pairs, empty_east, total_pairs = [], 0, [], 0
    for base, zones in groups.items():
        if "east" not in zones:
            continue
        east_cid = max(zones["east"],
                       key=lambda c: d["future_real"].get(c, 0))
        east_real = [(s, t) for s, e, t, ph in d["progs"].get(east_cid, [])
                     if e > now and not ph]
        if not east_real:
            empty_east.append(f"{base} (east {east_cid} has no future real)")
            continue
        for zone, off in TZ_OFFSETS.items():
            if zone not in zones:
                continue
            for var_cid in zones[zone]:
                total_pairs += 1
                var_set = {(t, s) for s, _e, t, ph
                           in d["progs"].get(var_cid, []) if not ph}
                delta = timedelta(hours=off)
                match = sum(1 for s, t in east_real if (t, s + delta) in var_set)
                if match / len(east_real) >= TZ_MIN_RATIO:
                    ok_pairs += 1
                else:
                    broken.append(f"{base} {zone} ({var_cid}): "
                                  f"{match}/{len(east_real)}")
    tz_detail = f"{ok_pairs}/{total_pairs} pairs ok"
    if empty_east:
        tz_detail += f"; {len(empty_east)} groups with empty east"
    out.append(check("TZ-01 zone pairs >=80% shifted match", not broken,
                     tz_detail if not broken
                     else tz_detail + " | BROKEN: " + "; ".join(broken[:6])))

    # ---- HBO West specifically ----
    hbw = [c for c in d["seen"] if c == "hbowest.us"]
    hbo_detail, hbo_ok = "hbowest.us not in release", False
    if hbw:
        east_cands = [c for c, n in d["names"].items()
                      if re.match(r"^USA\s+HBO\s+East", n or "", re.I)]
        if east_cands:
            east_cid = max(east_cands, key=lambda c: d["future_real"].get(c, 0))
            east_real = [(s, t) for s, e, t, ph in d["progs"].get(east_cid, [])
                         if e > now and not ph]
            var_set = {(t, s) for s, _e, t, ph
                       in d["progs"].get("hbowest.us", []) if not ph}
            delta = timedelta(hours=-3)
            match = sum(1 for s, t in east_real if (t, s + delta) in var_set)
            hbo_ok = bool(east_real) and match / len(east_real) >= TZ_MIN_RATIO
            hbo_detail = (f"east={east_cid} ({len(east_real)} future real), "
                          f"west match {match}/{len(east_real)}")
    out.append(check("TZ-02 HBO West = HBO East -3h", hbo_ok, hbo_detail))

    # ---- spot checks ----
    kgmb = "cbs5kgmb.us"
    kgmb_n = d["future_real"].get(kgmb, 0)
    out.append(check("SPOT-01 KGMB Honolulu real future",
                     kgmb_n > 0,
                     f"{kgmb_n} future real programmes" if kgmb in d["seen"]
                     else "cbs5kgmb.us not in release"))
    kboi_cids = [c for c in d["seen"] if "kboi" in c.lower()]
    kboi_n = sum(d["future_real"].get(c, 0) for c in kboi_cids)
    out.append(check("SPOT-02 KBOI real future", kboi_n > 0,
                     f"{kboi_n} future real across {kboi_cids}"
                     if kboi_cids else "no KBOI channel in release"))
    return out, d


def verify_247(path):
    d = scan(path)
    out = []
    out.append(check("247-01 XML well-formed", d["well_formed"], d["fatal"] or "ok"))
    if not d["well_formed"]:
        return out
    lo, hi = int(EXPECTED_247_CHANNELS * 0.99), int(EXPECTED_247_CHANNELS * 1.01)
    out.append(check("247-02 channel count", lo <= d["channels"] <= hi,
                     f'{d["channels"]} (expected ~{EXPECTED_247_CHANNELS})'))
    out.append(check("247-03 programmes present", d["programmes"] > 100000,
                     f'{d["programmes"]} programmes'))
    out.append(check("247-04 duplicate channel ids", d["dup_ids"] == 0,
                     f'{d["dup_ids"]} dups'))
    out.append(check("247-05 icons", d["icons"] > 2000,
                     f'{d["icons"]} icons'))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="latest",
                    help="release tag (default: latest)")
    ap.add_argument("--out", default=os.path.expanduser(
        "~/workspace/your_files/epg_proof_sheet.md"))
    args = ap.parse_args()

    tag = resolve_tag(args.tag)
    print(f"proof sheet for release {tag}")

    tmp = tempfile.mkdtemp(prefix="proof_")
    main_path = os.path.join(tmp, "epg.xml")
    t247_path = os.path.join(tmp, "epg_247.xml")
    base = f"https://github.com/{REPO}/releases/download/{tag}"
    print("downloading epg.xml ...", flush=True)
    download(base + "/epg.xml", main_path)
    print("downloading epg_247.xml ...", flush=True)
    download(base + "/epg_247.xml", t247_path)

    # expected NFL games from the local schedule (best effort)
    nfl_expect = {}
    try:
        sched = json.load(open(os.path.expanduser(
            "~/workspace/epg_build/nfl_sunday_ticket.json")))
        for cid, plist in sched.items():
            if cid.startswith("_"):
                continue
            m = re.search(r"nfl-sunday-(7\d\d)", cid)
            if not m:
                continue
            titles = re.findall(r"<title>([^<]*)</title>", "".join(plist))
            if titles:
                # "NFL Football: Lions vs Bills" -> "lions vs bills"
                t = titles[0].lower().replace("nfl football:", "").strip()
                nfl_expect["nfl-sunday-" + m.group(1)] = t
    except Exception as ex:  # noqa: BLE001
        print(f"(no local NFL schedule for cross-check: {ex})")

    print("auditing epg.xml ...", flush=True)
    main_checks, _d = verify_main(main_path, nfl_expect or None)
    print("auditing epg_247.xml ...", flush=True)
    checks_247 = verify_247(t247_path)

    all_checks = main_checks + checks_247
    passed = sum(1 for c in all_checks if c["ok"])
    failed = [c for c in all_checks if not c["ok"]]
    verdict = "PASS" if not failed else "FAIL"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [f"# EPG proof sheet — {tag}",
             f"_Generated {stamp} from the live release assets "
             f"(`releases/download/{tag}/epg.xml`, `epg_247.xml`)._",
             "",
             f"## Verdict: {verdict} ({passed}/{len(all_checks)} checks)",
             ""]
    if failed:
        lines += ["### FAILED", ""]
        for c in failed:
            lines.append(f"- **{c['name']}** — {c['detail']}")
        lines.append("")
    lines += ["### All checks", "",
              "| Check | Result | Detail |",
              "|---|---|---|"]
    for c in all_checks:
        mark = "✅ PASS" if c["ok"] else "❌ FAIL"
        lines.append(f"| {c['name']} | {mark} | {c['detail']} |")
    lines += ["",
              "_Gates mirror the CI builder (epg/weekly_build.py as released): "
              "channels 10940±2%, programmes 561796±5%, icons ≥10400, "
              "stale ≤2%, zone pairs ≥80% shifted match. Placeholders are the "
              "honest 'No programme schedule was supplied' blocks._",
              ""]
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n{verdict}: {passed}/{len(all_checks)} passed -> {args.out}")
    for c in failed:
        print(f"  FAIL {c['name']}: {c['detail']}")
    return 0 if not failed else 1


if __name__ == "--main__":
    pass
if __name__ == "__main__":
    sys.exit(main())
