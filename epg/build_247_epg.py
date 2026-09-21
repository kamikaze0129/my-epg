#!/usr/bin/env python3
"""Build a standalone 24/7 marathon EPG from the service XML.

Source: the service's XMLTV (repaired_epg.xml from the provider), which maps
each 24/7 marathon channel into 30-min / 60-min / 120-min programme blocks,
each titled with the show's name.

Output: a separate XMLTV file containing ONLY the 24/7 channels, with rolling
7-day programme grids in the same block format (block size is per-channel,
taken from the service data). Icons are pulled from the production epg.xml
where the channel IDs match.

Usage: python3 build_247_epg.py [--service-xml PATH] [--prod-epg PATH] [--out PATH] [--days N]
"""
import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape, quoteattr

TS_FMT = "%Y%m%d%H%M%S +0000"
JUNK_DESCS = {"programming.", "programming", "no information.", "no information", ""}

def parse_service_channels(data):
    """id -> (display_names, is_247)"""
    out = {}
    for m in re.finditer(r'<channel id="([^"]+)">(.*?)</channel>', data, re.S):
        cid, inner = m.group(1), m.group(2)
        names = re.findall(r'<display-name[^>]*>([^<]*)</display-name>', inner)
        is247 = cid.startswith("m3u-247-") or any("24/7" in n for n in names)
        out[cid] = (names, is247)
    return out

def parse_service_programmes(data, wanted):
    """Per-channel: durations, titles, descs for wanted channel ids."""
    pat = re.compile(r'<programme start="(\d{14})[^"]*" stop="(\d{14})[^"]*" channel="([^"]+)">', re.S)
    tpat = re.compile(r'<title[^>]*>([^<]*)</title>')
    dpat = re.compile(r'<desc[^>]*>([^<]*)</desc>')
    stats = {cid: {"dur": Counter(), "title": Counter(), "desc": Counter()} for cid in wanted}
    for m in pat.finditer(data):
        cid = m.group(3)
        if cid not in stats:
            continue
        s = stats[cid]
        mins = (datetime.strptime(m.group(2), "%Y%m%d%H%M%S")
                - datetime.strptime(m.group(1), "%Y%m%d%H%M%S")).total_seconds() / 60
        if mins <= 700:  # ignore giant placeholder blocks for sizing
            s["dur"][round(mins)] += 1
        end = data.find('</programme>', m.end())
        inner = data[m.end():end]
        t = tpat.search(inner)
        if t and t.group(1).strip():
            s["title"][t.group(1).strip()] += 1
        d = dpat.search(inner)
        if d and d.group(1).strip():
            s["desc"][d.group(1).strip()] += 1
    return stats

def parse_prod_icons(path):
    icons = {}
    names = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        data = f.read()
    for m in re.finditer(r'<channel id="([^"]+)">(.*?)</channel>', data, re.S):
        cid, inner = m.group(1), m.group(2)
        im = re.search(r'<icon src="([^"]+)"', inner)
        if im:
            icons[cid] = im.group(1)
        nm = re.findall(r'<display-name[^>]*>([^<]*)</display-name>', inner)
        if nm:
            names[cid] = nm
    return icons, names

def clean_title_for_desc(title):
    return title

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--service-xml", default="/tmp/repaired_epg/repaired_epg.xml")
    ap.add_argument("--prod-epg", default="/home/hatch/workspace/your_files/epg.xml")
    ap.add_argument("--manifest", default=None,
                    help="Build from a compact 247 manifest JSON instead of parsing the service XML "
                         "(CI mode: no 92MB service file needed).")
    ap.add_argument("--out", default="/home/hatch/workspace/your_files/epg_247.xml")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--emit-manifest", default=None,
                    help="Write a compact manifest JSON alongside the build.")
    args = ap.parse_args()

    if args.manifest:
        print("[1/3] loading manifest...", flush=True)
        manifest = json.load(open(args.manifest, encoding="utf-8"))
        print(f"      24/7 channels in manifest: {len(manifest)}", flush=True)
        wanted = None
    else:
        print("[1/4] parsing service XML...", flush=True)
        with open(args.service_xml, encoding="utf-8", errors="replace") as f:
            svc = f.read()
        svc_channels = parse_service_channels(svc)
        wanted = [cid for cid, (_, is247) in svc_channels.items() if is247]
        print(f"      24/7 channels in service XML: {len(wanted)}", flush=True)

        print("[2/4] parsing service programmes + production icons...", flush=True)
        stats = parse_service_programmes(svc, set(wanted))
        icons, prod_names = parse_prod_icons(args.prod_epg)
        manifest = []
        for cid in sorted(wanted):
            svc_names, _ = svc_channels[cid]
            st = stats[cid]
            block_mins = st["dur"].most_common(1)[0][0] if st["dur"] else 60
            title = st["title"].most_common(1)[0][0] if st["title"] else None
            if not title:
                dn = svc_names[0] if svc_names else cid
                title = re.sub(r"^24/7\s+", "", dn).strip() or cid
            desc = st["desc"].most_common(1)[0][0] if st["desc"] else ""
            if desc.strip().lower() in JUNK_DESCS:
                desc = f"{title}."
            disp = svc_names or prod_names.get(cid, [title])
            manifest.append({"id": cid, "names": disp,
                             "icon": icons.get(cid),
                             "block_mins": block_mins, "title": title, "desc": desc})
        if args.emit_manifest:
            json.dump(manifest, open(args.emit_manifest, "w", encoding="utf-8"))
            print(f"      manifest written to {args.emit_manifest}", flush=True)

    anchor = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end = anchor + timedelta(days=args.days)

    channels_xml = []
    programmes_xml = []
    n_prog = 0
    n_icon = 0
    for entry in manifest:
        cid = entry["id"]
        block_mins = entry["block_mins"]
        title = entry["title"]
        desc = entry["desc"]
        icon = entry.get("icon")
        if icon:
            n_icon += 1
        ch = [f"  <channel id={quoteattr(cid)}>"]
        for dn in entry["names"]:
            ch.append(f"    <display-name>{escape(dn)}</display-name>")
        if icon:
            ch.append(f"    <icon src={quoteattr(icon)} />")
        ch.append("  </channel>")
        channels_xml.append("\n".join(ch))
        t = anchor
        while t < end:
            stop = min(t + timedelta(minutes=block_mins), end)
            programmes_xml.append(
                f'  <programme start="{t.strftime(TS_FMT)}" stop="{stop.strftime(TS_FMT)}" channel={quoteattr(cid)}>\n'
                f"    <title>{escape(title)}</title>\n"
                f"    <desc>{escape(desc)}</desc>\n"
                f"  </programme>"
            )
            n_prog += 1
            t = stop

    step = "[2/3]" if args.manifest else "[3/4]"
    print(f"{step} writing {args.out} ...", flush=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        f.write("<tv>\n")
        f.write("\n".join(channels_xml))
        f.write("\n")
        f.write("\n".join(programmes_xml))
        f.write("\n</tv>\n")

    step = "[3/3]" if args.manifest else "[4/4]"
    print(f"{step} validating...", flush=True)
    import xml.etree.ElementTree as ET
    tree = ET.parse(args.out)
    root = tree.getroot()
    ch_ids = [c.get("id") for c in root.findall("channel")]
    progs = root.findall("programme")
    assert len(ch_ids) == len(set(ch_ids)), "duplicate channel ids!"
    assert len(ch_ids) == len(manifest), f"channel count mismatch {len(ch_ids)} vs {len(manifest)}"
    prog_ch = Counter(p.get("channel") for p in progs)
    missing = [e["id"] for e in manifest if prog_ch[e["id"]] == 0]
    assert not missing, f"{len(missing)} channels without programmes"
    short = 0
    for e in manifest:
        cid = e["id"]
        stops = sorted(p.get("stop") for p in progs if p.get("channel") == cid)
        if stops[-1].replace(" +0000", "") < end.strftime("%Y%m%d%H%M%S"):
            short += 1
    print(f"      channels={len(ch_ids)} programmes={len(progs)} icons={n_icon}")
    print(f"      window: {anchor.strftime(TS_FMT)} -> {end.strftime(TS_FMT)}")
    print(f"      channels missing programmes: {len(missing)}, short of window: {short}")
    assert short == 0, "some channels don't cover the full window"
    print("      OK - all gates passed")

if __name__ == "__main__":
    main()
