#!/usr/bin/env python3
"""Weekly EPG rebuild orchestrator.

Reproduces the curated final build (10,940 channels / ~485k programmes /
10,550 icons) from fresh weekly data, then validates with hard gates.
Only a fully-validated file is promoted to ~/workspace/your_files/epg.xml.

PIPELINE (reconstructed from the build scripts in this directory):  1. epg_logos4.xml  -- base: 386 missing tvg-ids integrated + logo waves 1-4
                       (one-time curation; the channel ROSTER is stable and is
                       reloaded from the previous final build each week)
  2. apply_pubfeed.py (batch 1: 26 verified epg.pw matches, DE/BR/IN/FR/AU/GB)
     apply_pubfeed2.py (batch 2: 130 verified same-country exact-after-strip)
  3. apply_pubfeed3..13.py (batches 3-13: 50 verified UK/CA/US focus matches)
  4. 24/7 poster application (1,788 icons, zero overwrites -- logos247_results.json)
  => epg_logos19.xml (10,940 channels, 485,477 programmes, 10,550 with icons)

WEEKLY REFRESH SOURCES (what is actually re-fetchable):
  - epg.pw public country feeds (https://epg.pw/xmltv/epg_{CC}.xml.gz):
    fresh ~1-4 day windows for the 206 verified replacement channels.
    Because the windows are short, the automation runs DAILY (11:00 UTC);
    a weekly cadence would leave these channels stale 4-6 days a week.
  - 24/7 synthetic marathon grids: openly synthetic filler; shifted forward so
    the grid re-anchors at build time every run (daily re-anchor keeps the
    ~2.5-day grids perpetually fresh; NOT extended to 7 days -- that would
    nearly double the file for zero user benefit).
  - Placeholder "no schedule" blocks: honest timeless text ("No programme
    schedule was supplied..."), regenerated every run with rolling dates
    (anchor -> anchor+7d) so they never expire into "No information".
  - Regular channels: carried over (past programmes included, keeps counts
    stable), OR refreshed from a provider XMLTV file if one is supplied via
    --service-xml / $EPG_SERVICE_XML (tvg-id match). Regulars with nothing
    left in the future get one rolling honest placeholder block.

NOT re-run weekly (documented dead ends, kept local-only, never published):
  - fetch_stream_epg.py / salvage_cats.py / target_streams.py: the provider
    per-stream EPG endpoint returns zero listings; the category->stream map
    is stable. The provider xmltv.php endpoint refuses connections and the
    bulk service XML (service_epg_full.xml) was a user-provided file, so bulk
    provider programmes are not re-fetchable by automation.

NEVER logs secrets: the only network fetch here is the public epg.pw feeds.

CI MODE (GitHub Actions): set EPG_PREV_BUILD to the previous epg.xml
(downloaded from the latest release), EPG_RUNS_DIR to a workspace dir, and
pass --no-promote --out <path> so the validated build lands at a known path
for the release step. No credentials needed: feeds are public.
"""
import gzip
import json
import os
import re
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from xml.etree.ElementTree import iterparse
from xml.sax.saxutils import escape

# ---------------------------------------------------------------- config
BUILD_DIR = os.path.dirname(os.path.abspath(__file__))
# Env overrides let this run on a stateless CI runner (e.g. GitHub Actions):
# EPG_PREV_BUILD = previous final epg.xml (downloaded from latest release),
# EPG_RUNS_DIR  = where run reports/backups go (repo-local on CI).
PREV_BUILD = os.environ.get('EPG_PREV_BUILD',
                            os.path.expanduser('~/workspace/your_files/epg.xml'))
PUBFEED_DIR = os.path.join(BUILD_DIR, 'pubfeed')
HIDDEN_RUNS = os.environ.get('EPG_RUNS_DIR', os.path.join(BUILD_DIR, 'hidden_runs'))
BACKUP_DIR = os.path.join(HIDDEN_RUNS, 'backups')
LOGOS247 = os.path.join(BUILD_DIR, 'logos247_results.json')

FEED_CCS = ['DE', 'BR', 'AU', 'CA', 'GB', 'FR', 'IN', 'US', 'ID']
FEED_URL = 'https://epg.pw/xmltv/epg_{cc}.xml.gz'

# Known-good build this pipeline reproduces.
KNOWN_CHANNELS = 10940
KNOWN_PROGRAMMES = 485477
KNOWN_ICONS = 10550

# Hard gates.
CH_MIN = int(KNOWN_CHANNELS * 0.98)
CH_MAX = int(KNOWN_CHANNELS * 1.02)
PR_MIN = int(KNOWN_PROGRAMMES * 0.95)
PR_MAX = int(KNOWN_PROGRAMMES * 1.05)
ICON_MIN = 10400  # known-good is 10550; never regress below 10400

PLACEHOLDER_MARK = 'No programme schedule was supplied'
Q = {'"': '&quot;'}

# Rolling "no data" blocks: openly honest placeholders, regenerated every run
# with dates anchored at build time so they never expire into "No information".
# Text deliberately states that no schedule was supplied -- never a fake show.
PLACEHOLDER_TITLE = 'Programming'
PLACEHOLDER_DESC = ('No programme schedule was supplied for this channel '
                    'in the source EPG.')
PLACEHOLDER_DAYS = 7


def is_placeholder_prog(elem):
    """True if this <programme> is one of our honest no-schedule blocks."""
    return PLACEHOLDER_MARK in (elem.findtext('desc') or '')


def rolling_placeholder(cid, anchor):
    """One placeholder block spanning anchor -> anchor+7d for channel cid."""
    return serialize_fresh_prog(fmt_ts(anchor),
                                fmt_ts(anchor + timedelta(days=PLACEHOLDER_DAYS)),
                                cid, PLACEHOLDER_TITLE, PLACEHOLDER_DESC)

# Batch order for the verified pubfeed set (later batches override earlier).
FOCUS_BATCH_FILES = (
    ['focus_uk_ca_us_matches.json'] +
    [f'focus_batch{n}_matches.json' for n in (4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)]
)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def esc_q(v):
    return escape(v or '', Q)


# ---------------------------------------------------------------- time helpers
def parse_ts(s):
    s = (s or '').strip()
    for fmt in ('%Y%m%d%H%M%S %z', '%Y%m%d%H%M%S'):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def fmt_ts(dt):
    return dt.astimezone(timezone.utc).strftime('%Y%m%d%H%M%S +0000')


def shift_ts_str(s, delta):
    dt = parse_ts(s)
    return fmt_ts(dt + delta) if dt else s


# ---------------------------------------------------------------- serializers
def serialize_channel(cid, name, icon):
    out = [f'  <channel id="{esc_q(cid)}">\n',
           f'    <display-name>{escape(name or "")}</display-name>\n']
    if icon:
        out.append(f'    <icon src="{esc_q(icon)}" />\n')
    out.append('  </channel>\n')
    return ''.join(out)


def serialize_programme(el, channel=None, start=None, stop=None, delta=None):
    """Re-serialize a parsed <programme>, optionally remapping channel id
    and shifting all timestamps by delta (timedelta)."""
    s = start or el.get('start') or ''
    e = stop or el.get('stop') or ''
    if delta:
        s = shift_ts_str(s, delta)
        e = shift_ts_str(e, delta)
    at = [f'start="{esc_q(s)}"', f'stop="{esc_q(e)}"',
          f'channel="{esc_q(channel or el.get("channel") or "")}"']
    for k in ('start_timestamp', 'stop_timestamp'):
        v = el.get(k)
        if v:
            if delta:
                # epoch-int timestamps: shift by the same delta
                try:
                    v = str(int(v) + int(delta.total_seconds()))
                except ValueError:
                    pass
            at.append(f'{k}="{esc_q(v)}"')
    out = ['  <programme ' + ' '.join(at) + '>\n']
    for child in el:
        lang = child.get('lang')
        lang_s = f' lang="{esc_q(lang)}"' if lang else ''
        out.append(f'    <{child.tag}{lang_s}>{escape(child.text or "")}'
                   f'</{child.tag}>\n')
    out.append('  </programme>\n')
    return ''.join(out)


def serialize_fresh_prog(start, stop, channel, title, desc):
    """Fresh programme in the exact format the apply_pubfeed*.py scripts used."""
    out = [f'  <programme start="{esc_q(start)}" stop="{esc_q(stop)}" '
           f'channel="{esc_q(channel)}">\n',
           f'    <title lang="en">{escape(title)}</title>\n']
    if desc:
        out.append(f'    <desc lang="en">{escape(desc)}</desc>\n')
    out.append('  </programme>\n')
    return ''.join(out)

# ---------------------------------------------------------------- fetch
def download(url, dest, retries=3):
    last = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'muse-epg-weekly/1.0'})
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, 'wb') as f:
                shutil.copyfileobj(r, f)
            return True
        except Exception as e:
            last = e
            log(f"download attempt {attempt}/{retries} failed for {url}: {type(e).__name__}")
            time.sleep(3 * attempt)
    log(f"FATAL: could not download {url}: {last}")
    return False


def fetch_feeds(workdir):
    """Download fresh epg.pw country feeds. Any failure aborts the run
    (fail-safe: never ship silently-stale verified channels)."""
    feed_dir = os.path.join(workdir, 'pubfeed')
    os.makedirs(feed_dir, exist_ok=True)
    paths = {}
    for cc in FEED_CCS:
        url = FEED_URL.format(cc=cc)
        dest = os.path.join(feed_dir, f'epg_{cc}.xml.gz')
        log(f"fetching {cc} feed ...")
        if not download(url, dest):
            raise RuntimeError(f"feed download failed for {cc}; aborting run")
        # sanity: must be a parseable gzip with a <tv> root
        try:
            with gzip.open(dest, 'rb') as f:
                head = f.read(200)
            assert b'<tv' in head
        except Exception as e:
            raise RuntimeError(f"feed {cc} downloaded but is not valid gzip/XMLTV: {e}")
        paths[cc] = dest
        log(f"  {cc}: {os.path.getsize(dest)} bytes")
    return paths


# ---------------------------------------------------------------- roster
def load_roster(prev_path):
    """Parse the previous final build: channel roster + classification.

    Returns (roster, classes) where roster[chan_id] = (display_name, icon_src)
    and classes maps chan_id -> one of:
      'verified247' handled separately; values: 'v247' (24/7 synthetic grid),
      'placeholder' (all programmes are timeless placeholders),
      'regular' (everything else; verified set applied afterwards by id).
    Also returns min_start_247: {chan_id: earliest programme start (datetime)}
    for grid shifting.
    """
    roster = {}
    order = []
    prog_stats = {}   # chan_id -> [total, placeholder_count]
    min_start_247 = {}
    for event, elem in iterparse(prev_path, events=('end',)):
        if elem.tag == 'channel':
            cid = elem.get('id')
            if cid not in roster:
                roster[cid] = (elem.findtext('display-name') or '',
                               (elem.find('icon').get('src')
                                if elem.find('icon') is not None else ''))
                order.append(cid)
            elem.clear()
        elif elem.tag == 'programme':
            c = elem.get('channel')
            st = prog_stats.setdefault(c, [0, 0])
            st[0] += 1
            if PLACEHOLDER_MARK in (elem.findtext('desc') or ''):
                st[1] += 1
            name = roster.get(c, ('', ''))[0] if c in roster else ''
            if name.startswith('24/7'):
                dt = parse_ts(elem.get('start') or '')
                if dt and (c not in min_start_247 or dt < min_start_247[c]):
                    min_start_247[c] = dt
            elem.clear()
    classes = {}
    for cid in order:
        name = roster[cid][0]
        if name.startswith('24/7'):
            classes[cid] = 'v247'
        else:
            tot, ph = prog_stats.get(cid, (0, 0))
            classes[cid] = 'placeholder' if (tot and ph == tot) else 'regular'
    log(f"roster: {len(roster)} channels "
        f"({sum(1 for v in classes.values() if v == 'v247')} 24/7, "
        f"{sum(1 for v in classes.values() if v == 'placeholder')} placeholder, "
        f"{sum(1 for v in classes.values() if v == 'regular')} regular)")
    return roster, order, classes, min_start_247


# ---------------------------------------------------------------- verified matches
def load_verified_matches():
    """The locked verified set: batch 1 (26) + batch 2 (130) + focus (50).

    Returns {target_id: (feed_channel_id, feed_country)}; later batches win.
    """
    matches = {}

    d1 = json.load(open(os.path.join(PUBFEED_DIR, 'pubfeed_matches.json')))
    for m in d1['matches']:
        matches[m['target_id']] = (m['feed_channel_id'], m['feed_country'])
    log(f"batch1 verified: {len(d1['matches'])}")

    cands = {c['target_id']: c for c in d1['unvalidated_name_candidates']}
    applied2 = json.load(open(os.path.join(PUBFEED_DIR, 'applied_batch2.json')))
    n2 = 0
    for tid in applied2:
        c = cands.get(tid)
        if c:
            matches[tid] = (c['feed_channel_id'], c['feed_country'])
            n2 += 1
    log(f"batch2 verified: {n2}")

    n3 = 0
    for fname in FOCUS_BATCH_FILES:
        path = os.path.join(PUBFEED_DIR, fname)
        if not os.path.isfile(path):
            continue
        for m in json.load(open(path)):
            if isinstance(m, dict) and m.get('target_id'):
                matches[m['target_id']] = (m['feed_channel_id'], m['feed_country'])
                n3 += 1
    log(f"focus batches verified entries: {n3}")
    log(f"verified set total (unique target_ids): {len(matches)}")
    return matches


def extract_feed_programmes(matches, feed_paths, cutoff14):
    """Pull fresh programmes for verified targets from fresh feeds.

    Same semantics as apply_pubfeed*.py: valid times, non-empty title,
    stop after cutoff, sorted by start, >=5 programmes or the channel is
    skipped (keeps its previous programmes).
    Returns ({target_id: [xml,...]}, skipped:[(target_id, n)]).
    """
    fresh = {}
    skipped = []
    # group targets by feed file for single-pass extraction
    by_feed = {}
    for tid, (fcid, fcc) in matches.items():
        by_feed.setdefault(fcc, []).append((tid, fcid))
    for fcc, pairs in by_feed.items():
        want = {fcid: tid for tid, fcid in pairs}
        buckets = {tid: [] for tid, _ in pairs}
        with gzip.open(feed_paths[fcc], 'rb') as f:
            for event, elem in iterparse(f, events=('end',)):
                if elem.tag == 'programme':
                    fcid = elem.get('channel')
                    if fcid in want:
                        s, e = elem.get('start'), elem.get('stop')
                        t = (elem.findtext('title') or '').strip()
                        ds = (elem.findtext('desc') or '').strip()
                        if s and e and t and s[:14] < e[:14] and e[:14] > cutoff14:
                            buckets[want[fcid]].append(
                                (s, serialize_fresh_prog(s, e, want[fcid], t, ds)))
                    elem.clear()
        for tid, rows in buckets.items():
            rows.sort(key=lambda x: x[0])
            if len(rows) >= 5:
                fresh[tid] = [p for _, p in rows]
            else:
                skipped.append((tid, len(rows)))
    log(f"feed extraction: {len(fresh)} channels refreshed, "
        f"{sum(len(v) for v in fresh.values())} programmes; "
        f"skipped (<5 progs): {len(skipped)}")
    for tid, n in skipped[:10]:
        log(f"   skip {tid}: {n} programmes in fresh feed")
    return fresh, skipped


# ---------------------------------------------------------------- optional provider XML hook
def load_service_xml(path):
    """Tolerant parse of a provider XMLTV file -> {tvg_id: [programme xml]}.

    The provider's file may be truncated; complete <programme> elements are
    salvaged with regex and programmes are keyed by their channel="tvg-id".
    """
    if not path or not os.path.isfile(path):
        return {}
    log(f"loading service XML hook: {path}")
    txt = open(path, encoding='utf-8', errors='ignore').read()
    progs = {}
    n = 0
    for m in re.finditer(r'<programme\b([^>]*)>(.*?)</programme>', txt, re.S):
        attrs, inner = m.group(1), m.group(2)
        ch = re.search(r'channel="([^"]*)"', attrs)
        s = re.search(r'start="([^"]*)"', attrs)
        e = re.search(r'stop="([^"]*)"', attrs)
        if not (ch and s and e):
            continue
        cid, start, stop = ch.group(1), s.group(1), e.group(1)
        if not (cid and start and stop and start[:14] < stop[:14]):
            continue
        tm = re.search(r'<title[^>]*>(.*?)</title>', inner, re.S)
        dm = re.search(r'<desc[^>]*>(.*?)</desc>', inner, re.S)
        title = (tm.group(1).strip() if tm else '')
        if not title:
            continue
        desc = dm.group(1).strip() if dm else ''
        ts = ''
        for k in ('start_timestamp', 'stop_timestamp'):
            mm = re.search(k + r'="([^"]*)"', attrs)
            if mm:
                ts += f' {k}="{esc_q(mm.group(1))}"'
        xml = (f'  <programme start="{esc_q(start)}" stop="{esc_q(stop)}"{ts} '
               f'channel="{esc_q(cid)}">\n'
               f'    <title lang="en">{escape(title)}</title>\n')
        if desc:
            xml += f'    <desc lang="en">{escape(desc)}</desc>\n'
        xml += '  </programme>\n'
        progs.setdefault(cid, []).append((start, xml))
        n += 1
    for cid in progs:
        progs[cid].sort(key=lambda x: x[0])
        progs[cid] = [p for _, p in progs[cid]]
    log(f"service XML: {n} programmes across {len(progs)} tvg-ids")
    return progs

# ---------------------------------------------------------------- build
def build_output(prev_path, out_path, roster, order, classes, min_start_247,
                 verified_fresh, service_progs, anchor):
    """Stream the previous build, replacing programmes per classification.

    - verified targets in verified_fresh: drop old, fresh appended afterwards
    - verified targets skipped (<5 fresh progs): keep old programmes
    - v247: shift every programme grid so it starts at `anchor`. The grids are
      ~2.5 days of openly synthetic marathon filler (206k programmes); they are
      deliberately NOT extended to 7 days because that would nearly double the
      file (~120MB -> ~200MB) for zero user benefit -- the daily rebuild
      re-anchors them every morning so they never go stale.
    - placeholder: drop the old fixed-date block, write one fresh rolling
      placeholder (anchor -> anchor+7d). The text openly states no schedule
      was supplied; only the dates roll.
    - regular: drop any stale placeholder blocks (prevents accumulation across
      runs), carry all other programmes unchanged (past programmes included --
      keeps validation counts stable), then if nothing remains with
      stop > anchor, append one fresh rolling placeholder so the channel shows
      "Programming" instead of "No information".
    Channels (id/display-name/icon) are preserved byte-faithfully.
    Returns counters dict.
    """
    verified_new = set(verified_fresh)
    service_new = set(service_progs)
    c = {'channels': 0, 'dropped_verified': 0, 'dropped_service': 0,
         'shifted_247': 0, 'carried': 0, 'orphans_dropped': 0,
         'dropped_placeholder': 0, 'placeholder_written': 0,
         'fresh_appended': 0, 'service_appended': 0}
    roster_ids = set(roster)
    reg_max_stop = {}  # cid -> latest programme stop (regular class only)
    with open(out_path, 'w', encoding='utf-8') as fout:
        fout.write('<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')
        for cid in order:
            name, icon = roster[cid]
            fout.write(serialize_channel(cid, name, icon))
            c['channels'] += 1
        for event, elem in iterparse(prev_path, events=('end',)):
            if elem.tag == 'channel':
                elem.clear()  # channels were already written from the roster
                continue
            if elem.tag != 'programme':
                continue  # never clear title/desc/etc: children must stay
                          # intact until their parent <programme> is serialized
            cid = elem.get('channel')
            if cid not in roster_ids:
                c['orphans_dropped'] += 1
                elem.clear()
                continue
            cls = classes[cid]
            if cid in verified_new:
                c['dropped_verified'] += 1
            elif cls == 'v247':
                ms = min_start_247.get(cid)
                delta = (anchor - ms) if ms else timedelta(0)
                fout.write(serialize_programme(elem, delta=delta))
                c['shifted_247'] += 1
            elif cls == 'placeholder':
                # Old fixed-date block is expired by design; drop it. A fresh
                # rolling block is written per channel after the main loop.
                c['dropped_placeholder'] += 1
            elif cid in service_new:
                c['dropped_service'] += 1
            elif is_placeholder_prog(elem):
                # Stale placeholder appended by an earlier run: drop so they
                # never accumulate; a fresh one is appended below if needed.
                c['dropped_placeholder'] += 1
            else:
                fout.write(serialize_programme(elem))
                c['carried'] += 1
                stop = parse_ts(elem.get('stop') or '')
                if stop and (cid not in reg_max_stop or stop > reg_max_stop[cid]):
                    reg_max_stop[cid] = stop
            elem.clear()
        for cid in order:
            if classes.get(cid) == 'placeholder':
                fout.write(rolling_placeholder(cid, anchor))
                c['placeholder_written'] += 1
            elif classes.get(cid) == 'regular' \
                    and reg_max_stop.get(cid, datetime.min.replace(tzinfo=timezone.utc)) <= anchor:
                fout.write(rolling_placeholder(cid, anchor))
                c['placeholder_written'] += 1
        for tid in sorted(verified_fresh):
            for p in verified_fresh[tid]:
                fout.write(p)
                c['fresh_appended'] += 1
        for cid in sorted(service_new):
            if cid in roster_ids and classes.get(cid) == 'regular' \
                    and cid not in verified_new:
                for p in service_progs[cid]:
                    fout.write(p)
                    c['service_appended'] += 1
        fout.write('</tv>\n')
    log("build: " + ", ".join(f"{k}={v}" for k, v in c.items()))
    return c


def ensure_icons(out_path, roster):
    """Idempotent icon ensure from logos247_results.json.

    The roster already carries all curated icons; this only fills gaps and
    never overwrites an existing icon (mirrors the original application:
    1,788 icons, 0 overwrites, 0 missing ids).
    Implemented as a targeted channel-element patch on the built file.
    """
    results = json.load(open(LOGOS247))
    items = results if isinstance(results, list) else results.get('results', [])
    want = {}
    for r in items:
        cid = r.get('channel_id')
        if cid and cid in roster and not roster[cid][1]:
            want[cid] = r.get('poster_url')
    if not want:
        log("ensure_icons: no gaps to fill (all result channels already have icons)")
        return 0
    tmp = out_path + '.iconfix'
    filled = 0
    with open(out_path, encoding='utf-8') as fin, open(tmp, 'w', encoding='utf-8') as fout:
        pending = None
        for line in fin:
            m = re.match(r'\s*<channel id="([^"]*)">\s*$', line)
            if m and m.group(1) in want:
                pending = m.group(1)
                fout.write(line)
                continue
            if pending and re.match(r'\s*</channel>\s*$', line):
                fout.write(f'    <icon src="{esc_q(want[pending])}" />\n')
                filled += 1
                pending = None
            fout.write(line)
    os.replace(tmp, out_path)
    log(f"ensure_icons: filled {filled} gaps, 0 overwrites")
    return filled


# ---------------------------------------------------------------- gates
def validate(out_path):
    """Hard validation gates. Returns (ok, report_dict)."""
    rep = {'channels': 0, 'programmes': 0, 'icons': 0, 'dup_channel_ids': 0,
           'orphans': 0, 'missing_title_time': 0, 'invalid_duration': 0,
           'unparseable_time': 0, 'stale_channels': 0}
    seen = set()      # channel ids
    prog_cids = set()  # channel ids referenced by programmes
    chan_max_stop = {}  # channel id -> latest programme stop (freshness gate)
    dups = set()
    try:
        for event, elem in iterparse(out_path, events=('end',)):
            if elem.tag == 'channel':
                rep['channels'] += 1
                cid = elem.get('id')
                if cid in seen:
                    dups.add(cid)
                seen.add(cid)
                ie = elem.find('icon')
                if ie is not None and ie.get('src'):
                    rep['icons'] += 1
                elem.clear()
            elif elem.tag == 'programme':
                rep['programmes'] += 1
                cid = elem.get('channel')
                prog_cids.add(cid)
                s, e = elem.get('start'), elem.get('stop')
                t = (elem.findtext('title') or '').strip()
                if not (s and e and t):
                    rep['missing_title_time'] += 1
                else:
                    ds, de = parse_ts(s), parse_ts(e)
                    if not (ds and de):
                        rep['unparseable_time'] += 1
                    elif de <= ds:
                        rep['invalid_duration'] += 1
                    else:
                        prev = chan_max_stop.get(cid)
                        if prev is None or de > prev:
                            chan_max_stop[cid] = de
                elem.clear()
    except Exception as ex:
        return False, {'fatal': f'XML not well-formed: {ex}'}
    rep['dup_channel_ids'] = len(dups)
    rep['orphans'] = len(prog_cids - seen)

    # Freshness gate: every channel must have some programme extending past
    # now+12h. Rolling placeholders + daily rebuilds make this hold; a breach
    # means the feeds went stale or the rolling logic regressed -- never ship.
    horizon = datetime.now(timezone.utc) + timedelta(hours=12)
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    rep['stale_channels'] = sum(
        1 for cid in seen if chan_max_stop.get(cid, epoch) <= horizon)

    # logos247 coverage: every result id must exist in the output
    results = json.load(open(LOGOS247))
    items = results if isinstance(results, list) else results.get('results', [])
    missing_ids = [r['channel_id'] for r in items
                   if r.get('channel_id') and r['channel_id'] not in seen]
    rep['logos247_missing_ids'] = len(missing_ids)

    failures = []
    if not (CH_MIN <= rep['channels'] <= CH_MAX):
        failures.append(f"channels {rep['channels']} outside [{CH_MIN},{CH_MAX}]")
    if not (PR_MIN <= rep['programmes'] <= PR_MAX):
        failures.append(f"programmes {rep['programmes']} outside [{PR_MIN},{PR_MAX}]")
    if rep['dup_channel_ids']:
        failures.append(f"{rep['dup_channel_ids']} duplicate channel ids")
    if rep['orphans']:
        failures.append(f"{rep['orphans']} orphan programmes")
    if rep['missing_title_time']:
        failures.append(f"{rep['missing_title_time']} missing title/time")
    if rep['invalid_duration'] or rep['unparseable_time']:
        failures.append(f"{rep['invalid_duration']} invalid durations, "
                        f"{rep['unparseable_time']} unparseable times")
    if rep['icons'] < ICON_MIN:
        failures.append(f"icon coverage {rep['icons']} below minimum {ICON_MIN}")
    if rep['stale_channels'] > 0.10 * rep['channels']:
        failures.append(f"freshness: {rep['stale_channels']} channels with no "
                        f"coverage past now+12h (>10%)")
    if rep['logos247_missing_ids']:
        failures.append(f"{rep['logos247_missing_ids']} logos247 ids missing from output")
    rep['failures'] = failures
    return (not failures), rep


def freshness_stats(out_path):
    """Report-only: how much of the file is actually fresh."""
    now = datetime.now(timezone.utc)
    soon = now - timedelta(hours=12)
    total = future = 0
    per_class = {}
    for event, elem in iterparse(out_path, events=('end',)):
        if elem.tag == 'programme':
            total += 1
            dt = parse_ts(elem.get('start') or '')
            if dt and dt >= soon:
                future += 1
            elem.clear()
    return {'programmes_total': total,
            'programmes_starting_within_window': future,
            'fresh_pct': round(100.0 * future / max(total, 1), 2),
            'as_of_utc': now.strftime('%Y-%m-%d %H:%M:%S')}


# ---------------------------------------------------------------- report / promote
def write_report(report, workdir):
    os.makedirs(HIDDEN_RUNS, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    path = os.path.join(HIDDEN_RUNS, f'run-{ts}.json')
    json.dump(report, open(path, 'w'), indent=1)
    log(f"run report: {path}")
    return path


def icon_losers(prev_path, out_path):
    """Channels that had an icon in the previous build but none in the new."""
    def icons_of(path):
        d = {}
        for event, elem in iterparse(path, events=('end',)):
            if elem.tag == 'channel':
                ie = elem.find('icon')
                d[elem.get('id')] = bool(ie is not None and ie.get('src'))
                elem.clear()
            elif elem.tag == 'programme':
                elem.clear()
        return d
    prev, new = icons_of(prev_path), icons_of(out_path)
    return sorted(cid for cid, had in prev.items() if had and not new.get(cid, False))


def promote(out_path):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    backup = os.path.join(BACKUP_DIR, f'epg.xml.bak-{ts}')
    shutil.copy2(PREV_BUILD, backup)
    log(f"backed up previous build -> {backup}")
    os.replace(out_path, PREV_BUILD)
    log(f"promoted new build -> {PREV_BUILD}")


# ---------------------------------------------------------------- main
def main(argv):
    no_promote = '--no-promote' in argv
    service_xml = None
    workdir = None
    out_copy = None
    for i, a in enumerate(argv):
        if a == '--service-xml' and i + 1 < len(argv):
            service_xml = argv[i + 1]
        if a == '--workdir' and i + 1 < len(argv):
            workdir = argv[i + 1]
        if a == '--out' and i + 1 < len(argv):
            out_copy = argv[i + 1]
    if not service_xml:
        service_xml = os.environ.get('EPG_SERVICE_XML')
    ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    workdir = workdir or os.path.join(BUILD_DIR, 'work', ts)
    os.makedirs(workdir, exist_ok=True)

    report = {'run_ts_utc': ts, 'workdir': workdir, 'no_promote': no_promote,
              'stages': {}}
    try:
        if not os.path.isfile(PREV_BUILD):
            raise RuntimeError(f"previous build not found: {PREV_BUILD}")

        # 1. fresh feeds
        feed_paths = fetch_feeds(workdir)

        # 2. roster + classification from previous final build
        roster, order, classes, min_start_247 = load_roster(PREV_BUILD)

        # 3. verified set + fresh feed programmes
        matches = load_verified_matches()
        missing_targets = [t for t in matches if t not in roster]
        if missing_targets:
            log(f"WARNING: {len(missing_targets)} verified targets not in roster "
                f"(kept out of refresh): {missing_targets[:5]}")
            matches = {t: v for t, v in matches.items() if t in roster}
        cutoff14 = (datetime.now(timezone.utc) - timedelta(hours=6)
                    ).strftime('%Y%m%d%H%M%S')
        verified_fresh, skipped = extract_feed_programmes(matches, feed_paths, cutoff14)
        report['stages']['feeds'] = {
            'verified_targets': len(matches),
            'refreshed': len(verified_fresh),
            'fresh_programmes': sum(len(v) for v in verified_fresh.values()),
            'skipped_lt5': [(t, n) for t, n in skipped]}

        # 4. optional provider XML hook for regular channels
        service_progs = load_service_xml(service_xml) if service_xml else {}
        if service_xml and not service_progs:
            log("WARNING: --service-xml given but yielded no programmes; "
                "regular channels will be carried over")

        # 5. build
        anchor = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        out_path = os.path.join(workdir, 'epg_new.xml')
        counters = build_output(PREV_BUILD, out_path, roster, order, classes,
                                min_start_247, verified_fresh, service_progs, anchor)
        report['stages']['build'] = counters

        # 6. idempotent icon ensure
        filled = ensure_icons(out_path, roster)
        report['stages']['icons_filled'] = filled

        # 7. hard gates
        ok, vrep = validate(out_path)
        report['stages']['validation'] = vrep
        report['stages']['freshness'] = freshness_stats(out_path)
        for f in vrep.get('failures', []):
            log(f"GATE FAILURE: {f}")
        if not ok:
            report['result'] = 'ABORTED: validation gates failed; no release cut'
            write_report(report, workdir)
            log("ABORTED: gates failed. Previous build untouched, no release.")
            return 1

        # 8. icon-loss diff vs previous run
        losers = icon_losers(PREV_BUILD, out_path)
        report['icon_losers'] = losers
        log(f"channels that lost icons vs previous run: {len(losers)}")
        for cid in losers[:10]:
            log(f"   lost icon: {cid}")

        # 9. promote
        report['result'] = 'OK'
        write_report(report, workdir)
        if out_copy:
            shutil.copy2(out_path, out_copy)
            log(f"copied final build -> {out_copy}")
        if no_promote:
            log(f"OK: all gates passed. --no-promote: new build left at {out_path}")
        else:
            promote(out_path)
        log(f"done: {vrep['channels']} channels, {vrep['programmes']} programmes, "
            f"{vrep['icons']} icons")
        return 0
    except Exception as e:
        report['result'] = f'ABORTED: {type(e).__name__}: {e}'
        try:
            write_report(report, workdir)
        except Exception:
            pass
        log(f"ABORTED: {e}. Previous build untouched, no release.")
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
