#!/usr/bin/env python3
"""Weekly EPG rebuild orchestrator.

Reproduces the curated final build (10,940 channels / ~485k programmes /
10,550 icons) from fresh weekly data, then validates with hard gates.
Only a fully-validated file is promoted to ~/workspace/your_files/epg.xml.

PIPELINE (reconstructed from the build scripts in this directory):
  1. epg_logos4.xml  -- base: 386 missing tvg-ids integrated + logo waves 1-4
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
  - EPGShare01 international feeds (https://epgshare01.online/epgshare01/
    epg_ripper_{CODE}.xml.gz): ~90 country/themed XMLTV feeds, no signup,
    fresh ~4-5 day windows. 772 verified channel matches
    (pubfeed/epgshare_matches.json: 599 exact-ID + 173 exact-after-strip,
    same-country only). Best-effort fetch -- a failed feed only skips its
    channels, never aborts the run.
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

NEVER logs secrets: the only network fetches here are the public epg.pw
and EPGShare01 feeds.
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

# Strip characters illegal in XML 1.0 (control chars except tab/newline/CR).
# Provider feeds occasionally contain these in titles/descs; they would make
# the rebuilt output not well-formed and trip the fatal XML validation gate.
_ILLEGAL_XML_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')


def _sanitize_xml_text(s):
    """Remove illegal XML 1.0 chars so reconstructed programme XML stays well-formed."""
    return _ILLEGAL_XML_RE.sub('', s) if s else s

# ---------------------------------------------------------------- config
BUILD_DIR = os.path.dirname(os.path.abspath(__file__))
# CI override: EPG_PREV_BUILD env var (set by GitHub Actions workflow)
PREV_BUILD = os.environ.get('EPG_PREV_BUILD') or os.path.expanduser('~/workspace/your_files/epg.xml')
PUBFEED_DIR = os.path.join(BUILD_DIR, 'pubfeed')
# CI override: EPG_RUNS_DIR env var (set by GitHub Actions workflow)
HIDDEN_RUNS = os.environ.get('EPG_RUNS_DIR') or os.path.join(BUILD_DIR, 'hidden_runs')
BACKUP_DIR = os.path.join(HIDDEN_RUNS, 'backups')
LOGOS247 = os.path.join(BUILD_DIR, 'logos247_results.json')

FEED_CCS = ['DE', 'BR', 'AU', 'CA', 'GB', 'FR', 'IN', 'US', 'ID']
FEED_URL = 'https://epg.pw/xmltv/epg_{cc}.xml.gz'

# EPGShare01 international feeds (https://epgshare01.online): ~90 country/
# themed XMLTV feeds, no signup. 772 verified channel matches
# (599 exact-ID + 173 exact-after-strip, same-country) in
# pubfeed/epgshare_matches.json. Feed codes are derived from that file;
# downloads are best-effort (a blocked/failed feed only skips its channels,
# never aborts the run).
EPGSHARE_URL = 'https://epgshare01.online/epgshare01/epg_ripper_{code}.xml.gz'
EPGSHARE_MATCHES = os.path.join(PUBFEED_DIR, 'epgshare_matches.json')

# vcicio/US-EPG merged US guide (https://github.com/vcicio/us-epg): single
# 9.6-day-window feed, 5,489 channels, rebuilt every 6h. Candidate USA matches
# in pubfeed/vcicio_usa_matches.json ({chris_id: {feed_channel_id, feed_name,
# future_progs}}) are PROVISIONAL until audited — do not call them verified.
# LEGAL: vcicio repackages EPGShare01 feeds and its repo has no license file;
# Chris approved EPGShare01's own redistribution gray area, but the vcicio
# repackaging question is still open — no public release of vcicio-derived
# data until he answers it. On overlap, the feed with the longest valid
# future span of real programmes wins (never placeholders over real data).
VCICIO_URL = 'https://vcicio.github.io/US-EPG/merged_epg.xml.gz'
VCICIO_MATCHES = os.path.join(PUBFEED_DIR, 'vcicio_usa_matches.json')

# --- Sunday 2026-09-27: additional approved guide sources ---
# Precedence (after epg.pw verified set and EPGShare01/vcicio): iptv-epg.org
# -> Sky DE -> Sky UK -> iptvtalk -> TVGuide. Each is best-effort (a failed
# download only skips its channels, never aborts). NEVER open-epg.com
# (evaluated, not approved).
IPTVEPG_URL = 'https://iptv-epg.org/files/epg-us.xml.gz'
IPTVEPG_MATCHES = os.path.join(PUBFEED_DIR, 'iptv_epg_org_matches.json')
SKYDE_URL = 'https://muq-org.github.io/tv-epg/epg_sky.xml'
SKYDE_MATCHES = os.path.join(PUBFEED_DIR, 'sky_de_matches.json')
SKYUK_URL = ('https://raw.githubusercontent.com/Permanently/sky-epg-xmltv/'
             'main/guides/london_hd.xml')
SKYUK_MATCHES = os.path.join(PUBFEED_DIR, 'sky_uk_matches.json')
IPTVTALK_URLS = {
    'US': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/US_guide.xml.gz',
    'UK': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/UK_guide.xml.gz',
    'Latino': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/Latino_guide.xml.gz',
    'US_local': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/US_local_guide.xml.gz',
}
IPTVTALK_MATCHES = os.path.join(PUBFEED_DIR, 'iptvtalk_matches.json')

# EPGTalk 7-day guides (https://github.com/acidjesuz/EPGTalk): US/UK/Latino
# XMLTV, ~7.4-day windows, Schedules Direct/Gracenote channel IDs matched by
# normalized display name in pubfeed/epgtalk_matches.json
# ({our_id: {feed_channel_id, feed_code, tier}}; tier 1 = exact name match,
# tier 2 = market/callsign-stripped match). Numbered event channels
# (ESPN+ 016, Canal 19, ...) are never matched. Stage 3b2 runs right after
# EPGShare01: it never displaces epg.pw/EPGShare01 data (fills only channels
# with no real listings yet), and because it runs before the legacy iptvtalk
# stage (3h, same upstream guides), EPGTalk wins on any overlap there too.
EPGTALK_URLS = {
    'US': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/US_guide.xml.gz',
    'UK': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/UK_guide.xml.gz',
    'Latino': 'https://raw.githubusercontent.com/acidjesuz/EPGTalk/master/Latino_guide.xml.gz',
}
EPGTALK_MATCHES = os.path.join(PUBFEED_DIR, 'epgtalk_matches.json')
TVGUIDE_MATCHES = os.path.join(PUBFEED_DIR, 'tvguide_matches.json')
TVGUIDE_API = ('https://backend.tvguide.com/tvschedules/tvguide/{pid}/web'
               '?start={start}&duration={dur}&channelSourceIds={sid}'
               '&apiKey={key}')
TVGUIDE_API_KEY = os.environ.get('TVGUIDE_API_KEY', '')

# Event-label injection files (PPV / ESPN+ / FloSports / Fanatiz).
# Each entry: {epg_channel_id, event_title, start_utc, duration_min_estimated,
# status}. Future events become programme blocks; never overwrite real data.
EVENT_FILES = {
    'ppv': os.path.join(PUBFEED_DIR, 'ppv_events.json'),
    'espnplus': os.path.join(PUBFEED_DIR, 'espnplus_events.json'),
    'flosports': os.path.join(PUBFEED_DIR, 'flosports_events.json'),
    'fanatiz': os.path.join(PUBFEED_DIR, 'fanatiz_events.json'),
}

# College-network epg.pw backups (ACC/Big Ten/SEC via the already-downloaded
# US feed; ESPNU via provider/TVGuide). Wired as verified matches.
COLLEGE_BACKUPS = {
    'accnetwork.us': ('464879', 'US'),
    'epg-usa-big-ten-network-uhd-025dc210': ('465073', 'US'),
    'secnetwork.us': ('465266', 'US'),
}

# Roster corrections (2026-09-27).
ROSTER_RENAMES = {  # KMTV is CBS, not ABC (TVGuide confirms KMTV-DT CBS)
    'abcketv.us': 'USA CBS 3 Omaha (KMTV)',
    'epg-usa-abc-13-omaha-kmtv-1095e622': 'USA CBS 3 Omaha (KMTV)',
    # 360north.us is the provider's tvg-id for USA The Movie Channel Xtra;
    # the roster had fossilized it as "USA Fox KRQE Albuquerque" (wrong
    # station entirely -- KRQE is covered by m3u-nm-alburquerque-fox-krqe).
    '360north.us': 'USA The Movie Channel Xtra',
    # retroplex.us was fossilized as "USA Latin RetroPlex TV" (the provider
    # reuses this one tvg-id for East, West, AND Latin); the sourced feed is
    # the East schedule, so the name says East honestly.
    'retroplex.us': 'USA RetroPlex East',
}
ROSTER_DROPS = {'epg-24-7-hunted-e77b2975'}  # obsolete; superseded by m3u-247-hunted

# Roster additions: provider playlist channels whose numeric stream ids never
# entered the roster (load_roster only carries ids from the previous build, so
# without this they can never appear). Keys are the provider programme keys
# (stream-<id> for streams with no epg_channel_id). Added 2026-09-27:
# Alaska/Hawaii locals from Chris's playlist.
ROSTER_ADDITIONS = {  # target_id: display_name (icon filled by icon stages)
    'stream-648019': 'AK Fairbanks NBC KTVF',
    'stream-648020': 'AK Juneau-Douglas NBC KATH',
    'stream-648341': 'USA ABC13 KYUR Anchorage',
    'stream-648677': 'USA NBC2 KTUU Anchorage',
    'stream-517241': 'USA FOX 4 KTBY Anchorage',
    'stream-517488': 'USA ABC 13 KYUR Anchorage',
    'stream-517790': 'USA NBC 2 KTUU Anchorage',
    'stream-517901': 'USA NBC 11 KTVF Fairbanks',
    'stream-648084': 'HI Honolulu CBS KGMB',
    'stream-648085': 'HI Honolulu FOX KHON',
    'stream-648086': 'HI Honolulu NBC KHNL',
    'stream-517437': 'USA ABC 4 KITV Honolulu',
    'stream-648359': 'USA ABC4 KITV Honolulu',
    # Final-9 cable networks (2026-09-29): USA channels missing from roster,
    # sources verified live on TV Insider. Chris confirmed provider carries them.
    'adultswim.us': 'USA Adult Swim',
    'bloomberg.us': 'USA Bloomberg TV',
    'fs1.us': 'USA FS1',
    'mgmplus.us': 'USA MGM+',
}

# Display-name disambiguation for channels that share a name with another
# channel in Chris's playlist (2026-09-27). Applied after the roster is built.
DISPLAY_NAME_OVERRIDES = {
    'ctv2ottawa.ca': 'CA CTV 2 (Ottawa)',  # was "CA CTV 2 (London)", dup of ctv2london.ca
    'msgsnplus2.us': 'USA MSG Sportsnet Plus 2 HD',  # was "USA MSG 2 PLUS HD", dup of msg2.us
}

# Programme clones: target_id -> source_id. The target is a playlist alias of
# the same station and inherits the source's programmes verbatim. Sources are
# previous-build channels whose timestamps are already in local air time
# (Alaska -4h / Hawaii -6h applied at ingest), so clones are NEVER re-shifted.
# Targets without a source (648020 KATH Juneau, 648677/517790 KTUU Anchorage:
# no EPG source exists anywhere in the pipeline) keep honest placeholders.
CLONE_SOURCES = {
    'stream-648019': 'nbc11ktvf.us',
    'stream-648341': 'abc13kyur.us',
    'stream-517241': 'fox4ktby.us',
    'stream-517488': 'abc13kyur.us',
    'stream-517901': 'nbc11ktvf.us',
    'stream-648084': 'cbs5kgmb.us',
    'stream-648085': 'foxkhon.us',
    'stream-648086': 'nbckhnl.us',
    'stream-517437': 'abckitv.us',
    'stream-648359': 'abckitv.us',
}

# Fossilized per-event display names: numbered event channels (BIG10+, ESPN+,
# PPV) whose names baked in a past event (e.g. "Fri @ Sep 18") and now lie in
# the guide. Strip to the honest generic number until a fresh event-label
# stage renames them with current events. (regex, replacement)
ROSTER_RENAME_PATTERNS = [
    (re.compile(r'^(BIG10\+\s*\d+):.*', re.I), r'\1'),
    (re.compile(r'^(USA\s+ESPN\+\s*\d+):.*', re.I), r'\1'),
    (re.compile(r'^(USA\s+PPV\d+):.*', re.I), r'\1'),
]

# Chris's hand-made icons WIN; hunt posters fill gaps only.
CHRIS_ICONS = os.path.join(BUILD_DIR, 'work', 'chris_logos', 'icon_urls.json')
HUNT_ICONS = os.path.join(BUILD_DIR, 'work', 'chris_logos', 'poster_hunt_results.json')
# CI repo-local fallbacks (copied into epg_actions/epg/ by the 24/7 prep).
CHRIS_ICONS_REPO = os.path.join(BUILD_DIR, 'chris_icon_urls.json')
HUNT_ICONS_REPO = os.path.join(BUILD_DIR, 'poster_hunt_urls.json')

# Rehosted logos (2026-10-01): ~4.3k http:// provider/IP-hosted channel logos
# rehosted under epg/logos/rehosted/ because Android/TiviMate blocks
# cleartext http. Maps old URL -> new raw.githubusercontent.com URL.
REHOST_MAP_FILE = os.path.join(BUILD_DIR, 'rehosted_logo_map.json')

# Provider service-XML timezone shifts (provider timestamps are Eastern-
# semantics; these stations air on local time). Applied to service-XML
# programmes only, never to public-feed data.
SERVICE_TZ_SHIFTS = {  # channel_id -> hours to shift
    'foxkhon.us': -6, 'abckitv.us': -6, 'cbs5kgmb.us': -6, 'nbckhnl.us': -6,
    'fox4ktby.us': -4, 'abc13kyur.us': -4, 'nbc11ktvf.us': -4,
}

# NFL Sunday Ticket guide (manual schedule injection).
# nfl_sunday_ticket.json holds {channel_id: [programme XML strings]} built
# from ESPN's published schedule via the live browser. Games are one-off
# events, so the file carries valid_until_utc and the stage skips itself
# once the games have ended. Channel->game order is best-effort: the
# provider assigns games to its 705-717 numbers opaquely, and channels
# 705-707 bake the matchup into their IDs (they rename weekly).
NFL_SCHEDULE = os.path.join(BUILD_DIR, 'nfl_sunday_ticket.json')

# NHL schedule injection (same pattern as NFL). nhl_schedule.json holds
# {channel_id: [programme XML strings]} for USA NHL 01-06, built from ESPN's
# published schedule via update_nhl_schedule.py. Games are one-off events,
# so the file carries valid_until_utc and the stage skips itself once the
# games have ended. Only USA NHL 01-06 -- the 18-42 channels are mislabeled
# NCAA and are excluded.
NHL_SCHEDULE = os.path.join(BUILD_DIR, 'nhl_schedule.json')

# WNBA playoff injection (same pattern as NFL/NHL). wnba_playoffs.json holds
# {channel_id: [programme XML strings]} for USA WNBA 01-07, built from ESPN's
# published schedule via update_wnba_schedule.py. Games are one-off events,
# so the file carries valid_until_utc and the stage skips itself once the
# games have ended.
WNBA_SCHEDULE = os.path.join(BUILD_DIR, 'wnba_playoffs.json')

# BIG10+ injection (same pattern as NFL/NHL/WNBA). big10plus_schedule.json
# holds {logical_channel_id: [programme XML strings]} for BIG10+ 01-24, built
# from ESPN's published schedule via update_big10plus_schedule.py. Keys are
# LOGICAL ids (m3u-big10-01 .. m3u-big10-24); the roster carries fossilized
# per-event IDs (m3u-big10-04-volleyball-w-...-fri-sep-18-...), so stage 3d4
# resolves each number to the actual roster ID via regex at build time.
BIG10PLUS_SCHEDULE = os.path.join(BUILD_DIR, 'big10plus_schedule.json')

# FloRacing injection (same pattern as NFL/NHL/WNBA/BIG10+).
# floracing_schedule.json holds {channel_id: [programme XML strings]} for
# the single USA Flo Racing channel (m3u-usa-flo-racing), built from
# FloRacing's public schedule API via update_floracing_schedule.py. The
# channel ID is stable (no fossilized per-event IDs), so stage 3d5 maps
# directly -- no number resolution needed.
FLORACING_SCHEDULE = os.path.join(BUILD_DIR, 'floracing_schedule.json')

# NBA schedule injection (same pattern as NFL/NHL). nba_schedule.json holds
# {channel_id: [programme XML strings]} for USA NBA 01-06 (NBA League Pass,
# m3u-usa-nba-0X IDs), built from ESPN's published schedule via
# update_nba_schedule.py. Games are one-off events, so the file carries
# valid_until_utc and the stage skips itself once the games have ended.
NBA_SCHEDULE = os.path.join(BUILD_DIR, 'nba_schedule.json')

# TV Passport local listings via update_tvpassport_schedule.py (stage 3d7).
# Keyed by chris_id; values are pre-built XML programme strings.
TVP_SCHEDULE = os.path.join(BUILD_DIR, 'tvpassport_schedule.json')

# Cable-network listings via update_cable_schedule.py (stage 3d7).
# {"sources": {chris_id: {"source":..., "source_url":...,
#   "programmes": [{"start": ISO, "stop": ISO, "title":...}]}}}
CABLE_SCHEDULE = os.path.join(BUILD_DIR, 'cable_schedule.json')

# Known-good build this pipeline reproduces.
# 2026-09-21: baseline recalibrated after the fossil drop (programmes ending
# >6h before the anchor are no longer carried). ~300k = real future coverage
# + rolling placeholders; the old 561k figure counted ~263k dead past
# programmes that TiviMate never renders.
KNOWN_CHANNELS = 10940
KNOWN_PROGRAMMES = 745000  # empirical: 2026-09-27 CI with service XML has ~620k (was 560k without)
KNOWN_ICONS = 10550

# Hard gates.
CH_MIN = int(KNOWN_CHANNELS * 0.98)
CH_MAX = int(KNOWN_CHANNELS * 1.02)
PR_MIN = int(KNOWN_PROGRAMMES * 0.85)
PR_MAX = int(KNOWN_PROGRAMMES * 1.15)
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


def rolling_placeholder(cid, start, title=None):
    """One honest placeholder block spanning start -> start+7d for channel cid.
    Title defaults to the channel's display name (not generic 'Programming')."""
    t = title or PLACEHOLDER_TITLE
    return serialize_fresh_prog(fmt_ts(start),
                                fmt_ts(start + timedelta(days=PLACEHOLDER_DAYS)),
                                cid, t, PLACEHOLDER_DESC)

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
    # Apply display-name disambiguation overrides (e.g. duplicate channel names
    # in Chris's playlist).
    name = DISPLAY_NAME_OVERRIDES.get(cid, name)
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


def fetch_epgshare_feeds(workdir, codes):
    """Download EPGShare01 per-country feeds. Best-effort: an individual
    feed failure is logged and skipped (its channels simply keep their
    carried-forward programmes) -- unlike epg.pw, this source never aborts
    the run, because the site sometimes blocks datacenter IPs/VPNs."""
    feed_dir = os.path.join(workdir, 'epgshare')
    os.makedirs(feed_dir, exist_ok=True)
    paths = {}
    for code in sorted(codes):
        url = EPGSHARE_URL.format(code=code)
        dest = os.path.join(feed_dir, f'epgshare_{code}.xml.gz')
        log(f"fetching epgshare {code} ...")
        if not download(url, dest):
            log(f"  WARNING: epgshare {code} download failed; skipping")
            continue
        try:
            with gzip.open(dest, 'rb') as f:
                head = f.read(200)
            assert b'<tv' in head
        except Exception as e:
            log(f"  WARNING: epgshare {code} not valid gzip/XMLTV ({e}); skipping")
            continue
        paths[code] = dest
        log(f"  {code}: {os.path.getsize(dest)} bytes")
    if not paths:
        log("WARNING: no EPGShare01 feeds downloaded; those channels keep "
            "carried-forward programmes")
    return paths


def fetch_epgtalk_feeds(workdir, codes):
    """Download EPGTalk 7-day guides (US/UK/Latino). Best-effort: an
    individual feed failure is logged and skipped (its channels simply keep
    their carried-forward programmes) -- this source never aborts the run."""
    paths = {}
    for code in sorted(codes):
        url = EPGTALK_URLS.get(code)
        if not url:
            log(f"  WARNING: no EPGTalk URL for code {code}; skipping")
            continue
        dest = os.path.join(workdir, f'epgtalk_{code}.xml.gz')
        p = fetch_xml_feed(url, dest, f'EPGTalk {code}')
        if p:
            paths[code] = p
    if not paths:
        log("WARNING: no EPGTalk feeds downloaded; those channels keep "
            "carried-forward programmes")
    return paths


def fetch_vcicio_feed(workdir):
    """Download vcicio/US-EPG merged US guide. Best-effort like EPGShare:
    a failed download only skips its channels, never aborts the run."""
    feed_dir = os.path.join(workdir, 'vcicio')
    os.makedirs(feed_dir, exist_ok=True)
    dest = os.path.join(feed_dir, 'merged_epg.xml.gz')
    log("fetching vcicio US-EPG ...")
    if not download(VCICIO_URL, dest):
        log("  WARNING: vcicio download failed; skipping")
        return None
    try:
        with gzip.open(dest, 'rb') as f:
            head = f.read(200)
        assert b'<tv' in head
    except Exception as e:
        log(f"  WARNING: vcicio not valid gzip/XMLTV ({e}); skipping")
        return None
    log(f"  vcicio: {os.path.getsize(dest)} bytes")
    return dest


def fetch_xml_feed(url, dest, label):
    """Download one XMLTV feed (gzipped or plain). Best-effort: a failure
    only skips its channels, never aborts the run."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    log(f"fetching {label} ...")
    if not download(url, dest):
        log(f"  WARNING: {label} download failed; skipping")
        return None
    try:
        with open_feed(dest) as f:
            head = f.read(200)
        assert b'<tv' in head
    except Exception as e:
        log(f"  WARNING: {label} not valid XMLTV ({e}); skipping")
        return None
    log(f"  {label}: {os.path.getsize(dest)} bytes")
    return dest


def open_feed(path):
    """Open an XMLTV feed that may be gzipped or plain XML."""
    with open(path, 'rb') as f:
        magic = f.read(2)
    if magic == b'\x1f\x8b':
        return gzip.open(path, 'rb')
    return open(path, 'rb')


def fetch_tvguide(matches, workdir):
    """Pull 7-day schedules from the TVGuide API for staged matches.

    Best-effort; requires TVGUIDE_API_KEY in the environment. Returns
    {target_id: [programme xml]}. Without a key, skips quietly (channels
    keep carried data).
    """
    if not TVGUIDE_API_KEY:
        log("tvguide: no TVGUIDE_API_KEY; skipping")
        return {}, []
    if not matches:
        return {}, []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    fresh, skipped = {}, []
    for tid, m in matches.items():
        pid = m.get('tvguide_lineup_id')
        sid = m.get('feed_channel_id')
        if not (pid and sid):
            skipped.append((tid, 'no lineup/source id'))
            continue
        url = TVGUIDE_API.format(pid=pid, start=now_ts, dur=10080,
                                 sid=sid, key=TVGUIDE_API_KEY)
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/126.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.tvguide.com/listings/'})
            with urllib.request.urlopen(req, timeout=45) as r:
                d = json.loads(r.read().decode('utf-8', 'replace'))
        except Exception as e:
            skipped.append((tid, f'api error {type(e).__name__}'))
            continue
        progs = []
        for it in d.get('data', {}).get('items', []):
            ch = it.get('channel', {}) or {}
            if str(ch.get('sourceId')) != str(sid):
                continue
            for p in it.get('programSchedules', []) or []:
                st, et, t = p.get('startTime'), p.get('endTime'), p.get('title') or ''
                if st and et and t and et > now_ts:
                    s = datetime.fromtimestamp(st, tz=timezone.utc)
                    e = datetime.fromtimestamp(et, tz=timezone.utc)
                    progs.append(serialize_fresh_prog(fmt_ts(s), fmt_ts(e),
                                                     tid, t.strip(), ''))
        progs.sort()
        if len(progs) >= 5:
            fresh[tid] = progs
        else:
            skipped.append((tid, f'only {len(progs)} future programmes'))
        time.sleep(0.5)
    log(f"tvguide: {len(fresh)} channels refreshed, "
        f"{sum(len(v) for v in fresh.values())} programmes; "
        f"skipped: {len(skipped)}")
    return fresh, skipped


def inject_event_labels(roster, verified_fresh):
    """Stage 3j: turn staged event labels (PPV/ESPN+/FloSports/Fanatiz)
    into programme blocks. Only future events with a real EPG channel id,
    and never overwrites channels that already have real listings."""
    now = datetime.now(timezone.utc)
    injected = {}
    for label, path in EVENT_FILES.items():
        if not os.path.isfile(path):
            continue
        try:
            events = json.load(open(path))
        except Exception:
            continue
        if isinstance(events, dict):  # ppv file is a list; be tolerant
            events = events.get('events', [])
        n = 0
        for ev in events:
            tid = ev.get('epg_channel_id')
            if not tid or tid not in roster or tid in verified_fresh:
                continue
            if ev.get('status') != 'future':
                continue
            title = (ev.get('event_title') or '').strip()
            start = parse_ts(ev.get('start_utc') or '')
            if not (title and start and start > now):
                continue
            dur = int(ev.get('duration_min_estimated') or 180)
            stop = start + timedelta(minutes=dur)
            prog = serialize_fresh_prog(fmt_ts(start), fmt_ts(stop), tid,
                                        title, '')
            injected.setdefault(tid, []).append(prog)
            n += 1
        if n:
            log(f"events: {label}: {n} future event blocks for "
                f"{len([t for t in injected])} channels")
    for tid, plist in injected.items():
        if tid not in verified_fresh:
            verified_fresh[tid] = sorted(plist)
    return {'channels': len(injected),
            'programmes': sum(len(v) for v in injected.values())}


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


# ---------------------------------------------------------------- roster patches
def _icon_file(*paths):
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None


def apply_roster_patches(roster, order, classes):
    """Sunday corrections applied to the in-memory roster before the build.

    - ROSTER_RENAMES: fix wrong display names (KMTV ABC -> CBS).
    - ROSTER_RENAME_PATTERNS: strip fossilized per-event names to honest
      generic (BIG10+/ESPN+/PPV numbered event channels).
    - ROSTER_DROPS: remove obsolete channel ids (deduped Hunted).
    - ROSTER_ADDITIONS: insert playlist channels missing from the roster
      (Alaska/Hawaii numeric stream ids); classed 'regular'.
    - Icons: Chris's hand-made icon_urls.json wins outright; the auto-hunt
      poster_hunt_results.json fills only channels that still lack an icon.
    Returns the pruned order list.
    """
    ren = sum(1 for cid, name in ROSTER_RENAMES.items() if cid in roster)
    for cid, name in ROSTER_RENAMES.items():
        if cid in roster:
            roster[cid] = (name, roster[cid][1])
    pat_ren = 0
    for rx, repl in ROSTER_RENAME_PATTERNS:
        for cid in list(roster):
            name, icon = roster[cid]
            new = rx.sub(repl, name or '')
            if new != name:
                roster[cid] = (new, icon)
                pat_ren += 1
    dropped = [cid for cid in ROSTER_DROPS if cid in roster]
    for cid in dropped:
        del roster[cid]
    order = [cid for cid in order if cid not in ROSTER_DROPS]
    added = 0
    for cid, name in ROSTER_ADDITIONS.items():
        if cid not in roster:
            roster[cid] = (name, '')
            order.append(cid)
            classes[cid] = 'regular'
            added += 1

    chris_path = _icon_file(CHRIS_ICONS, CHRIS_ICONS_REPO)
    chris = json.load(open(chris_path)) if chris_path else {}
    overwrote = filled = 0
    for cid, url in chris.items():
        if cid in roster:
            if roster[cid][1] != url:
                overwrote += 1
            roster[cid] = (roster[cid][0], url)
    hunt_path = _icon_file(HUNT_ICONS, HUNT_ICONS_REPO)
    hunt = json.load(open(hunt_path)) if hunt_path else {}
    for cid, url in hunt.items():
        if cid in roster and not roster[cid][1]:
            roster[cid] = (roster[cid][0], url)
            filled += 1
    log(f"roster patches: {ren} renames, {pat_ren} pattern renames, "
        f"{len(dropped)} drops, {added} additions, "
        f"{overwrote} chris icons applied, {filled} hunt gaps filled")
    return order


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
    for tid, (fcid, fcc) in COLLEGE_BACKUPS.items():
        matches[tid] = (fcid, fcc)
    log(f"college backups: {len(COLLEGE_BACKUPS)}")
    log(f"verified set total (unique target_ids): {len(matches)}")
    return matches


_span_re = re.compile(
    r'<programme\s+start="([^"]+)"\s+stop="([^"]+)"[^>]*>'
    r'(?:.*?<title[^>]*>(.*?)</title>)?'
    r'(?:.*?<desc[^>]*>(.*?)</desc>)?',
    re.S)


def future_span(progs, now):
    """(real_future_count, max_real_future_stop) for a list of serialized
    programme XML strings. 'Real' excludes our honest placeholder blocks and
    bare 'Programming' titles, so a placeholder-only feed can never outrank
    real listings by span alone."""
    n = 0
    max_stop = None
    for x in progs:
        m = _span_re.search(x)
        if not m:
            continue
        s, e = parse_ts(m.group(1)), parse_ts(m.group(2))
        if not (s and e and e > now):
            continue
        title = (m.group(3) or '').strip()
        desc = m.group(4) or ''
        if title.lower().rstrip('.') == 'programming':
            continue
        if PLACEHOLDER_MARK in desc:
            continue
        n += 1
        if max_stop is None or e > max_stop:
            max_stop = e
    return n, max_stop


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
        # NOTE: several roster IDs can share one feed channel (Chris keeps
        # duplicate IDs for the same station, e.g. epg-usa-*/m3u-usa-*/*.us).
        # Map feed->ALL its tids; a {fcid: tid} dict silently drops every
        # duplicate but one (that bug cost ~578 vcicio channels their data).
        want = {}
        for tid, fcid in pairs:
            want.setdefault(fcid, []).append(tid)
        buckets = {tid: [] for tid, _ in pairs}
        with open_feed(feed_paths[fcc]) as f:
            for event, elem in iterparse(f, events=('end',)):
                if elem.tag == 'programme':
                    fcid = elem.get('channel')
                    if fcid in want:
                        s, e = elem.get('start'), elem.get('stop')
                        t = (elem.findtext('title') or '').strip()
                        ds = (elem.findtext('desc') or '').strip()
                        if s and e and t and s[:14] < e[:14] and e[:14] > cutoff14:
                            for tid in want[fcid]:
                                buckets[tid].append(
                                    (s, serialize_fresh_prog(s, e, tid, t, ds)))
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
        title = _sanitize_xml_text(tm.group(1).strip() if tm else '')
        if not title:
            continue
        desc = _sanitize_xml_text(dm.group(1).strip() if dm else '')
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
      placeholder. The text openly states no schedule was supplied; only the
      dates roll. If fresh verified data arrives for the channel, the real
      listings replace the placeholder (with a tail placeholder only if the
      fresh schedule ends within 12h of the anchor).
    - regular: drop any stale placeholder blocks (prevents accumulation across
      runs) and drop fossil programmes that ended more than 6h before the
      anchor (TiviMate only shows now->future; carrying them forever inflated
      programme counts while the visible guide stayed empty -- fixed
      2026-09-21), carry all other programmes unchanged, then if the latest
      programme (carried or freshly fetched) ends at or before anchor+12h,
      append one honest placeholder starting at max(anchor, latest stop) so
      the channel shows "Programming" instead of "No information" once its
      schedule runs out.
    Channels (id/display-name/icon) are preserved byte-faithfully.
    Returns counters dict.
    """
    verified_new = set(verified_fresh)
    service_new = set(service_progs)
    c = {'channels': 0, 'dropped_verified': 0, 'dropped_service': 0,
         'shifted_247': 0, 'carried': 0, 'orphans_dropped': 0,
         'dropped_placeholder': 0, 'dropped_fossil': 0,
         'placeholder_written': 0, 'fresh_appended': 0, 'service_appended': 0}
    roster_ids = set(roster)
    reg_max_stop = {}  # cid -> latest programme stop (regular class only)
    reg_min_start = {}  # cid -> earliest programme start (for leading-gap fill)
    fossil_cutoff = anchor - timedelta(hours=6)
    # Programme clones (Alaska/Hawaii playlist aliases): source_id ->
    # [target_ids]. Clone copies are written wherever the source's
    # programmes are written, with the channel id swapped. Sources are
    # already in local air time; clones are NEVER re-shifted.
    clone_targets = {}
    for _tid, _sid in CLONE_SOURCES.items():
        if _tid in roster_ids and _sid in roster_ids:
            clone_targets.setdefault(_sid, []).append(_tid)
    _chan_attr_re = re.compile(r'channel="[^"]*"')
    if clone_targets:
        log(f"build: {sum(len(v) for v in clone_targets.values())} clone "
            f"targets from {len(clone_targets)} sources")
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
                for _t in clone_targets.get(cid, ()):
                    fout.write(serialize_programme(elem, channel=_t,
                                                  delta=delta))
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
                stop = parse_ts(elem.get('stop') or '')
                if stop and stop < fossil_cutoff:
                    # Fossil: ended >6h before the anchor. TiviMate renders
                    # now->future only, so these are invisible there; carrying
                    # them forever is what inflated "real data" counts while
                    # the visible guide showed "Programming".
                    c['dropped_fossil'] += 1
                else:
                    fout.write(serialize_programme(elem))
                    for _t in clone_targets.get(cid, ()):
                        fout.write(serialize_programme(elem, channel=_t))
                    c['carried'] += 1
                    start_ts = parse_ts(elem.get('start') or '')
                    if stop and (cid not in reg_max_stop
                                 or stop > reg_max_stop[cid]):
                        reg_max_stop[cid] = stop
                        for _t in clone_targets.get(cid, ()):
                            if (_t not in reg_max_stop
                                    or stop > reg_max_stop[_t]):
                                reg_max_stop[_t] = stop
                    if start_ts and (cid not in reg_min_start
                                     or start_ts < reg_min_start[cid]):
                        reg_min_start[cid] = start_ts
                        for _t in clone_targets.get(cid, ()):
                            if (_t not in reg_min_start
                                    or start_ts < reg_min_start[_t]):
                                reg_min_start[_t] = start_ts
            elem.clear()
        # Freshness: a channel counts as covered only if some programme
        # extends past anchor+12h (this mirrors the validation gate). The
        # latest stop must include the fresh verified/service programmes
        # appended below -- previously only carried stops were considered,
        # so a channel whose schedule ran out soon (but after the anchor)
        # lost its placeholder and went dark in the guide. Now such
        # channels get an honest tail placeholder starting at
        # max(anchor, latest stop), so it never overlaps real listings.
        horizon = anchor + timedelta(hours=12)
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        fresh_max_stop = {}
        fresh_min_start = {}
        stop_re = re.compile(r'stop="(\d{14})')
        start_re = re.compile(r'start="(\d{14})')
        for src in (verified_fresh, service_progs):
            for fcid, plist in src.items():
                for p in plist:
                    m = stop_re.search(p)
                    if m:
                        st = parse_ts(m.group(1))
                        if st and (fcid not in fresh_max_stop
                                   or st > fresh_max_stop[fcid]):
                            fresh_max_stop[fcid] = st
                    m2 = start_re.search(p)
                    if m2:
                        st2 = parse_ts(m2.group(1))
                        if st2 and (fcid not in fresh_min_start
                                    or st2 < fresh_min_start[fcid]):
                            fresh_min_start[fcid] = st2
        # Clone targets inherit their source's latest stop and earliest start
        # so the rolling placeholder loop below never overlaps cloned real
        # listings and fills leading gaps correctly.
        for _tid, _sid in CLONE_SOURCES.items():
            if _tid in roster_ids and _sid in fresh_max_stop:
                fresh_max_stop[_tid] = fresh_max_stop[_sid]
            if _tid in roster_ids and _sid in fresh_min_start:
                fresh_min_start[_tid] = fresh_min_start[_sid]
        for cid in order:
            if classes.get(cid) not in ('placeholder', 'regular'):
                continue
            latest = reg_max_stop.get(cid, epoch)
            fs = fresh_max_stop.get(cid)
            if fs and fs > latest:
                latest = fs
            # Leading gap: if the first programme starts after anchor, fill
            # from anchor to that start so there's no "No information" gap.
            first = reg_min_start.get(cid)
            ffs = fresh_min_start.get(cid)
            if ffs and (first is None or ffs < first):
                first = ffs
            if first and first > anchor:
                disp = roster.get(cid, ('', ''))[0] or PLACEHOLDER_TITLE
                fout.write(serialize_fresh_prog(fmt_ts(anchor),
                                               fmt_ts(first),
                                               cid, disp,
                                               PLACEHOLDER_DESC))
                c['placeholder_written'] += 1
            if latest <= horizon:
                start = max(anchor, latest) if latest > epoch else anchor
                # Don't double-write if we already filled a leading gap that
                # extends past latest (shouldn't happen, but be safe).
                if not (first and first > anchor and start < first):
                    disp = roster.get(cid, ('', ''))[0] or PLACEHOLDER_TITLE
                    fout.write(rolling_placeholder(cid, start, disp))
                    c['placeholder_written'] += 1
        for tid in sorted(verified_fresh):
            for p in verified_fresh[tid]:
                # 2026-09-29: defensive drop of invalid-duration programmes
                # (stop <= start) from live feeds — one bad feed entry must
                # not nuke the whole build. Logged, not silent.
                _m = _span_re.search(p)
                if _m:
                    _ds, _de = parse_ts(_m.group(1)), parse_ts(_m.group(2))
                    if _ds and _de and _de <= _ds:
                        log(f"drop_invalid_duration: {tid} "
                            f"{_m.group(1)}->{_m.group(2)}")
                        c['dropped_invalid_duration'] = \
                            c.get('dropped_invalid_duration', 0) + 1
                        continue
                fout.write(p)
                c['fresh_appended'] += 1
                for _t in clone_targets.get(tid, ()):
                    fout.write(_chan_attr_re.sub(f'channel="{_t}"', p,
                                                 count=1))
                    c['fresh_appended'] += 1
        for cid in sorted(service_new):
            if cid in roster_ids and classes.get(cid) == 'regular' \
                    and cid not in verified_new:
                for p in service_progs[cid]:
                    fout.write(p)
                    c['service_appended'] += 1
                    for _t in clone_targets.get(cid, ()):
                        fout.write(_chan_attr_re.sub(f'channel="{_t}"', p,
                                                     count=1))
                        c['service_appended'] += 1
        fout.write('</tv>\n')
    log("build: " + ", ".join(f"{k}={v}" for k, v in c.items()))
    return c


# ---------------------------------------------------------------- timezone shifts
# Chris's rule (2026-09-21): USA timezone-variant channels are delay feeds of
# the East Coast feed. East is the base; variants shift back:
#   West -3h, Mountain -2h, Central -1h, Alaska -4h, Hawaii -6h.
# Verified 2026-09-21: 14 of 20 east/west pairs already shift correctly;
# HBO/NatGeo/StarZ/StarzEncore/Flix/Bravo West did not (0-27% title match),
# and no Alaska/Hawaii channels exist at all. This step re-derives any
# variant whose programmes don't match the shifted East feed, so the rule
# holds on every build instead of depending on whichever feed matched.
TZ_SHIFT_OFFSETS = {'west': -3, 'mountain': -2, 'central': -1,
                    'alaska': -4, 'hawaii': -6}
TZ_SHIFT_MIN_EAST = 1     # if east has any future data, west gets built from it
TZ_SHIFT_MIN_RATIO = 0.8  # variant must match >= this or it gets re-derived

_zone_name_re = re.compile(
    r'^USA\s+(.+?)\s+(East|West|Mountain|Central|Alaska|Hawaii)\s*\*?\s*$', re.I)
_ts_attr_re = re.compile(r'(start|stop)="(\d{14})([^"]*)"')


def _shift_prog_xml(xml, hours):
    def rep(m):
        attr, digits, rest = m.group(1), m.group(2), m.group(3)
        try:
            dt = datetime.strptime(digits, '%Y%m%d%H%M%S') + timedelta(hours=hours)
        except (ValueError, TypeError):
            # Malformed timestamp from provider feed — leave as-is rather than crashing
            return m.group(0)
        return f'{attr}="{dt.strftime("%Y%m%d%H%M%S")}{rest}"'
    return _ts_attr_re.sub(rep, xml)


def enforce_timezone_shifts(out_path, roster):
    """Re-derive broken USA timezone-variant feeds from the East feed."""
    groups = {}
    for cid, (name, _icon) in roster.items():
        m = _zone_name_re.match(name or '')
        if not m:
            continue
        base = re.sub(r'\s+', ' ', m.group(1)).strip().lower()
        # 2026-09-21: keep ALL candidates per zone — duplicate "East"
        # channels exist (e.g. two "USA HBO East*"); the one with data wins.
        groups.setdefault(base, {}).setdefault(m.group(2).lower(), []).append(cid)

    prog_re = re.compile(
        r'<programme start="([^"]+)" stop="([^"]+)"[^>]*channel="([^"]+)"[^>]*>'
        r'(.*?)</programme>', re.S)
    title_re = re.compile(r'<title[^>]*>([^<]*)</title>')
    text = open(out_path, encoding='utf-8').read()
    ch_progs = {}
    for m in prog_re.finditer(text):
        s = parse_ts(m.group(1))
        e = parse_ts(m.group(2))
        tm = title_re.search(m.group(4))
        title = tm.group(1) if tm else ''
        if s and e:
            ch_progs.setdefault(m.group(3), []).append((s, e, title, m.group(0)))

    def is_ph(t):
        return t.strip().lower().rstrip('.') == 'programming'

    now = datetime.now(timezone.utc)
    repaired = {}
    for base, zones in groups.items():
        if 'east' not in zones:
            continue
        # 2026-09-21: when duplicate "East" channels exist, copy from the
        # one with the most future real programmes — never the empty one.
        def _east_score(c):
            return sum(1 for _s, e, t, _x in ch_progs.get(c, [])
                       if e > now and not is_ph(t))
        east_cid = max(zones['east'], key=_east_score)
        east_all = ch_progs.get(east_cid, [])
        # 2026-09-21: only FUTURE east programmes can seed a re-derivation.
        # Shifting expired listings once wiped a variant's real future
        # schedule (hbowest.us lost 42 future programmes to 9 dead hbo.us
        # ones). An east feed with no future data is skipped, never copied.
        east_future = [(s, e, t, x) for s, e, t, x in east_all if e > now]
        east_real = [(s, t) for s, _e, t, _x in east_future if not is_ph(t)]
        if len(east_real) < TZ_SHIFT_MIN_EAST:
            continue
        east_max_stop = max(e for _s, e, _t, _x in east_future)
        for zone, off in TZ_SHIFT_OFFSETS.items():
            if zone not in zones or zone == 'east':
                continue
            for var_cid in zones[zone]:
                var_set = {(t, s) for s, _e, t, _x in ch_progs.get(var_cid, [])}
                delta = timedelta(hours=off)
                match = sum(1 for s, t in east_real if (t, s + delta) in var_set)
                if match / len(east_real) >= TZ_SHIFT_MIN_RATIO:
                    continue
                new_xml = []
                for s, e, t, x in east_future:
                    nx = _shift_prog_xml(x, off).replace(
                        f'channel="{east_cid}"', f'channel="{var_cid}"', 1)
                    new_xml.append(nx)
                # keep variant programmes that extend past the east window so no
                # coverage is ever lost by the re-derivation
                horizon = east_max_stop + delta
                for s, e, t, x in ch_progs.get(var_cid, []):
                    if s >= horizon:
                        new_xml.append(x)
                repaired[var_cid] = new_xml
                log(f"tz-shift: {var_cid} re-derived from {east_cid} "
                    f"({off}h, match was {match}/{len(east_real)})")

    if not repaired:
        return {'groups': len(groups), 'repaired': 0, 'programmes_rederived': 0}

    tv_close = text.rfind('</tv>')
    body, tail = text[:tv_close], text[tv_close:]
    tmp = out_path + '.tzfix'
    with open(tmp, 'w', encoding='utf-8') as fout:
        last = 0
        for m in prog_re.finditer(body):
            if m.group(3) in repaired:
                fout.write(body[last:m.start()])
                last = m.end()
        fout.write(body[last:])
        for xmls in repaired.values():
            for x in xmls:
                fout.write(x + '\n')
        fout.write(tail)
    os.replace(tmp, out_path)
    return {'groups': len(groups), 'repaired': len(repaired),
            'programmes_rederived': sum(len(v) for v in repaired.values())}


def fix_247_aliases(out_path):
    """2026-09-27: Copy 24/7 marathon data to m3u-* alias channels.

    The roster contains both m3u-247-X (with real marathon schedules and
    Chris's catbox logos) and m3u-X (empty, dead imgur icons). Some of
    Chris's TiviMate sources use the m3u-X IDs. This copies the icon
    and all programmes from each m3u-247-X to its m3u-X counterpart.

    2026-09-28 pass 2: the 156 extra-roster 24/7 channels (Toddler/Anime)
    exist ONLY in the standalone 24/7 file as m3u-247-X; their provider-ID
    counterparts (m3u-X) sit in this main file with a "Programming"
    placeholder and a dead icon. TiviMate matches Chris's playlist by
    provider ID, so without this the real marathon grids never reach his
    guide -- refreshing the 24/7 source could not fix it (wrong IDs, not
    stale data). This pass generates the same deterministic marathon
    blocks build_247_epg.py writes and injects them under the m3u-X IDs,
    after stripping the placeholder block. A channel is only touched when
    its current programmes are all placeholders (or absent) -- real data
    is never overwritten.
    """
    text = open(out_path, encoding='utf-8').read()

    # Find m247 icons: <channel id="m3u-247-xxx"> ... <icon src="..." />
    ch_icon_re = re.compile(
        r'<channel id="(m3u-247-[^"]+)">.*?<icon src="([^"]+)"', re.S)
    icons_247 = {m.group(1): m.group(2) for m in ch_icon_re.finditer(text)}

    # All channel ids in this file
    main_ids = set(re.findall(r'<channel id="([^"]+)">', text))

    # Find which m3u-* (non-247) channels exist
    ch_id_re = re.compile(r'<channel id="(m3u-(?!247-)[^"]+)"')
    m3u_ids = set(m.group(1) for m in ch_id_re.finditer(text))

    # Existing programme titles per channel (one scan) -- placeholder-only
    # safety check: never overwrite real listings.
    title_re = re.compile(
        r'<programme[^>]*channel="([^"]+)"[^>]*>.*?<title[^>]*>([^<]*)</title>',
        re.S)
    titles_by_cid = {}
    for c, t in title_re.findall(text):
        titles_by_cid.setdefault(c, set()).add(t.strip())

    def _is_placeholder_only(cid):
        return not (titles_by_cid.get(cid, set()) - {'Programming'})

    # Build pairs (pass 1: m3u-247-X counterpart already in this file)
    pairs = {}  # m3u_id -> (m247_id, icon)
    for m247_id, icon in icons_247.items():
        m3u_id = 'm3u-' + m247_id[8:]
        if m3u_id in m3u_ids and _is_placeholder_only(m3u_id):
            pairs[m3u_id] = (m247_id, icon)

    # Collect programmes from m247 channels present in this file
    prog_re = re.compile(
        r'<programme start="[^"]+" stop="[^"]+"[^>]*channel="([^"]+)"[^>]*>'
        r'.*?</programme>', re.S)
    progs_247 = {}
    for m in prog_re.finditer(text):
        cid = m.group(1)
        if cid in icons_247:
            progs_247.setdefault(cid, []).append(m.group(0))

    # ---- pass 2: extra-roster 24/7 channels absent from this file ----
    extra_pairs = 0
    try:
        extra = json.load(open(os.path.join(BUILD_DIR, '247_extra_roster.json'),
                               encoding='utf-8'))
        if isinstance(extra, dict):
            extra = list(extra.values())
        chris_layer = json.load(open(os.path.join(BUILD_DIR, 'chris_icon_urls.json'),
                                     encoding='utf-8'))
        poster_layer = json.load(open(os.path.join(BUILD_DIR, 'poster_hunt_urls.json'),
                                      encoding='utf-8'))
    except (OSError, ValueError) as exc:
        log(f"247-aliases pass 2 skipped: {exc}")
        extra, chris_layer, poster_layer = [], {}, {}
    if extra:
        # Known slug mismatches: extra-roster m3u-247-X id -> actual
        # provider m3u-* id in this file (same stream, different slug).
        EXTRA_ALIASES = {
            'm3u-247-care-bears-welcome-to-care-a-lot':
                'm3u-care-bears-welcome-to-carealot',
            'm3u-247-steins-gate': 'm3u-steinsgate',
        }
        anchor = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        end = anchor + timedelta(days=7)
        for e in extra:
            m247_id = e.get('id')
            if not (m247_id and m247_id.startswith('m3u-247-')):
                continue
            m3u_id = EXTRA_ALIASES.get(m247_id, 'm3u-' + m247_id[8:])
            if (m3u_id in pairs or m3u_id not in m3u_ids
                    or m247_id in main_ids
                    or not _is_placeholder_only(m3u_id)):
                continue
            icon = (chris_layer.get(m247_id) or poster_layer.get(m247_id)
                    or e.get('icon'))
            if not icon:
                continue
            block_mins = int(e.get('block_mins') or 30)
            title = e.get('title') or m3u_id
            desc = e.get('desc') or ''
            gen = []
            t = anchor
            while t < end:
                stop = min(t + timedelta(minutes=block_mins), end)
                gen.append(
                    f'  <programme start="{t.strftime("%Y%m%d%H%M%S +0000")}" '
                    f'stop="{stop.strftime("%Y%m%d%H%M%S +0000")}" '
                    f'channel="{m247_id}">\n'
                    f'    <title>{escape(title)}</title>\n'
                    f'    <desc>{escape(desc)}</desc>\n'
                    f'  </programme>')
                t = stop
            icons_247[m247_id] = icon
            progs_247[m247_id] = gen
            pairs[m3u_id] = (m247_id, icon)
            extra_pairs += 1

    if not pairs:
        return {'pairs': 0, 'icons_fixed': 0, 'programmes_copied': 0,
                'extra_roster_pairs': 0, 'placeholders_stripped': 0}

    # Remove the placeholder programmes on paired m3u-* channels (one scan)
    # so the injected marathon blocks never overlap them.
    paired_ids = set(pairs)
    stripped = [0]
    prog_all_re = re.compile(
        r'<programme[^>]*channel="([^"]+)"[^>]*>.*?</programme>\s*', re.S)

    def _strip_prog(m):
        if m.group(1) in paired_ids:
            stripped[0] += 1
            return ''
        return m.group(0)

    text = prog_all_re.subn(_strip_prog, text)[0]

    # Fix icons: replace <icon src="..."> inside m3u-* channel blocks
    icons_fixed = 0

    def _fix_icon(m):
        nonlocal icons_fixed
        cid, inner = m.group(1), m.group(2)
        if cid in pairs:
            new_icon = pairs[cid][1]
            inner2, n = re.subn(r'<icon src="[^"]+"',
                                f'<icon src="{new_icon}"', inner, count=1)
            if not n:
                # no icon element at all: insert one after display-name
                inner2, n = re.subn(r'(<display-name[^>]*>[^<]*</display-name>)',
                                    rf'\g<1><icon src="{new_icon}" />',
                                    inner, count=1)
            if n:
                icons_fixed += 1
                return f'<channel id="{cid}">{inner2}</channel>'
        return m.group(0)

    ch_block_re = re.compile(r'<channel id="(m3u-(?!247-)[^"]+)">(.*?)</channel>', re.S)
    text = ch_block_re.sub(_fix_icon, text)

    # Inject copied programmes before </tv>
    injected = 0
    prog_copies = []
    for m3u_id, (m247_id, _icon) in pairs.items():
        for prog_xml in progs_247.get(m247_id, []):
            new_prog = prog_xml.replace(f'channel="{m247_id}"',
                                        f'channel="{m3u_id}"', 1)
            prog_copies.append(new_prog)
            injected += 1
    if prog_copies:
        text = text.replace('</tv>', ''.join(prog_copies) + '</tv>', 1)

    # Write back
    tmp = out_path + '.tmp247'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(text)
    os.replace(tmp, out_path)
    log(f"247-aliases: {len(pairs)} channels ({extra_pairs} extra-roster), "
        f"{icons_fixed} icons fixed, {stripped[0]} placeholders stripped, "
        f"{injected} programmes copied")
    return {'pairs': len(pairs), 'icons_fixed': icons_fixed,
            'programmes_copied': injected, 'extra_roster_pairs': extra_pairs,
            'placeholders_stripped': stripped[0]}


def apply_rehosted_logos(roster):
    """Rewrite roster icons via rehosted_logo_map.json (2026-10-01).

    ~4.3k http:// provider/IP-hosted logos were rehosted on GitHub because
    Android/TiviMate blocks cleartext http. Missing/unreadable map -> no-op.
    Returns the number of roster icons rewritten.
    """
    try:
        with open(REHOST_MAP_FILE, encoding='utf-8') as f:
            rmap = json.load(f)
    except (OSError, ValueError):
        return 0
    n = 0
    for cid, (name, icon) in list(roster.items()):
        new = rmap.get(icon)
        if new:
            roster[cid] = (name, new)
            n += 1
    if n:
        log(f"rehosted logos: rewrote {n} roster icons to GitHub URLs")
    return n


# 2026-10-02: serve all GitHub-hosted logos via jsDelivr CDN — TiviMate
# does not reliably load raw.githubusercontent.com. Runs after the rehost
# map so already-migrated raw URLs get rewritten too.
JSDELIVR_OLD_PREFIX = "https://raw.githubusercontent.com/kamikaze0129/my-epg/main/"
JSDELIVR_NEW_PREFIX = "https://cdn.jsdelivr.net/gh/kamikaze0129/my-epg@main/"

def apply_jsdelivr_cdn(roster):
    """Rewrite any kamikaze0129/my-epg raw.githubusercontent.com icon URLs
    to the jsDelivr CDN equivalent. Returns the number rewritten."""
    n = 0
    for cid, (name, icon) in list(roster.items()):
        if icon and icon.startswith(JSDELIVR_OLD_PREFIX):
            roster[cid] = (name, JSDELIVR_NEW_PREFIX + icon[len(JSDELIVR_OLD_PREFIX):])
            n += 1
    if n:
        log(f"jsdelivr: rewrote {n} roster icons to CDN URLs")
    return n


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
    if rep['stale_channels'] > 0.02 * rep['channels']:
        failures.append(f"freshness: {rep['stale_channels']} channels with no "
                        f"coverage past now+12h (>2%)")
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
    out_arg = None
    for i, a in enumerate(argv):
        if a == '--service-xml' and i + 1 < len(argv):
            service_xml = argv[i + 1]
        if a == '--workdir' and i + 1 < len(argv):
            workdir = argv[i + 1]
        if a == '--out' and i + 1 < len(argv):
            out_arg = argv[i + 1]
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

        # 2b. Sunday roster patches: renames (KMTV), drops (Hunted),
        # Chris icons win, hunt posters fill gaps.
        order = apply_roster_patches(roster, order, classes)

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

        # 3b. EPGShare01 international feeds (best-effort; never aborts).
        # Reuses extract_feed_programmes: same >=5-programme rule, same
        # cutoff semantics. The epg.pw verified set wins on any overlap.
        es_raw = json.load(open(EPGSHARE_MATCHES)) \
            if os.path.isfile(EPGSHARE_MATCHES) else {}
        es_matches = {t: (v['feed_channel_id'], v['feed_code'])
                      for t, v in es_raw.items() if t in roster}
        es_codes = sorted(set(fcc for _, fcc in es_matches.values()))
        es_paths = fetch_epgshare_feeds(workdir, es_codes) if es_codes else {}
        es_matches = {t: v for t, v in es_matches.items() if v[1] in es_paths}
        es_fresh, es_skipped = extract_feed_programmes(
            es_matches, es_paths, cutoff14)
        for tid, progs in es_fresh.items():
            if tid not in verified_fresh:
                verified_fresh[tid] = progs
        report['stages']['epgshare'] = {
            'targets': len(es_matches),
            'refreshed': len(es_fresh),
            'fresh_programmes': sum(len(v) for v in es_fresh.values()),
            'skipped_lt5': len(es_skipped),
            'feeds_ok': len(es_paths), 'feeds_wanted': len(es_codes)}

        # 3b2. EPGTalk 7-day guides (best-effort; never aborts).
        # Name-matched in pubfeed/epgtalk_matches.json. Same >=5-programme
        # rule and cutoff semantics as EPGShare01. Fills only channels with
        # no real listings yet, so the epg.pw verified set and EPGShare01 win
        # on overlap; runs before the legacy iptvtalk stage (3h, same upstream
        # guides), so EPGTalk wins there as well.
        et_raw = json.load(open(EPGTALK_MATCHES)) \
            if os.path.isfile(EPGTALK_MATCHES) else {}
        et_matches = {t: (v['feed_channel_id'], v['feed_code'])
                      for t, v in et_raw.items() if t in roster}
        et_codes = sorted(set(fcc for _, fcc in et_matches.values()))
        et_paths = fetch_epgtalk_feeds(workdir, et_codes) if et_codes else {}
        et_matches = {t: v for t, v in et_matches.items() if v[1] in et_paths}
        et_fresh, et_skipped = extract_feed_programmes(
            et_matches, et_paths, cutoff14)
        for tid, progs in et_fresh.items():
            if tid not in verified_fresh:
                verified_fresh[tid] = progs
        report['stages']['epgtalk'] = {
            'targets': len(et_matches),
            'refreshed': len(et_fresh),
            'fresh_programmes': sum(len(v) for v in et_fresh.values()),
            'skipped_lt5': len(et_skipped),
            'feeds_ok': len(et_paths), 'feeds_wanted': len(et_codes)}

        # 3c. vcicio/US-EPG merged US guide (best-effort; never aborts).
        # 9.6-day window for USA channels incl. locals (KGMB Honolulu etc.).
        # It does NOT unconditionally overwrite epg.pw/EPGShare data: for each
        # channel the feed with the longest valid future span of REAL
        # programmes wins (ties keep the incumbent). Placeholder-only or
        # shorter feeds can never displace longer real listings.
        vc_raw = json.load(open(VCICIO_MATCHES)) \
            if os.path.isfile(VCICIO_MATCHES) else {}
        vc_matches = {t: (v['feed_channel_id'], 'vcicio')
                      for t, v in vc_raw.items() if t in roster}
        vc_path = fetch_vcicio_feed(workdir) if vc_matches else None
        if vc_path:
            vc_fresh, vc_skipped = extract_feed_programmes(
                vc_matches, {'vcicio': vc_path}, cutoff14)
            now = datetime.now(timezone.utc)
            vc_won = vc_kept = vc_ph_rejected = 0
            for tid, progs in vc_fresh.items():
                vc_n, vc_stop = future_span(progs, now)
                if tid not in verified_fresh:
                    verified_fresh[tid] = progs
                    vc_won += 1
                    continue
                if vc_n == 0:
                    # vcicio brought no real future programmes; never let a
                    # placeholder-only/short feed displace real listings.
                    vc_ph_rejected += 1
                    continue
                inc_n, inc_stop = future_span(verified_fresh[tid], now)
                if inc_stop is None or (vc_stop is not None and vc_stop > inc_stop):
                    verified_fresh[tid] = progs
                    vc_won += 1
                else:
                    vc_kept += 1
            report['stages']['vcicio'] = {
                'targets': len(vc_matches),
                'refreshed': len(vc_fresh),
                'fresh_programmes': sum(len(v) for v in vc_fresh.values()),
                'skipped_lt5': len(vc_skipped),
                'won_longest_span': vc_won,
                'incumbent_kept_longer': vc_kept,
                'placeholder_rejected': vc_ph_rejected}
            log(f"vcicio: {len(vc_fresh)} USA channels refreshed "
                f"({vc_won} won longest-span, {vc_kept} kept incumbent, "
                f"{vc_ph_rejected} placeholder-only rejected)")
        else:
            report['stages']['vcicio'] = {
                'targets': len(vc_matches), 'refreshed': 0,
                'note': 'feed download failed; channels keep carried data'}

        # 3e-3h. Sunday 2026-09-27 approved sources (best-effort).
        # Precedence: epg.pw verified > EPGShare01 > vcicio > iptv-epg.org >
        # Sky DE > Sky UK > iptvtalk > TVGuide. Each fills only channels with
        # no real listings yet; never displaces an incumbent.
        def _fill_from(stage_name, matches_file, feed_paths, extra=None):
            raw = json.load(open(matches_file)) \
                if os.path.isfile(matches_file) else []
            ms = {}
            for m in raw:
                tid = m.get('target_id')
                if tid and tid in roster and tid not in verified_fresh:
                    fcc = extra(m) if extra else m.get('feed_country')
                    ms[tid] = (m.get('feed_channel_id'), fcc)
            ms = {t: v for t, v in ms.items() if v[1] in feed_paths}
            if not ms:
                report['stages'][stage_name] = {'targets': 0, 'refreshed': 0}
                return
            fsh, skp = extract_feed_programmes(ms, feed_paths, cutoff14)
            for tid, progs in fsh.items():
                if tid not in verified_fresh:
                    verified_fresh[tid] = progs
            report['stages'][stage_name] = {
                'targets': len(ms), 'refreshed': len(fsh),
                'fresh_programmes': sum(len(v) for v in fsh.values()),
                'skipped_lt5': len(skp)}

        # 3e. iptv-epg.org US feed
        ie_path = fetch_xml_feed(
            IPTVEPG_URL, os.path.join(workdir, 'iptv-epg-us.xml.gz'), 'iptv-epg.org US')
        if ie_path:
            _fill_from('iptv_epg_org', IPTVEPG_MATCHES, {'iptv-epg-us': ie_path})

        # 3f. Sky Germany (plain XML)
        sd_path = fetch_xml_feed(
            SKYDE_URL, os.path.join(workdir, 'sky_de.xml'), 'sky-de')
        if sd_path:
            _fill_from('sky_de', SKYDE_MATCHES, {'sky-de': sd_path})

        # 3g. Sky UK (plain XML)
        su_path = fetch_xml_feed(
            SKYUK_URL, os.path.join(workdir, 'sky_uk.xml'), 'sky-uk')
        if su_path:
            _fill_from('sky_uk', SKYUK_MATCHES, {'sky-uk': su_path})

        # 3h. iptvtalk (4 subfeeds; match picks its subfeed)
        it_paths = {}
        for sub, url in IPTVTALK_URLS.items():
            p = fetch_xml_feed(
                url, os.path.join(workdir, f'iptvtalk_{sub}.xml.gz'),
                f'iptvtalk {sub}')
            if p:
                it_paths[f'iptvtalk-{sub}'] = p
        if it_paths:
            _fill_from('iptvtalk', IPTVTALK_MATCHES, it_paths,
                       extra=lambda m: 'iptvtalk-' + m.get('feed_subfeed', 'US'))

        # 3i. TVGuide API (needs TVGUIDE_API_KEY; best-effort)
        tg_raw = json.load(open(TVGUIDE_MATCHES)) \
            if os.path.isfile(TVGUIDE_MATCHES) else []
        tg_matches = {m['target_id']: m for m in tg_raw
                      if m.get('target_id') in roster
                      and m['target_id'] not in verified_fresh}
        tg_fresh, tg_skipped = fetch_tvguide(tg_matches, workdir)
        for tid, progs in tg_fresh.items():
            if tid not in verified_fresh:
                verified_fresh[tid] = progs
        report['stages']['tvguide'] = {
            'targets': len(tg_matches), 'refreshed': len(tg_fresh),
            'fresh_programmes': sum(len(v) for v in tg_fresh.values()),
            'skipped': len(tg_skipped)}

        # 3j. Event-label injection (PPV / ESPN+ / FloSports / Fanatiz)
        report['stages']['events'] = inject_event_labels(roster, verified_fresh)

        # 3d. NFL Sunday Ticket schedule injection (best-effort; never aborts).
        # One-off game entries from nfl_sunday_ticket.json. Skips itself once
        # valid_until_utc has passed. Only fills channels with no real future
        # data -- never overwrites existing listings. Also refreshes the
        # roster display names from the injected matchup (2026-09-27: the
        # old code injected programmes but left 705-707 showing last week's
        # matchup and 708-713 blank).
        nfl_injected = 0
        nfl_renamed = 0
        try:
            if os.path.isfile(NFL_SCHEDULE):
                nfl_raw = json.load(open(NFL_SCHEDULE))
                valid_until = nfl_raw.get('_meta', {}).get('valid_until_utc', '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = nfl_raw.get('_display_names', {}) or {}
                    _nfl_title_re = re.compile(
                        r'<title[^>]*>\s*NFL Football:\s*(.+?)\s+at\s+(.+?)\s*</title>',
                        re.I)
                    _nfl_kick_re = re.compile(
                        r'Kickoff\s+(\d{1,2}:\d{2}\s*[AP]M\s*ET)', re.I)
                    _nfl_num_re = re.compile(r'nfl-sunday-(70[5-9]|71[0-7])\b',
                                             re.I)
                    for tcid, plist in nfl_raw.items():
                        if tcid.startswith('_') or tcid not in roster:
                            continue
                        if tcid not in verified_fresh:
                            verified_fresh[tcid] = plist
                            nfl_injected += 1
                        # Derive the display name from the injected game:
                        # prefer the updater's _display_names, else parse
                        # the programme XML.
                        _disp = _disp_names.get(tcid)
                        if not _disp:
                            _nm = _nfl_num_re.search(tcid)
                            _t = _nfl_title_re.search(plist[0] if plist else '')
                            _k = _nfl_kick_re.search(plist[0] if plist else '')
                            if _nm and _t:
                                # 2026-09-27: Keep display-name as "USA NFL Sunday 7XX:"
                                # to match Chris's playlist tvg-name for TiviMate
                                # auto-matching. Matchup details stay in programme
                                # titles, not the channel name.
                                _disp = f"USA NFL Sunday {_nm.group(1)}:"
                        if _disp and roster[tcid][0] != _disp:
                            roster[tcid] = (_disp, roster[tcid][1])
                            nfl_renamed += 1
                else:
                    log("nfl: schedule expired, skipping")
        except Exception as e:
            log(f"nfl: injection failed ({e}); continuing")
        report['stages']['nfl'] = {'injected_channels': nfl_injected,
                                   'renamed': nfl_renamed}
        if nfl_renamed:
            log(f"nfl: refreshed {nfl_renamed} display names")

        # 3d2. NHL schedule injection (best-effort; never aborts).
        # Same pattern as NFL: one-off game entries from nhl_schedule.json.
        # Skips itself once valid_until_utc has passed. Only fills channels
        # with no real future data -- never overwrites existing listings.
        # Also refreshes the roster display names from the injected matchup.
        nhl_injected = 0
        nhl_renamed = 0
        try:
            if os.path.isfile(NHL_SCHEDULE):
                nhl_raw = json.load(open(NHL_SCHEDULE))
                valid_until = nhl_raw.get('_meta', {}).get('valid_until_utc', '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = nhl_raw.get('_display_names', {}) or {}
                    _nhl_title_re = re.compile(
                        r'<title[^>]*>\s*NHL Hockey:\s*(.+?)\s+at\s+(.+?)\s*</title>',
                        re.I)
                    _nhl_kick_re = re.compile(
                        r'Puck drop\s+(\d{1,2}:\d{2}\s*[AP]M\s*ET)', re.I)
                    _nhl_num_re = re.compile(r'm3u-usa-nhl-0([1-6])\b', re.I)
                    for tcid, plist in nhl_raw.items():
                        if tcid.startswith('_') or tcid not in roster:
                            continue
                        if tcid not in verified_fresh:
                            verified_fresh[tcid] = plist
                            nhl_injected += 1
                        _disp = _disp_names.get(tcid)
                        if not _disp:
                            _nm = _nhl_num_re.search(tcid)
                            _t = _nhl_title_re.search(plist[0] if plist else '')
                            _k = _nhl_kick_re.search(plist[0] if plist else '')
                            if _nm and _t:
                                _disp = (f"USA NHL 0{_nm.group(1)}: "
                                         f"{_t.group(1)} vs {_t.group(2)}"
                                         + (f" @ {_k.group(1)}" if _k else ""))
                        if _disp and roster[tcid][0] != _disp:
                            roster[tcid] = (_disp, roster[tcid][1])
                            nhl_renamed += 1
                else:
                    log("nhl: schedule expired, skipping")
        except Exception as e:
            log(f"nhl: injection failed ({e}); continuing")
        report['stages']['nhl'] = {'injected_channels': nhl_injected,
                                   'renamed': nhl_renamed}
        if nhl_renamed:
            log(f"nhl: refreshed {nhl_renamed} display names")

        # 3d3. WNBA playoff injection (best-effort; never aborts).
        # Same pattern as NFL/NHL: one-off game entries from wnba_playoffs.json.
        # Skips itself once valid_until_utc has passed. Only fills channels
        # with no real future data -- never overwrites existing listings.
        # Also refreshes the roster display names from the injected matchup.
        wnba_injected = 0
        wnba_renamed = 0
        try:
            if os.path.isfile(WNBA_SCHEDULE):
                wnba_raw = json.load(open(WNBA_SCHEDULE))
                valid_until = wnba_raw.get('_meta', {}).get('valid_until_utc', '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = wnba_raw.get('_display_names', {}) or {}
                    _wnba_title_re = re.compile(
                        r'<title[^>]*>\s*WNBA[^:]*:\s*(.+?)\s+at\s+(.+?)\s*</title>',
                        re.I)
                    _wnba_tip_re = re.compile(
                        r'Tipoff\s+(\d{1,2}:\d{2}\s*[AP]M\s*ET)', re.I)
                    _wnba_num_re = re.compile(r'm3u-usa-wnba-0([1-7])\b', re.I)
                    for tcid, plist in wnba_raw.items():
                        if tcid.startswith('_') or tcid not in roster:
                            continue
                        if tcid not in verified_fresh:
                            verified_fresh[tcid] = plist
                            wnba_injected += 1
                        _disp = _disp_names.get(tcid)
                        if not _disp:
                            _nm = _wnba_num_re.search(tcid)
                            _t = _wnba_title_re.search(plist[0] if plist else '')
                            _k = _wnba_tip_re.search(plist[0] if plist else '')
                            if _nm and _t:
                                _disp = (f"USA WNBA 0{_nm.group(1)}: "
                                         f"{_t.group(1)} vs {_t.group(2)}"
                                         + (f" @ {_k.group(1)}" if _k else ""))
                        if _disp and roster[tcid][0] != _disp:
                            roster[tcid] = (_disp, roster[tcid][1])
                            wnba_renamed += 1
                else:
                    log("wnba: schedule expired, skipping")
        except Exception as e:
            log(f"wnba: injection failed ({e}); continuing")
        report['stages']['wnba'] = {'injected_channels': wnba_injected,
                                    'renamed': wnba_renamed}
        if wnba_renamed:
            log(f"wnba: refreshed {wnba_renamed} display names")

        # 3d4. BIG10+ schedule injection (best-effort; never aborts).
        # Same pattern as NFL/NHL/WNBA: one-off event entries from
        # big10plus_schedule.json. Skips itself once valid_until_utc has
        # passed. Only fills channels with no real future data -- never
        # overwrites existing listings. Also refreshes the roster display
        # names from the injected matchup (the old code left fossilized
        # "Fri @ Sep 18" names in the guide).
        # Number-based resolution: the JSON is keyed by logical IDs
        # (m3u-big10-01 .. m3u-big10-24); each is resolved to the actual
        # roster ID (fossilized per-event form) here, and the programme
        # XML channel attributes are rewritten to match.
        big10_injected = 0
        big10_renamed = 0
        big10_unmatched = []
        try:
            if os.path.isfile(BIG10PLUS_SCHEDULE):
                big10_raw = json.load(open(BIG10PLUS_SCHEDULE))
                valid_until = big10_raw.get('_meta', {}).get(
                    'valid_until_utc', '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = big10_raw.get('_display_names', {}) or {}
                    _big10_lid_re = re.compile(r'^m3u-big10-0*(\d+)$', re.I)
                    _big10_rid_re = re.compile(r'^m3u-big10-0*(\d+)(?:-|$)',
                                               re.I)
                    _big10_chan_re = re.compile(r'channel="[^"]*"')
                    _num_to_id = {}
                    for _cid in roster:
                        _m = _big10_rid_re.match(_cid)
                        if _m:
                            _num_to_id.setdefault(int(_m.group(1)), _cid)
                    for _lid, _plist in big10_raw.items():
                        if _lid.startswith('_'):
                            continue
                        _lm = _big10_lid_re.match(_lid)
                        if not _lm:
                            continue
                        _tcid = _num_to_id.get(int(_lm.group(1)))
                        if not _tcid:
                            big10_unmatched.append(_lid)
                            continue
                        if _tcid not in verified_fresh:
                            verified_fresh[_tcid] = [
                                _big10_chan_re.sub(
                                    f'channel="{_tcid}"', _p, count=1)
                                for _p in _plist]
                            big10_injected += 1
                        _disp = _disp_names.get(_lid)
                        if _disp and roster[_tcid][0] != _disp:
                            roster[_tcid] = (_disp, roster[_tcid][1])
                            big10_renamed += 1
                else:
                    log("big10+: schedule expired, skipping")
        except Exception as e:
            log(f"big10+: injection failed ({e}); continuing")
        report['stages']['big10plus'] = {'injected_channels': big10_injected,
                                         'renamed': big10_renamed,
                                         'unmatched': big10_unmatched}
        if big10_renamed:
            log(f"big10+: refreshed {big10_renamed} display names")
        if big10_unmatched:
            log(f"big10+: no roster match for {big10_unmatched}")

        # 3d5. FloRacing schedule injection (best-effort; never aborts).
        # Same pattern as NFL/NHL/WNBA/BIG10+: event entries from
        # floracing_schedule.json for USA Flo Racing (m3u-usa-flo-racing).
        # Skips itself once valid_until_utc has passed. Only fills the
        # channel when it has no real future data -- never overwrites
        # existing listings. The channel ID is stable, so no number
        # resolution is needed; the JSON is keyed by the real roster ID.
        floracing_injected = 0
        try:
            if os.path.isfile(FLORACING_SCHEDULE):
                flo_raw = json.load(open(FLORACING_SCHEDULE))
                valid_until = flo_raw.get('_meta', {}).get('valid_until_utc',
                                                           '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = flo_raw.get('_display_names', {}) or {}
                    for tcid, plist in flo_raw.items():
                        if tcid.startswith('_') or tcid not in roster:
                            continue
                        if tcid not in verified_fresh:
                            verified_fresh[tcid] = plist
                            floracing_injected += 1
                        _disp = _disp_names.get(tcid)
                        if _disp and roster[tcid][0] != _disp:
                            roster[tcid] = (_disp, roster[tcid][1])
                else:
                    log("floracing: schedule expired, skipping")
        except Exception as e:
            log(f"floracing: injection failed ({e}); continuing")
        report['stages']['floracing'] = {
            'injected_channels': floracing_injected}
        if floracing_injected:
            log(f"floracing: injected {floracing_injected} channel(s)")

        # 3d6. NBA schedule injection (best-effort; never aborts).
        # Same pattern as NFL/NHL: one-off game entries from
        # nba_schedule.json for USA NBA 01-06 (NBA League Pass,
        # m3u-usa-nba-0X). Skips itself once valid_until_utc has passed.
        # Only fills channels with no real future data -- never overwrites
        # existing listings. Also refreshes the roster display names from
        # the injected matchup. The JSON is keyed by the real roster IDs,
        # so no number resolution is needed.
        nba_injected = 0
        nba_renamed = 0
        try:
            if os.path.isfile(NBA_SCHEDULE):
                nba_raw = json.load(open(NBA_SCHEDULE))
                valid_until = nba_raw.get('_meta', {}).get('valid_until_utc',
                                                           '')
                if valid_until and datetime.now(timezone.utc).isoformat() < valid_until:
                    _disp_names = nba_raw.get('_display_names', {}) or {}
                    for tcid, plist in nba_raw.items():
                        if tcid.startswith('_') or tcid not in roster:
                            continue
                        if tcid not in verified_fresh:
                            verified_fresh[tcid] = plist
                            nba_injected += 1
                        _disp = _disp_names.get(tcid)
                        if _disp and roster[tcid][0] != _disp:
                            roster[tcid] = (_disp, roster[tcid][1])
                            nba_renamed += 1
                else:
                    log("nba: schedule expired, skipping")
        except Exception as e:
            log(f"nba: injection failed ({e}); continuing")
        report['stages']['nba'] = {'injected_channels': nba_injected,
                                   'renamed': nba_renamed}
        if nba_injected:
            log(f"nba: injected {nba_injected} channel(s)")
        if nba_renamed:
            log(f"nba: refreshed {nba_renamed} display names")

        # 3d7. TV Passport locals + cable-network injection (best-effort;
        # never aborts). Chris's rule: FILL ONLY channels lacking meaningful
        # future data, or APPEND strictly after existing real listings end.
        # Never overwrite a healthy feed; never rename a provider display
        # name. Guards in mapping_guards.py already vetted the staged
        # mappings at updater time; this stage re-checks region routing
        # defensively before injecting.
        def _cable_prog_xml(cid, p):
            fmt = lambda s: (datetime.fromisoformat(s).astimezone(timezone.utc)
                             .strftime("%Y%m%d%H%M%S") + " +0000")
            out = [f'  <programme start="{fmt(p["start"])}" '
                   f'stop="{fmt(p["stop"])}" channel="{escape(cid)}">',
                   f'    <title>{escape(p.get("title", ""))}</title>']
            if p.get("episode"):
                out.append(f'    <sub-title>{escape(p["episode"])}</sub-title>')
            if p.get("desc"):
                out.append(f'    <desc>{escape(p["desc"])}</desc>')
            out.append('  </programme>')
            return "\n".join(out) + "\n"

        def _inject_3d6(schedule_path, kind):
            """Returns (filled, appended) channel counts."""
            filled, appended = 0, 0
            try:
                raw = json.load(open(schedule_path))
            except Exception as e:
                log(f"3d6/{kind}: no schedule file ({e}); skipping")
                return 0, 0
            now = datetime.now(timezone.utc)
            # normalize both file formats to {cid: [xml programme strings]}
            items = []
            if kind == "tvp":
                for tcid, plist in raw.items():
                    if tcid.startswith("_") or tcid not in roster:
                        continue
                    items.append((tcid, plist, {}))
            else:
                for tcid, src in (raw.get("sources") or {}).items():
                    if tcid not in roster:
                        continue
                    plist = [_cable_prog_xml(tcid, p)
                             for p in src.get("programmes", [])
                             if p.get("start") and p.get("stop")
                             and p.get("title")]
                    items.append((tcid, plist,
                                  {"source": src.get("source", "")}))
            for tcid, plist, meta in items:
                if not plist:
                    continue
                # mapping-guard re-check (defense in depth)
                try:
                    from mapping_guards import check_mapping
                    ok, reason = check_mapping(
                        tcid, roster[tcid][0], meta.get("source", ""))
                    if not ok:
                        log(f"3d6/{kind}: guard reject {tcid} ({reason})")
                        continue
                except Exception:
                    pass
                existing = verified_fresh.get(tcid)
                if not existing:
                    verified_fresh[tcid] = sorted(plist)
                    filled += 1
                    continue
                n_real, max_stop = future_span(existing, now)
                if n_real == 0:
                    # placeholder-only feed: replace with real data
                    verified_fresh[tcid] = sorted(plist)
                    filled += 1
                    continue
                if tcid in ROSTER_RENAMES:
                    # identity correction (wrong station in roster): the old
                    # programmes are wrong-station data -- replace, don't append
                    verified_fresh[tcid] = sorted(plist)
                    filled += 1
                    continue
                # append strictly after existing real listings end
                tail = [p for p in plist
                        if parse_ts(_span_re.search(p).group(1)) > max_stop]
                if tail:
                    verified_fresh[tcid] = sorted(existing + tail)
                    appended += 1
            return filled, appended

        tvp_f, tvp_a = _inject_3d6(TVP_SCHEDULE, "tvp")
        cab_f, cab_a = _inject_3d6(CABLE_SCHEDULE, "cable")
        report['stages']['tvp_locals'] = {'filled': tvp_f, 'appended': tvp_a}
        report['stages']['cable'] = {'filled': cab_f, 'appended': cab_a}
        if tvp_f or tvp_a or cab_f or cab_a:
            log(f"3d6: tvp filled={tvp_f} appended={tvp_a} | "
                f"cable filled={cab_f} appended={cab_a}")

        # 4. optional provider XML hook for regular channels
        service_progs = load_service_xml(service_xml) if service_xml else {}
        # 4b. Alaska/Hawaii: provider timestamps are Eastern-semantics;
        # shift AK -4h / HI -6h so they air on local time. Applies to
        # service-XML programmes only.
        for cid, hrs in SERVICE_TZ_SHIFTS.items():
            if cid in service_progs and hrs:
                service_progs[cid] = [_shift_prog_xml(x, hrs)
                                      for x in service_progs[cid]]
                log(f"service tz-shift: {cid} {hrs}h "
                    f"({len(service_progs[cid])} programmes)")
        if service_xml and not service_progs:
            log("WARNING: --service-xml given but yielded no programmes; "
                "regular channels will be carried over")

        # 4c. 2026-10-01: rewrite http:// provider logos to rehosted GitHub
        # URLs (Android/TiviMate blocks cleartext http).
        n_rehost = apply_rehosted_logos(roster)
        report['stages']['rehosted_logos'] = {'rewritten': n_rehost}

        # 4d. 2026-10-02: serve all GitHub-hosted logos via jsDelivr CDN.
        n_cdn = apply_jsdelivr_cdn(roster)
        report['stages']['jsdelivr_cdn'] = {'rewritten': n_cdn}

        # 5. build
        anchor = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        out_path = out_arg or os.path.join(workdir, 'epg_new.xml')
        counters = build_output(PREV_BUILD, out_path, roster, order, classes,
                                min_start_247, verified_fresh, service_progs, anchor)
        report['stages']['build'] = counters

        # 5b. enforce Chris's USA timezone-shift rule (east base; west -3h,
        # mountain -2h, central -1h, alaska -4h, hawaii -6h)
        tzc = enforce_timezone_shifts(out_path, roster)
        report['stages']['tz_shifts'] = tzc
        log(f"tz-shifts: {tzc['repaired']} variant feeds re-derived "
            f"across {tzc['groups']} east/west groups")

        # 5c. 2026-09-27: copy 24/7 marathon data+icons to m3u-* aliases
        a247 = fix_247_aliases(out_path)
        report['stages']['fix_247_aliases'] = a247

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
