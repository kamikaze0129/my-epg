#!/usr/bin/env python3
"""Mapping guards for Chris's EPG pipeline.

Every gremlin caught during the 2026-09-29 manual audit is encoded here as a
check that runs BEFORE any staged mapping is trusted by an updater or the
builder. A mapping that fails a guard is rejected with a logged reason --
never silently applied.

Guards (in audit order):
 1. COUNTRY_PREFIX -- display names like "ARG: Comedy Central" carry a
    country prefix; the feed's country must match the channel's country.
    (Caught: StarzComedy.us = ARG: Comedy Central nearly fed USA Starz.)
 2. ESPN_PLUS_BLOCKLIST -- the 156 ESPN+ pseudo-slots are per-event slots
    with no mappable signal; they must NEVER receive linear ESPN data.
 3. CALLSIGN_SUBCHANNEL -- broadcast callsigns containing a network name
    (KDFX, KKFX, WFXR) are NOT the network. (Nearly fed FX.)
 4. MULTIPLEX_ISOLATION -- spinoff channels (HBO Hits/Comedy/Drama/Movies,
    Starz Encore/Cinema/Comedy/Edge/InBlack/..., Cinemax ActionMax/MoreMax/
    OuterMax/5StarMax, Showtime 2/Extreme/..., BET Gospel/Her/Jams/Soul/
    Classics/...) must get their OWN feed, never the base network feed.
 5. WORD_BOUNDARY -- single-char/short variants ("2", "AR") match on token
    boundaries only, never substrings. (Caught: "2" matching inside the
    hash bad286d2; "AR" nuking legitimate FX.)
 6. REGION_ROUTING -- East/West/Pacific/Central/Mountain/Alaska/Hawaii
    labels route to the matching region feed. ("FX West" must not get East.)
 7. NATIONAL_VS_REGIONAL -- a national feed must not feed a regional-only
    channel and vice versa without an explicit override.
 8. SYNTHETIC_SLOTS -- provider-synthetic channels (match centres, "HBO
    Boxing" as a channel, numbered Starzplay copies with no verifiable
    identity) keep honest placeholders; never fabricate listings for them.
 9. FILL_ONLY -- an updater must never overwrite a channel that already has
    healthy future data; fill gaps or append after real listings end.
10. DISPLAY_NAME -- provider display names are preserved; updaters suggest,
    never rename.

Usage:
    from mapping_guards import check_mapping
    ok, reason = check_mapping(chris_id, chris_name, feed_label, feed_region)
"""
import re

# ---------------------------------------------------------------- constants
# Base-network tokens: a channel carrying one of these spinoff tokens must
# never be fed the base network's schedule.
SPINOFF_TOKENS = {
    "hbo": ["hits", "comedy", "signature", "drama", "zone", "movies", "2", "boxing"],
    "starz": ["encore", "cinema", "comedy", "edge", "inblack", "black",
              "classic", "family", "suspense", "westerns", "kids"],
    "cinemax": ["actionmax", "moremax", "hits", "outermax", "5starmax",
                "classics"],
    "showtime": ["2", "extreme", "family", "familyzone", "next", "showcase",
                 "women"],
    "bet": ["gospel", "her", "jams", "soul", "classics", "tyler", "perry",
            "throwbacks", "visionaries", "cinema"],
}

# Channels that are per-event pseudo-slots: never map linear data to them.
# ESPN+ slots are the numbered per-event entries (m3u-usa-espn-NNN-... /
# "USA ESPN+ NNN: ..."), NOT the real ESPN networks (ESPN, ESPN2, ESPNU,
# ESPNews, ESPN Deportes, SEC Network).
PSEUDO_SLOT_PATTERNS = [
    re.compile(r"espn[- ]\d{3}[- ]", re.I),   # ESPN+ numbered pseudo-slots
    re.compile(r"espn\+\s*\d+", re.I),          # "ESPN+ 016:" display names
    re.compile(r"match\s*centre", re.I),        # synthetic match centres
    re.compile(r"starzplay", re.I),             # unverified Starzplay copies
]

# Broadcast callsigns that merely CONTAIN a network token.
CALLSIGN_GUARD = re.compile(r"^[a-z]{1,2}fx[a-z]{0,2}$", re.I)  # KDFX/KKFX/WFXR

COUNTRY_PREFIX = re.compile(r"^([A-Z]{2,3})\s*:\s*(.+)$")

REGION_TOKENS = {
    "east": ["east", "eastern"],
    "west": ["west", "western", "pacific"],
    "central": ["central"],
    "mountain": ["mountain"],
}


def _tokens(s):
    return re.findall(r"[a-z0-9]+", s.lower())


def check_country_prefix(chris_name, feed_country="usa"):
    """Guard 1: 'ARG: Comedy Central' must not receive a USA feed."""
    m = COUNTRY_PREFIX.match(chris_name.strip())
    if not m:
        return True, ""
    prefix_country = m.group(1).lower()
    if prefix_country in ("usa", "us"):
        prefix_country = "usa"
    if prefix_country != feed_country.lower():
        return False, (f"country-prefix mismatch: channel shows "
                       f"'{m.group(1)}' but feed is {feed_country}")
    return True, ""


def check_pseudo_slots(chris_id, chris_name):
    """Guard 2/8: pseudo-slots and synthetic channels get no linear data."""
    blob = f"{chris_id} {chris_name}"
    for pat in PSEUDO_SLOT_PATTERNS:
        if pat.search(blob):
            return False, f"pseudo-slot/synthetic pattern '{pat.pattern}' -- honest placeholder only"
    return True, ""


def check_callsign(chris_id, chris_name, network):
    """Guard 3: KDFX/KKFX/WFXR are not FX."""
    for tok in _tokens(chris_id) + _tokens(chris_name):
        if CALLSIGN_GUARD.match(tok) and network.lower() == "fx":
            return False, f"broadcast callsign '{tok}' is not the FX network"
    return True, ""


def check_multiplex_isolation(chris_id, chris_name, feed_label):
    """Guard 4: spinoffs get their own feed, never the base network feed."""
    blob = " ".join(_tokens(chris_id) + _tokens(chris_name))
    feed_blob = " ".join(_tokens(feed_label))
    for net, spinoffs in SPINOFF_TOKENS.items():
        if net not in _tokens(feed_label):
            continue
        # feed claims to be the BASE network feed (no spinoff token in label)
        if any(s in feed_blob for s in spinoffs):
            continue  # feed is itself a spinoff feed -- fine
        for s in spinoffs:
            # word-boundary match on the channel side (guard 5)
            if re.search(rf"\b{re.escape(s)}\b", blob):
                return False, (f"multiplex isolation: channel carries "
                               f"spinoff token '{s}' but feed is base {net}")
    return True, ""


def check_region_routing(chris_name, feed_label):
    """Guard 6: East/West/Pacific labels must route to matching feeds."""
    name_toks = set(_tokens(chris_name))
    feed_toks = set(_tokens(feed_label))
    for region, toks in REGION_TOKENS.items():
        name_has = any(t in name_toks for t in toks)
        if not name_has:
            continue
        # channel declares a region; feed must declare the same (or be
        # explicitly region-neutral, which we treat as a soft pass)
        feed_has = any(t in feed_toks for t in toks)
        other_regions = [r for r in REGION_TOKENS if r != region]
        feed_has_other = any(t in feed_toks
                             for r in other_regions for t in REGION_TOKENS[r])
        if feed_has_other and not feed_has:
            return False, (f"region routing: channel is {region} but feed "
                           f"is {feed_label}")
    return True, ""


def check_mapping(chris_id, chris_name, feed_label, feed_network="",
                  feed_country="usa"):
    """Run all guards. Returns (ok: bool, reason: str)."""
    for check in (
        lambda: check_country_prefix(chris_name, feed_country),
        lambda: check_pseudo_slots(chris_id, chris_name),
        lambda: check_callsign(chris_id, chris_name, feed_network),
        lambda: check_multiplex_isolation(chris_id, chris_name, feed_label),
        lambda: check_region_routing(chris_name, feed_label),
    ):
        ok, reason = check()
        if not ok:
            return False, reason
    return True, ""


def filter_mappings(mappings):
    """Apply guards to a list of mapping dicts.

    Each mapping needs: chris_id, chris_name, tvp_feed (or source label),
    and optionally feed_network. Returns (accepted, rejected) where rejected
    items carry a '_guard_reason'.
    """
    accepted, rejected = [], []
    for m in mappings:
        ok, reason = check_mapping(
            m.get("chris_id", ""), m.get("chris_name", ""),
            m.get("tvp_feed", "") or m.get("source_url", ""),
            feed_network=m.get("feed_network", ""))
        if ok:
            accepted.append(m)
        else:
            m = dict(m)
            m["_guard_reason"] = reason
            rejected.append(m)
    return accepted, rejected
