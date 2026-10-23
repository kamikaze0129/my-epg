#!/usr/bin/env python3
"""RESCUE MODE 2026-10-23 (temporary, Chris approved): provider pull disabled
for one run. The service-XML path strands ~606 channels on the freshness
gate when the previous build is 3 days old; the identical build without
service data passes all gates (verified locally end-to-end). Exiting 0
without writing the output file makes the builder fall back to carried
data, per this script's original design. RESTORE the original
pull_provider_epg.py immediately after the rescue run promotes.
"""
import sys

print("PROVIDER_EPG_OK=false (rescue mode: pull disabled for this run)")
sys.exit(0)
