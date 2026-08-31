#!/usr/bin/env python3
"""
Fetch the SDE change summary between two builds from the EVE developer API.
Walks the lastBuildNumber chain from <currentBuild> back to <lastBuild>,
aggregating changes across all intermediate builds.

Prints a Discord-ready string with literal \\n between lines (for JSON embedding).

Usage: sde_changes.py <currentBuild> [<lastBuild>]
Exits 0 on success, 1 if the changes file cannot be fetched.
"""
import sys
import json
import urllib.request

CHANGES_URL = "https://developers.eveonline.com/static-data/tranquility/changes/{}.jsonl"
MAX_CHAIN = 20  # safety cap on how far back we walk


def fetch(build):
    url = CHANGES_URL.format(build)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jsonl-evesde/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode()
    except Exception:
        return None


def parse(text):
    """Return (meta_dict, list_of_change_records)."""
    meta, records = None, []
    for line in text.strip().splitlines():
        r = json.loads(line)
        if r.get("_key") == "_meta":
            meta = r
        else:
            records.append(r)
    return meta, records


def collect_chain(current_build, last_build):
    """
    Walk from current_build back toward last_build via lastBuildNumber links.
    Returns (list_of_builds_walked, aggregated_changes_dict).
    aggregated_changes_dict: key -> {schemaChanged, added_count, removed_count, reworked_count}
    """
    agg = {}
    builds = []
    build = str(current_build)
    stop = str(last_build) if last_build else None

    for _ in range(MAX_CHAIN):
        text = fetch(build)
        if text is None:
            break

        meta, records = parse(text)
        builds.append(build)

        for r in records:
            key = r["_key"]
            if key not in agg:
                agg[key] = {"schemaChanged": False, "added": 0, "removed": 0, "reworked": 0, "changed": 0}
            if r.get("schemaChanged"):
                agg[key]["schemaChanged"] = True
            agg[key]["added"]    += len(r.get("added")    or [])
            agg[key]["removed"]  += len(r.get("removed")  or [])
            agg[key]["reworked"] += len(r.get("reworked") or [])
            agg[key]["changed"]  += len(r.get("changed")  or [])

        if meta is None:
            break
        prev = str(meta.get("lastBuildNumber", ""))
        if not prev or prev == stop:
            break
        build = prev

    return builds, agg


def summarise(builds, agg, last_build):
    lines = []
    if last_build:
        if len(builds) > 1:
            lines.append(f"Changes since build {last_build} ({len(builds)} builds):")
        else:
            lines.append(f"Changes from build {last_build}:")
    else:
        lines.append(f"Build {builds[0]} changes:")

    for key, c in sorted(agg.items()):
        parts = []
        if c["schemaChanged"]:
            parts.append("schema changed")
        if c["added"]:
            parts.append(f"+{c['added']} added")
        if c["removed"]:
            parts.append(f"-{c['removed']} removed")
        if c["reworked"]:
            parts.append(f"~{c['reworked']} reworked")
        if c["changed"]:
            parts.append(f"~{c['changed']} changed")
        if not parts:
            parts.append("updated")
        lines.append(f"  {key}: {', '.join(parts)}")

    if not agg:
        lines.append("  (no data changes recorded)")

    return lines


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: sde_changes.py <currentBuild> [<lastBuild>]", file=sys.stderr)
        sys.exit(1)

    current = sys.argv[1]
    last    = sys.argv[2] if len(sys.argv) > 2 else ""

    builds, agg = collect_chain(current, last)
    if not builds:
        sys.exit(1)

    print("\\n".join(summarise(builds, agg, last)))
