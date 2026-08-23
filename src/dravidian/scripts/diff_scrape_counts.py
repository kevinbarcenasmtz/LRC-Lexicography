"""Old-vs-new diff for the markup-preserving StarlingDB re-scrape (roadmap item 3).

Compares the original flat scrape (starling_complete_data.json) against the
markup-preserving re-scrape (starling_complete_data_markup.json) BEFORE the new
file is adopted downstream. Checks that:

  * top-level and flattened entry counts line up,
  * per-field coverage hasn't silently dropped,
  * the flattened text values are still byte-identical (the whole point of the
    patch was to ADD _field_html without changing text), and
  * _field_html actually got captured on the markup-bearing fields.

Usage:
    python diff_scrape_counts.py [OLD_JSON] [NEW_JSON]
Defaults resolve relative to the repo's data/dravidian/starling/ dir.
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
STARLING = os.path.normpath(os.path.join(HERE, "..", "..", "..", "data", "dravidian", "starling"))
DEFAULT_OLD = os.path.join(STARLING, "starling_complete_data.json")
DEFAULT_NEW = os.path.join(STARLING, "starling_complete_data_markup.json")


def load(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def walk(record):
    """Yield a record and all of its nested _sub_entries."""
    yield record
    for sub in record.get("_sub_entries", []):
        yield from walk(sub)


def flatten(records):
    out = []
    for r in records:
        out.extend(walk(r))
    return out


def field_counts(entries):
    c = Counter()
    for e in entries:
        for k, v in e.items():
            if not k.startswith("_") and isinstance(v, str) and v.strip():
                c[k] += 1
    return c


def index_by_url(entries):
    idx = {}
    for e in entries:
        u = e.get("_url")
        if u:
            idx.setdefault(u, e)
    return idx


def main():
    old_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OLD
    new_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_NEW

    old, new = load(old_path), load(new_path)
    old_top, new_top = old["records"], new["records"]
    old_flat, new_flat = flatten(old_top), flatten(new_top)

    print("=" * 68)
    print("STARLING RE-SCRAPE DIFF")
    print("=" * 68)
    print(f"  old: {old_path}")
    print(f"  new: {new_path}\n")

    print(f"top-level records   old={len(old_top):>6}  new={len(new_top):>6}  "
          f"delta={len(new_top) - len(old_top):+d}")
    print(f"flattened entries   old={len(old_flat):>6}  new={len(new_flat):>6}  "
          f"delta={len(new_flat) - len(old_flat):+d}")
    print(f"metadata.unique     old={old['metadata'].get('unique_entries'):>6}  "
          f"new={new['metadata'].get('unique_entries'):>6}")

    # ---- _field_html coverage (new only) ----
    fh_entries = sum(1 for e in new_flat if e.get("_field_html"))
    fh_fields = Counter()
    for e in new_flat:
        for k in (e.get("_field_html") or {}):
            fh_fields[k] += 1
    print(f"\nentries with _field_html: {fh_entries} "
          f"({fh_entries * 100 // max(len(new_flat), 1)}% of flattened)")
    print("top _field_html fields:")
    for k, n in fh_fields.most_common(12):
        print(f"    {n:>6}  {k}")

    # ---- per-field coverage diff ----
    oc, nc = field_counts(old_flat), field_counts(new_flat)
    allf = sorted(set(oc) | set(nc), key=lambda k: -max(oc.get(k, 0), nc.get(k, 0)))
    print("\nper-field coverage (fields whose count changed):")
    changed = 0
    for k in allf:
        if oc.get(k, 0) != nc.get(k, 0):
            changed += 1
            print(f"    {oc.get(k,0):>6} -> {nc.get(k,0):>6}   {k}")
    if not changed:
        print("    (none — identical field coverage)")

    # ---- text-identity check on shared URLs ----
    oidx, nidx = index_by_url(old_flat), index_by_url(new_flat)
    shared = set(oidx) & set(nidx)
    only_old = set(oidx) - set(nidx)
    only_new = set(nidx) - set(oidx)
    mismatches = []
    for u in shared:
        oe, ne = oidx[u], nidx[u]
        for k, v in oe.items():
            if k.startswith("_") or not isinstance(v, str):
                continue
            if ne.get(k) != v:
                mismatches.append((u, k))
    print(f"\nURL overlap: shared={len(shared)}  only_old={len(only_old)}  only_new={len(only_new)}")
    print(f"text mismatches on shared URLs (non-_ fields): {len(mismatches)}")
    for u, k in mismatches[:10]:
        print(f"    {k}  @ {u[-55:]}")

    print("\n" + "=" * 68)
    verdict_ok = (len(new_top) == len(old_top)) and not mismatches and fh_entries > 0
    print("VERDICT:", "CLEAN — safe to adopt" if verdict_ok else "REVIEW NEEDED (see above)")
    print("=" * 68)


if __name__ == "__main__":
    main()
