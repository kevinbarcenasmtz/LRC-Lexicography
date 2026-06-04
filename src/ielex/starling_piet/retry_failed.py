"""
PIET Failed URL Retry & Patch Script
=====================================
Companion to scrape_piet.py. Retries URLs from failed_urls.csv and patches
the results back into piet_complete.json by replacing error stubs in the
record tree.

Two failure modes handled:
  1. Sub-entry failures: error stubs with {"_error": "fetch_failed", "_url": ...}
     exist in the JSON tree. This script re-fetches them, parses the content,
     and replaces the stub in-place.
  2. Main page failures: the checkpoint system in scrape_piet.py already handles
     this -- just re-run the main scraper to resume from where it stopped.

Usage:
    python retry_failed.py                              # default paths
    python retry_failed.py --data-dir data/indo-european
    python retry_failed.py --dry-run                    # show what would be retried
    python retry_failed.py --max-retries 5              # more attempts per URL
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://starlingdb.org"
REQUEST_DELAY = 2.0
DEFAULT_MAX_RETRIES = 5
BACKOFF_BASE = 3

FOLLOW_BASENAMES = {"baltet", "germet", "vasmer", "pokorny"}
CAPTURE_ONLY_BASENAMES = {"nostret"}
MAIN_BASENAMES = {"piet"}
MAX_DEPTH = 5


def basename_from_url(url: str) -> Optional[str]:
    m = re.search(r"basename=%2[fF]([^&]+)", url)
    if m:
        decoded = m.group(1).replace("%2f", "/").replace("%2F", "/")
        parts = [p for p in decoded.split("/") if p]
        return parts[-1] if parts else None
    m2 = re.search(r"basename=/([^&]+)", url)
    if m2:
        parts = [p for p in m2.group(1).split("/") if p]
        return parts[-1] if parts else None
    return None


def should_follow(url: str, visited: set[str]) -> str:
    if url in visited:
        return "skip"
    basename = basename_from_url(url)
    if not basename:
        return "skip"
    bl = basename.lower()
    if bl in MAIN_BASENAMES:
        return "skip"
    if bl in FOLLOW_BASENAMES:
        return "follow"
    if bl in CAPTURE_ONLY_BASENAMES:
        return "capture"
    return "skip"


def content_hash(data: dict) -> Optional[str]:
    filtered = {k: v for k, v in data.items() if not k.startswith("_")}
    if not filtered:
        return None
    raw = json.dumps(filtered, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def extract_sub_url(div: Tag) -> Optional[str]:
    subquery = div.find("div", class_="subquery_link")
    if not isinstance(subquery, Tag):
        return None
    img = subquery.find("img", attrs={"onclick": True})
    if not isinstance(img, Tag):
        return None
    onclick = img.get("onclick", "")
    m = re.search(r"'([^']+)'", str(onclick))
    if not m:
        return None
    return urljoin(BASE_URL + "/cgi-bin/", m.group(1))


def fetch_with_retries(
    session: requests.Session, url: str, max_retries: int
) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            wait = (BACKOFF_BASE ** attempt) * 2
            print(f"  Attempt {attempt + 1}/{max_retries}: {str(e)[:80]}, waiting {wait}s")
            time.sleep(wait)
    return None


def parse_sub_entry(
    session: requests.Session,
    url: str,
    depth: int,
    recurse: bool,
    visited: set[str],
    seen_content: set[str],
    max_retries: int,
    stats: dict,
) -> dict:
    """Parse a sub-entry page, optionally recursing into children."""
    if depth > MAX_DEPTH:
        return {"_depth_capped": True, "_url": url, "_depth": depth}

    if url in visited:
        return {"_circular_reference": True, "_url": url, "_depth": depth}

    basename = basename_from_url(url) or "unknown"
    visited.add(url)

    resp = fetch_with_retries(session, url, max_retries)
    if not resp:
        stats["still_failed"] += 1
        return {"_error": "fetch_failed", "_url": url, "_depth": depth}

    soup = BeautifulSoup(resp.text, "html.parser")
    record = soup.find("div", class_="results_record")
    if not isinstance(record, Tag):
        stats["still_failed"] += 1
        return {"_error": "no_record", "_url": url, "_depth": depth}

    entry = {"_url": url, "_depth": depth, "_basename": basename}

    for div in record.find_all("div", recursive=False):
        if not isinstance(div, Tag):
            continue
        fld = div.find("span", class_="fld")
        if not isinstance(fld, Tag):
            continue
        name = fld.get_text(strip=True).rstrip(":")
        val_span = div.find("span", class_="unicode")
        link = div.find("a")
        if isinstance(val_span, Tag):
            value = val_span.get_text(strip=True)
        elif isinstance(link, Tag):
            value = link.get_text(strip=True)
        else:
            value = div.get_text(strip=True).replace(fld.get_text(), "", 1).strip()
        entry[name] = value

    ch = content_hash(entry)
    if ch:
        if ch in seen_content:
            return {"_circular_reference": True, "_url": url, "_depth": depth, "_content_hash": ch}
        seen_content.add(ch)
        entry["_content_hash"] = ch

    stats["fetched"] += 1

    if not recurse:
        return entry

    for div in record.find_all("div", recursive=False):
        if not isinstance(div, Tag):
            continue
        fld = div.find("span", class_="fld")
        if not isinstance(fld, Tag):
            continue
        sub_url = extract_sub_url(div)
        if not sub_url:
            continue
        action = should_follow(sub_url, visited)
        if action == "skip":
            continue
        child_recurse = action == "follow"
        sub_data = parse_sub_entry(
            session, sub_url, depth + 1, child_recurse,
            visited, seen_content, max_retries, stats,
        )
        if not sub_data.get("_circular_reference") and not sub_data.get("_depth_capped"):
            if "_sub_entries" not in entry:
                entry["_sub_entries"] = []
            entry["_sub_entries"].append(sub_data)
        time.sleep(REQUEST_DELAY)

    return entry


def find_error_stubs(node: dict, path: str = "root") -> list[tuple[str, dict, str, int]]:
    """Walk the JSON tree and collect all error stubs with their parent context.
    Returns list of (url, node_ref, path, index_in_parent_sub_entries).
    """
    results = []

    sub_entries = node.get("_sub_entries", [])
    for i, child in enumerate(sub_entries):
        if not isinstance(child, dict):
            continue
        if child.get("_error") and child.get("_url"):
            results.append((child["_url"], node, f"{path}._sub_entries[{i}]", i))
        results.extend(find_error_stubs(child, f"{path}._sub_entries[{i}]"))

    return results


def collect_visited_and_hashes(records: list[dict]) -> tuple[set[str], set[str]]:
    """Rebuild visited_urls and seen_content sets from existing records."""
    visited = set()
    seen = set()

    def walk(node):
        url = node.get("_url")
        if url:
            visited.add(url)
        ch = node.get("_content_hash")
        if ch:
            seen.add(ch)
        for child in node.get("_sub_entries", []):
            if isinstance(child, dict):
                walk(child)

    for rec in records:
        walk(rec)
    return visited, seen


def main():
    parser = argparse.ArgumentParser(description="Retry failed PIET URLs and patch results.")
    parser.add_argument(
        "--data-dir", default=os.path.join("data", "indo-european"),
        help="Directory containing piet_complete.json and failed_urls.csv",
    )
    parser.add_argument("--dry-run", action="store_true", help="List stubs without retrying")
    parser.add_argument(
        "--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
        help=f"Max retry attempts per URL (default: {DEFAULT_MAX_RETRIES})",
    )
    args = parser.parse_args()

    json_path = os.path.join(args.data_dir, "piet_complete.json")
    failed_csv_path = os.path.join(args.data_dir, "failed_urls.csv")
    backup_path = os.path.join(args.data_dir, f"piet_complete.backup_{datetime.now():%Y%m%d_%H%M%S}.json")
    retry_report_path = os.path.join(args.data_dir, "retry_report.json")

    if not os.path.exists(json_path):
        print(f"No results file found at {json_path}")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Collect all error stubs across the entire tree
    all_stubs = []
    for rec_idx, rec in enumerate(records):
        stubs = find_error_stubs(rec, f"records[{rec_idx}]")
        all_stubs.extend(stubs)

    # Also read failed_urls.csv for reference
    csv_urls = set()
    if os.path.exists(failed_csv_path):
        with open(failed_csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                csv_urls.add(row.get("url", ""))

    stub_urls = {s[0] for s in all_stubs}
    print(f"Error stubs found in JSON tree: {len(all_stubs)}")
    print(f"Failed URLs in CSV: {len(csv_urls)}")
    print(f"Overlap: {len(stub_urls & csv_urls)}")
    print(f"Stubs not in CSV (inline errors): {len(stub_urls - csv_urls)}")

    if not all_stubs:
        print("No error stubs to retry.")
        sys.exit(0)

    for url, parent, path, idx in all_stubs:
        bn = basename_from_url(url) or "?"
        print(f"  {path}: [{bn}] {url}")

    if args.dry_run:
        print("\nDry run -- no changes made.")
        sys.exit(0)

    # Back up before modifying
    print(f"\nBacking up to {backup_path}")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Rebuild state from existing data
    visited, seen_content = collect_visited_and_hashes(records)
    # Remove failed URLs from visited so they can be retried
    for url, _, _, _ in all_stubs:
        visited.discard(url)

    session = requests.Session()
    session.headers.update(
        {"User-Agent": "LRC-Lexicography-Research/1.0 (academic linguistic research)"}
    )

    stats = {"fetched": 0, "still_failed": 0, "patched": 0}

    for url, parent, path, idx in all_stubs:
        basename = basename_from_url(url) or "unknown"
        bl = basename.lower()
        recurse = bl in FOLLOW_BASENAMES
        depth = parent["_sub_entries"][idx].get("_depth", 1)

        print(f"\nRetrying [{basename}] depth={depth}: {url}")
        result = parse_sub_entry(
            session, url, depth, recurse,
            visited, seen_content, args.max_retries, stats,
        )

        if result.get("_error"):
            print(f"  Still failed.")
        else:
            field_count = len({k: v for k, v in result.items() if not k.startswith("_")})
            child_count = len(result.get("_sub_entries", []))
            print(f"  Patched: {field_count} fields, {child_count} children")
            parent["_sub_entries"][idx] = result
            stats["patched"] += 1

        time.sleep(REQUEST_DELAY)

    # Save patched results
    print(f"\nSaving patched results to {json_path}")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Update failed_urls.csv with only the remaining failures
    remaining_stubs = []
    for rec in records:
        remaining_stubs.extend(find_error_stubs(rec))

    if remaining_stubs:
        with open(failed_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "url", "error", "basename", "attempts"])
            writer.writeheader()
            for url, _, path, _ in remaining_stubs:
                writer.writerow({
                    "timestamp": datetime.now().isoformat(),
                    "url": url,
                    "error": "still_failed_after_retry",
                    "basename": basename_from_url(url) or "",
                    "attempts": str(args.max_retries),
                })
    elif os.path.exists(failed_csv_path):
        os.remove(failed_csv_path)

    # Save retry report
    report = {
        "timestamp": datetime.now().isoformat(),
        "stubs_found": len(all_stubs),
        "patched": stats["patched"],
        "still_failed": stats["still_failed"],
        "sub_entries_fetched_during_retry": stats["fetched"],
        "remaining_stubs": len(remaining_stubs),
    }
    with open(retry_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"\n{'='*60}")
    print(f"RETRY COMPLETE")
    print(f"{'='*60}")
    print(f"Stubs found: {len(all_stubs)}")
    print(f"Patched: {stats['patched']}")
    print(f"Still failed: {stats['still_failed']}")
    print(f"Remaining stubs: {len(remaining_stubs)}")
    print(f"Backup: {backup_path}")
    print(f"Report: {retry_report_path}")


if __name__ == "__main__":
    main()