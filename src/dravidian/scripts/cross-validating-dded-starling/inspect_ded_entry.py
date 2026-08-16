"""
Ground-truth inspector for one Burrow DED entry.

Why this exists
----------------
Triaging a mismatch row requires knowing what Burrow actually published for
that DED number, not just what the parser extracted into the corpus. This
script looks a DED number up locally first (cached `raw_html` / `full_text`
in the corpus JSON, no network call), and only falls back to a live DSAL
fetch when the entry isn't cached or `--live` is explicitly passed.

It prints three things side by side:
  [1] Ground truth: the cached `full_text` as Burrow published it.
  [2] What's currently stored in the corpus's `attestations` list.
  [3] (with --reparse) What the *current* `BurrowEntryParser` would extract
      from the same cached `raw_html` right now -- lets you test a parser
      fix locally before committing to a full `reparse_burrow_corpus.py`
      run over the whole corpus.

A simple heuristic flags known Burrow abbreviations (from
`dialect_mapping.LANGUAGE_INVENTORY`) that appear in `full_text` but are
missing from the attestation set being shown -- a likely parser miss.

Usage
-----
    python inspect_ded_entry.py 412 --reparse
    python inspect_ded_entry.py 9999 --live
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import asdict
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from burrow_entry_parser import BurrowEntryParser
from dialect_mapping import LANGUAGE_INVENTORY

DEFAULT_CORPUS_PATH = "data/dravidian/burrow_ded/burrow_corpus.cleaned.json"

_DSAL_BASE_URL = "https://dsal.uchicago.edu/cgi-bin/app/burrow_query.py"
_DSAL_HOST = "https://dsal.uchicago.edu"

_entry_parser = BurrowEntryParser()

# Cache of {corpus_path: {clean_ded_number: entry_dict}}, built once per
# corpus file so repeated `inspect()` calls in one session (e.g. from
# Claude during a triage pass) don't re-parse a multi-MB JSON file each time.
_CORPUS_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {}


# --------------------------------------------------------------------------- #
# Local lookup (no network)
# --------------------------------------------------------------------------- #


def _load_corpus_index(corpus_path: str) -> Dict[str, Dict[str, Any]]:
    cached = _CORPUS_CACHE.get(corpus_path)
    if cached is not None:
        return cached

    with open(corpus_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    index: Dict[str, Dict[str, Any]] = {}
    for entry in data.get("entries", []):
        if entry.get("edition", "DEDR") == "Appendix":
            continue
        raw = entry.get("ded_number_raw") or entry.get("ded_number")
        if raw is None:
            continue
        key = _entry_parser.clean_ded_number(str(raw))
        index[key] = entry

    _CORPUS_CACHE[corpus_path] = index
    return index


def load_corpus_entry(corpus_path: str, ded_number: Any) -> Optional[Dict[str, Any]]:
    """Look up one DEDR entry from a cached corpus file. No network call."""
    index = _load_corpus_index(corpus_path)
    key = _entry_parser.clean_ded_number(str(ded_number))
    return index.get(key)


# --------------------------------------------------------------------------- #
# Ground-truth rendering
# --------------------------------------------------------------------------- #

_WORDCHAR = r"[A-Za-zÀ-ÖØ-öø-ÿĀ-žḀ-ỹ]"


def _find_missing_abbrevs(full_text: str, present_abbrevs: set) -> List[str]:
    """Known Burrow abbreviations that appear in full_text but aren't in present_abbrevs."""
    if not full_text:
        return []
    missing = []
    for abbrev in LANGUAGE_INVENTORY:
        if abbrev in present_abbrevs:
            continue
        pattern = re.compile(rf"(?<!{_WORDCHAR}){re.escape(abbrev)}(?!{_WORDCHAR})")
        if pattern.search(full_text):
            missing.append(abbrev)
    return sorted(missing)


def _format_attestation_lines(attestations: List[Dict[str, Any]]) -> List[str]:
    if not attestations:
        return ["  (none)"]
    lines = []
    for a in attestations:
        headwords = ", ".join(a.get("headwords", []) or [])
        lines.append(
            f"  {a.get('language_abbrev', ''):<10} ({a.get('language_name', '')}): "
            f"{headwords}  -- {a.get('gloss', '')}"
        )
    return lines


def render_ground_truth(entry: Dict[str, Any], reparse: bool = False) -> str:
    """Render the cached ground truth, stored attestations, and optionally a reparse preview."""
    lines: List[str] = []
    ded_display = entry.get("ded_number", "?")
    lines.append("=" * 70)
    lines.append(f"DED {ded_display}  (page {entry.get('page', '?')}, edition={entry.get('edition', '?')})")
    lines.append("=" * 70)

    full_text = entry.get("full_text", "") or ""
    lines.append("\n[1] Ground truth -- full_text as Burrow published it:")
    lines.append("-" * 70)
    lines.append(full_text or "(no full_text cached)")

    stored_atts: List[Dict[str, Any]] = entry.get("attestations", []) or []
    stored_abbrevs = {a.get("language_abbrev", "") for a in stored_atts if a.get("language_abbrev")}
    lines.append("\n[2] Currently stored attestations (what's in the corpus right now):")
    lines.append("-" * 70)
    lines.extend(_format_attestation_lines(stored_atts))

    compare_abbrevs = stored_abbrevs
    if reparse:
        ded_number_raw = entry.get("ded_number_raw") or entry.get("ded_number")
        reparsed = _entry_parser.parse_language_sections(
            entry.get("raw_html", ""), str(ded_number_raw) if ded_number_raw is not None else None
        )
        reparsed_dicts = [asdict(att) for att in reparsed]
        reparsed_abbrevs = {att.language_abbrev for att in reparsed}

        lines.append("\n[3] Reparse preview (current BurrowEntryParser against cached raw_html, right now):")
        lines.append("-" * 70)
        lines.extend(_format_attestation_lines(reparsed_dicts))

        gained = sorted(reparsed_abbrevs - stored_abbrevs)
        lost = sorted(stored_abbrevs - reparsed_abbrevs)
        if gained or lost:
            lines.append("")
            if gained:
                lines.append(f"  + gained vs. currently stored: {', '.join(gained)}")
            if lost:
                lines.append(f"  - lost vs. currently stored:   {', '.join(lost)}")
        else:
            lines.append("\n  (reparse matches what's currently stored -- no change)")

        compare_abbrevs = reparsed_abbrevs

    missing = _find_missing_abbrevs(full_text, compare_abbrevs)
    if missing:
        scope = "reparsed" if reparse else "stored"
        lines.append(
            f"\n[!] Heuristic: abbreviation(s) found in full_text but missing from "
            f"{scope} attestations -- possible parser miss:"
        )
        lines.append(f"    {', '.join(missing)}")

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Live DSAL fallback
# --------------------------------------------------------------------------- #


def _isolate_entry_html(page_html: str, clean_ded: str) -> Optional[str]:
    """Isolate the HTML for one entry within a page-query response.

    A "page=" DSAL response bundles multiple DED entries under one
    <div class="hw_result">, each wrapped in its own nested <div> carrying a
    <number> tag (mirrors the page-query branch of
    `BurrowEntryParser.parse_language_sections` /
    `BurrowCorpusScraper._extract_entries_from_result_div`). Without this,
    `full_text`/`raw_html` would cover the whole page -- every neighboring
    entry too -- which makes the missing-abbreviation heuristic in
    `render_ground_truth` misfire on a neighbor's language markers instead
    of this entry's own.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    result_div = soup.find("div", class_="hw_result")
    if not isinstance(result_div, Tag):
        return None

    nested_divs = [d for d in result_div.find_all("div", recursive=False) if isinstance(d, Tag)]
    if nested_divs:
        for div in nested_divs:
            number_tag = div.find("number")
            if isinstance(number_tag, Tag) and number_tag.get_text(strip=True) == clean_ded:
                return f"<div class='hw_result'>{str(div)}</div>"
        return None

    blockquote = result_div.find("blockquote")
    if isinstance(blockquote, Tag):
        return f"<div class='hw_result'>{str(blockquote)}</div>"
    return None


def fetch_live_entry(ded_number: Any, delay: float = 0.5) -> Optional[Dict[str, Any]]:
    """Fetch one DED entry live from DSAL: search -> extract page URL -> fetch page.

    Thin orchestration only, using DSAL's two-step query pattern (search ->
    page fetch) and the 0.5s politeness delay convention
    from `burrow_corpus_scraper.py`. Not a new scraper class -- `BurrowCorpusScraper`
    is tied to checkpoint/corpus file management this doesn't need.
    """
    clean_ded = _entry_parser.clean_ded_number(str(ded_number))

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    try:
        search_response = session.get(_DSAL_BASE_URL, params={"qs": clean_ded}, timeout=30)
        search_response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Live fetch failed (search step): {exc}", file=sys.stderr)
        return None

    page_url = _entry_parser.extract_page_url(search_response.text, clean_ded)
    if not page_url:
        print(f"Live fetch: no page URL found for DED {clean_ded}", file=sys.stderr)
        return None

    time.sleep(delay)

    full_url = f"{_DSAL_HOST}{page_url}"
    try:
        page_response = session.get(full_url, timeout=30)
        page_response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Live fetch failed (page step): {exc}", file=sys.stderr)
        return None

    entry_html = _isolate_entry_html(page_response.text, clean_ded)
    if entry_html is None:
        print(f"Live fetch: could not isolate entry HTML for DED {clean_ded} on its page", file=sys.stderr)
        return None

    attestations = _entry_parser.parse_language_sections(entry_html, clean_ded)
    if not attestations:
        return None

    full_text = BeautifulSoup(entry_html, "html.parser").get_text(" ", strip=True)

    page_num: Optional[int] = None
    parsed_qs = parse_qs(urlparse(page_url).query)
    if "page" in parsed_qs:
        try:
            page_num = int(parsed_qs["page"][0])
        except (ValueError, IndexError):
            page_num = None

    return {
        "page": page_num,
        "ded_number": clean_ded,
        "ded_number_raw": clean_ded,
        "edition": "DEDR",
        "raw_html": entry_html,
        "full_text": full_text,
        "attestations": [asdict(att) for att in attestations],
    }


# --------------------------------------------------------------------------- #
# Top-level entry point
# --------------------------------------------------------------------------- #


def inspect(
    ded_number: Any,
    corpus_path: str = DEFAULT_CORPUS_PATH,
    live: bool = False,
    reparse: bool = False,
    delay: float = 0.5,
) -> str:
    """Look up a DED number (cache-first unless live=True) and render its ground truth."""
    entry: Optional[Dict[str, Any]] = None
    source = ""

    if not live:
        entry = load_corpus_entry(corpus_path, ded_number)
        if entry is not None:
            source = f"cached corpus ({corpus_path})"

    if entry is None:
        entry = fetch_live_entry(ded_number, delay=delay)
        if entry is not None:
            source = "live DSAL fetch"

    if entry is None:
        return f"DED {ded_number}: not found in cached corpus and live fetch failed or returned no results."

    return f"[source: {source}]\n" + render_ground_truth(entry, reparse=reparse)


def main() -> None:
    # Burrow abbreviations / headwords carry diacritics; force UTF-8 stdout
    # so this doesn't crash under the default Windows console codepage.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Inspect ground truth for one Burrow DED entry: cached vs. (optionally) reparsed vs. (optionally) live."
    )
    parser.add_argument("ded_number", help="DED number, e.g. 412")
    parser.add_argument("--corpus", default=DEFAULT_CORPUS_PATH, help="Path to the cached corpus JSON")
    parser.add_argument("--live", action="store_true", help="Force a live DSAL fetch instead of the cached corpus")
    parser.add_argument(
        "--reparse",
        action="store_true",
        help="Show what the current BurrowEntryParser would extract from cached raw_html right now",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Politeness delay before the live page fetch")
    args = parser.parse_args()

    print(inspect(args.ded_number, corpus_path=args.corpus, live=args.live, reparse=args.reparse, delay=args.delay))


if __name__ == "__main__":
    main()
