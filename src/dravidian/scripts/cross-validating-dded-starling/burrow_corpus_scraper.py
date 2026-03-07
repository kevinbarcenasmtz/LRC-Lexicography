"""
Burrow DED corpus scraper.

Scrapes Burrow & Emeneau DED pages 1-514 into a local JSON corpus of entries
and language attestations for offline cross-validation.

Edition handling:
  Pages 1-508  -> DEDR (main dictionary, entries 1-5557)
  Pages 509-514 -> Appendix (IA/non-Dravidian items, entries App.1-61)
  Pages 515+   -> Indexes (not scraped as entries)

The Appendix restarts numbering from 1, so entries are prefixed "App." to
prevent collisions with DEDR entry numbers. Starling's "Number in DED"
refers exclusively to DEDR numbers.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import re
import time

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from burrow_entry_parser import BurrowEntryParser, LanguageAttestation

# Page boundary between DEDR main dictionary and Appendix.
# Derived from the CONTENTS in the frontmatter: "Appendix (1-61) 509"
APPENDIX_START_PAGE = 509


def classify_edition(page: int) -> str:
    """Return 'DEDR' or 'Appendix' based on the page number."""
    return "Appendix" if page >= APPENDIX_START_PAGE else "DEDR"


def detect_edition_from_text(full_text: str) -> Optional[str]:
    """
    Heuristic fallback: detect edition from cross-reference markers in the
    entry text itself.

    Appendix entries point forward to DEDR, e.g. "[DEDR 4054]" or "DEDR 4054".
    DEDR entries point backward to older editions, e.g. "DED(S) 2913" or "DEDS 8".
    """
    # Appendix entries contain forward refs like [DEDR NNNN]
    if re.search(r"\[DEDR\s+\d+\]", full_text):
        return "Appendix"
    # DEDR entries end with backward refs like "DED(S, N) 56" or "DEDS 8"
    if re.search(r"DED(\(S(?:,\s*N)?\)|S)\s+\d+", full_text):
        return "DEDR"
    return None


class BurrowCorpusScraper:
    """
    Scrape Burrow DED pages into a structured local corpus.

    For each page=1..N, this scraper:
    - Fetches the page listing.
    - For each entry-like block, extracts the per-entry HTML.
    - Uses BurrowEntryParser to extract language attestations.
    - Tags each entry with its edition (DEDR or Appendix).
    - Stores entries and attestations into a JSON corpus file.
    - Maintains a checkpoint so long runs can be resumed.
    """

    def __init__(
        self,
        start_page: int = 1,
        end_page: int = 514,
        output_dir: str = "validation_output/burrow_cache",
        corpus_filename: str = "burrow_corpus.json",
        checkpoint_filename: str = "burrow_corpus_checkpoint.json",
        max_retries: int = 3,
        timeout: int = 45,
    ) -> None:
        self.start_page = start_page
        self.end_page = end_page

        self.base_url = "https://dsal.uchicago.edu/cgi-bin/app/burrow_query.py"

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.corpus_path = self.output_dir / corpus_filename
        self.checkpoint_path = self.output_dir / checkpoint_filename

        self.max_retries = max_retries
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

        self.parser = BurrowEntryParser()

        self.entries: List[Dict[str, Any]] = []
        self.completed_pages: set[int] = set()

        self._load_existing_corpus()
        self._load_checkpoint()

    # --------------------------------------------------------------------- #
    # Persistence helpers
    # --------------------------------------------------------------------- #

    def _load_existing_corpus(self) -> None:
        if not self.corpus_path.exists():
            return

        try:
            with open(self.corpus_path, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            entries = data.get("entries", [])
            if isinstance(entries, list):
                self.entries = entries
                print(f"Loaded existing corpus with {len(self.entries)} entries.")
        except Exception as exc:
            print(f"Could not load existing corpus: {exc}")

    def _save_corpus(self) -> None:
        payload = {
            "metadata": {
                "start_page": self.start_page,
                "end_page": self.end_page,
                "completed_pages": sorted(self.completed_pages),
                "total_entries": len(self.entries),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            },
            "entries": self.entries,
        }
        with open(self.corpus_path, "w", encoding="utf-8-sig") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Corpus saved to: {self.corpus_path}")

    def _load_checkpoint(self) -> None:
        if not self.checkpoint_path.exists():
            return

        try:
            with open(self.checkpoint_path, "r", encoding="utf-8-sig") as f:
                checkpoint = json.load(f)

            completed = checkpoint.get("completed_pages", [])
            if isinstance(completed, list):
                self.completed_pages = {int(p) for p in completed}
            print(
                f"Loaded checkpoint: {len(self.completed_pages)} pages completed, "
                f"{checkpoint.get('total_entries', 0)} entries."
            )
        except Exception as exc:
            print(f"Could not load checkpoint: {exc}")

    def _save_checkpoint(self) -> None:
        checkpoint = {
            "start_page": self.start_page,
            "end_page": self.end_page,
            "completed_pages": sorted(self.completed_pages),
            "total_entries": len(self.entries),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        with open(self.checkpoint_path, "w", encoding="utf-8-sig") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        print(f"Checkpoint saved to: {self.checkpoint_path}")

    # --------------------------------------------------------------------- #
    # HTTP helpers
    # --------------------------------------------------------------------- #

    def fetch_with_retry(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[requests.Response]:
        """Fetch URL with exponential-backoff retry."""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                wait_time = (2**attempt) * 2
                print(
                    f"    Timeout (attempt {attempt + 1}/{self.max_retries}), "
                    f"waiting {wait_time}s..."
                )
                time.sleep(wait_time)
            except requests.exceptions.RequestException as exc:
                wait_time = (2**attempt) * 2
                print(
                    f"    Error (attempt {attempt + 1}/{self.max_retries}): "
                    f"{str(exc)[:80]}"
                )
                if attempt < self.max_retries - 1:
                    print(f"    Waiting {wait_time}s...")
                    time.sleep(wait_time)

        print(f"    Failed to fetch {url} after {self.max_retries} attempts.")
        return None

    # --------------------------------------------------------------------- #
    # Scraping logic
    # --------------------------------------------------------------------- #

    def _extract_entries_from_result_div(
        self, result_div: Tag, page: int
    ) -> List[Dict[str, Any]]:
        """
        Extract per-entry HTML chunks and language attestations from a single
        <div class="hw_result"> block on a page.
        """
        entries: List[Dict[str, Any]] = []
        edition_from_page = classify_edition(page)

        nested_divs = result_div.find_all("div", recursive=False)

        if nested_divs:
            candidate_divs: List[Tag] = [d for d in nested_divs if isinstance(d, Tag)]
            for div in candidate_divs:
                number_tag = div.find("number")
                ded_number: Optional[str]
                if isinstance(number_tag, Tag):
                    ded_number = number_tag.get_text(strip=True)
                else:
                    ded_number = None

                entry_html = f"<div class='hw_result'>{str(div)}</div>"

                attestations = self.parser.parse_language_sections(
                    entry_html, ded_number
                )
                if not attestations:
                    continue

                full_text = BeautifulSoup(entry_html, "html.parser").get_text(
                    " ", strip=True
                )

                # Determine edition: page-based primary, text-based fallback
                edition = edition_from_page
                text_edition = detect_edition_from_text(full_text)
                if text_edition and text_edition != edition:
                    edition = text_edition

                # For Appendix entries, prefix the number to avoid collisions
                display_number = ded_number
                if edition == "Appendix" and ded_number is not None:
                    display_number = f"App.{ded_number}"

                entries.append(
                    {
                        "page": page,
                        "ded_number": display_number,
                        "ded_number_raw": ded_number,
                        "edition": edition,
                        "raw_html": entry_html,
                        "full_text": full_text,
                        "attestations": [asdict(att) for att in attestations],
                    }
                )
        else:
            blockquote = result_div.find("blockquote")
            if not isinstance(blockquote, Tag):
                return entries

            number_tag = blockquote.find("number")
            if isinstance(number_tag, Tag):
                ded_number = number_tag.get_text(strip=True)
            else:
                ded_number = None

            entry_html = f"<div class='hw_result'>{str(blockquote)}</div>"

            attestations = self.parser.parse_language_sections(entry_html, ded_number)
            if not attestations:
                return entries

            full_text = BeautifulSoup(entry_html, "html.parser").get_text(
                " ", strip=True
            )

            edition = edition_from_page
            text_edition = detect_edition_from_text(full_text)
            if text_edition and text_edition != edition:
                edition = text_edition

            display_number = ded_number
            if edition == "Appendix" and ded_number is not None:
                display_number = f"App.{ded_number}"

            entries.append(
                {
                    "page": page,
                    "ded_number": display_number,
                    "ded_number_raw": ded_number,
                    "edition": edition,
                    "raw_html": entry_html,
                    "full_text": full_text,
                    "attestations": [asdict(att) for att in attestations],
                }
            )

        return entries

    def scrape_page(self, page: int) -> None:
        if page in self.completed_pages:
            print(f"Skipping page {page} (already completed).")
            return

        edition_label = classify_edition(page)
        print(f"\n{'=' * 70}")
        print(f"SCRAPING PAGE {page} [{edition_label}]")
        print(f"{'=' * 70}")

        params = {"page": page}
        response = self.fetch_with_retry(self.base_url, params=params)
        if not response:
            print(f"Failed to fetch page {page}")
            return

        soup = BeautifulSoup(response.content, "html.parser")
        raw_results = soup.find_all("div", class_="hw_result")
        results: List[Tag] = [r for r in raw_results if isinstance(r, Tag)]

        print(f"Found {len(results)} hw_result blocks on page {page}.")

        new_entries: List[Dict[str, Any]] = []
        for result_div in results:
            extracted = self._extract_entries_from_result_div(result_div, page)
            new_entries.extend(extracted)

        dedr_count = sum(1 for e in new_entries if e.get("edition") == "DEDR")
        app_count = sum(1 for e in new_entries if e.get("edition") == "Appendix")
        print(
            f"Extracted {len(new_entries)} entries from page {page} "
            f"(DEDR: {dedr_count}, Appendix: {app_count})."
        )

        if new_entries:
            self.entries.extend(new_entries)

        self.completed_pages.add(page)
        self._save_corpus()
        self._save_checkpoint()

        time.sleep(1.0)

    def scrape_all(self) -> None:
        print(f"Starting Burrow corpus scrape: pages {self.start_page}-{self.end_page}")
        print(f"Output directory: {self.output_dir}")
        print(f"Appendix starts at page {APPENDIX_START_PAGE}")

        for page in range(self.start_page, self.end_page + 1):
            self.scrape_page(page)

        dedr_total = sum(1 for e in self.entries if e.get("edition") == "DEDR")
        app_total = sum(1 for e in self.entries if e.get("edition") == "Appendix")

        print(f"\n{'=' * 70}")
        print("BURROW CORPUS SCRAPE COMPLETE")
        print(
            f"Total entries: {len(self.entries)} (DEDR: {dedr_total}, Appendix: {app_total})"
        )
        print(f"{'=' * 70}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Burrow DED pages into a local JSON corpus."
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="First page number to scrape (default: 1)",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=514,
        help="Last page number to scrape (default: 514)",
    )
    parser.add_argument(
        "--output-dir",
        default="validation_output/burrow_cache",
        help="Directory for burrow_corpus.json and checkpoint",
    )

    args = parser.parse_args()

    scraper = BurrowCorpusScraper(
        start_page=args.start_page,
        end_page=args.end_page,
        output_dir=args.output_dir,
    )
    scraper.scrape_all()


if __name__ == "__main__":
    main()
