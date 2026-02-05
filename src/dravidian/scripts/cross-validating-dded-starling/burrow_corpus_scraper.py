"""
Burrow DED corpus scraper.

Scrapes Burrow & Emeneau DED pages 1–514 into a local JSON corpus of entries
and language attestations for offline cross-validation.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import time

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from burrow_entry_parser import BurrowEntryParser, LanguageAttestation


class BurrowCorpusScraper:
    """
    Scrape Burrow DED pages into a structured local corpus.

    For each page=1..N, this scraper:
    - Fetches the page listing.
    - For each entry-like block, extracts the per-entry HTML.
    - Uses BurrowEntryParser to extract language attestations.
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
        except Exception as exc:  # pragma: no cover - defensive
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
        except Exception as exc:  # pragma: no cover - defensive
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
                response = self.session.get(
                    url, params=params, timeout=self.timeout
                )
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
        <div class=\"hw_result\"> block on a page.
        """
        entries: List[Dict[str, Any]] = []

        # Some pages contain a single blockquote; others contain multiple
        # nested <div> blocks each representing a DED entry.
        nested_divs = result_div.find_all("div", recursive=False)

        if nested_divs:
            # Page-style results with multiple nested entries.
            candidate_divs: List[Tag] = [
                d for d in nested_divs if isinstance(d, Tag)
            ]
            for div in candidate_divs:
                number_tag = div.find("number")
                ded_number: Optional[str]
                if isinstance(number_tag, Tag):
                    ded_number = number_tag.get_text(strip=True)
                else:
                    ded_number = None

                # Wrap into a hw_result container so BurrowEntryParser can work.
                entry_html = f"<div class='hw_result'>{str(div)}</div>"

                attestations = self.parser.parse_language_sections(
                    entry_html, ded_number
                )
                if not attestations:
                    continue

                full_text = BeautifulSoup(
                    entry_html, "html.parser"
                ).get_text(" ", strip=True)

                entries.append(
                    {
                        "page": page,
                        "ded_number": ded_number,
                        "raw_html": entry_html,
                        "full_text": full_text,
                        "attestations": [
                            asdict(att) for att in attestations
                        ],
                    }
                )
        else:
            # Simpler hw_result with a single blockquote.
            blockquote = result_div.find("blockquote")
            if not isinstance(blockquote, Tag):
                return entries

            number_tag = blockquote.find("number")
            if isinstance(number_tag, Tag):
                ded_number = number_tag.get_text(strip=True)
            else:
                ded_number = None

            entry_html = f"<div class='hw_result'>{str(blockquote)}</div>"

            attestations = self.parser.parse_language_sections(
                entry_html, ded_number
            )
            if not attestations:
                return entries

            full_text = BeautifulSoup(
                entry_html, "html.parser"
            ).get_text(" ", strip=True)

            entries.append(
                {
                    "page": page,
                    "ded_number": ded_number,
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

        print(f"\n{'=' * 70}")
        print(f"SCRAPING PAGE {page}")
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

        print(f"Extracted {len(new_entries)} entries from page {page}.")

        if new_entries:
            self.entries.extend(new_entries)

        self.completed_pages.add(page)
        self._save_corpus()
        self._save_checkpoint()

        # Polite delay between pages.
        time.sleep(1.0)

    def scrape_all(self) -> None:
        print(f"Starting Burrow corpus scrape: pages {self.start_page}–{self.end_page}")
        print(f"Output directory: {self.output_dir}")

        for page in range(self.start_page, self.end_page + 1):
            self.scrape_page(page)

        print(f"\n{'=' * 70}")
        print("BURROW CORPUS SCRAPE COMPLETE")
        print(f"Total entries in corpus: {len(self.entries)}")
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

