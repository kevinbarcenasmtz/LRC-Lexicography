"""
Semitic etymology scraper for StarlingDB (`semet`).

The output and checkpoint shape are aligned with the Indo-European and
Sino-Tibetan Starling scrapers in this repository:

- main JSON output is a bare list of top-level records
- each top-level record contains `_sub_entries`
- checkpoint state is stored separately in `checkpoint.json`
- failed fetches are written to `failed_urls.csv`

Scope note:
- the Semitic database exposes `Afroasiatic etymology` sub-links into `afaset`
- those Proto-Afroasiatic entries are explicitly ignored
- the scraper still inspects for sub-entry links so the structure stays
  consistent with the other Starling scrapers
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, cast
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


START_URL = (
    "https://starlingdb.org/cgi-bin/response.cgi?"
    "root=config&basename=%2fdata%2fsemham%2fsemet"
)
BASE_URL = "https://starlingdb.org"
TOTAL_PAGES = 147
RECORDS_PER_PAGE = 20
MAX_DEPTH = 5
REQUEST_DELAY = 1.0
DEPTH_DELAY_INCREMENT = 0.1
MAX_RETRIES = 3

OUTPUT_DIR = Path("data") / "indo-iranian"
RESULTS_JSON_FILENAME = "semitic_etymology_complete.json"
CHECKPOINT_FILENAME = "checkpoint.json"
FAILED_FILENAME = "failed_urls.csv"

FOLLOW_BASENAMES: set[str] = set()
CAPTURE_ONLY_BASENAMES: set[str] = set()
MAIN_BASENAMES = {"semet"}
IGNORED_BASENAMES = {"afaset"}

ScrapedRecord = dict[str, object]
Stats = dict[str, int]
FailedUrlRow = dict[str, str]


class SemiticEtymologyScraper:
    def __init__(
        self,
        start_page: int = 1,
        end_page: int = TOTAL_PAGES,
        output_dir: Path = OUTPUT_DIR,
    ) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.output_dir / CHECKPOINT_FILENAME
        self.results_json_file = self.output_dir / RESULTS_JSON_FILENAME
        self.failed_file = self.output_dir / FAILED_FILENAME

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "LRC-Lexicography-Research/1.0 (academic linguistic research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://starlingdb.org/cgi-bin/main.cgi",
            }
        )

        self.all_records: list[ScrapedRecord] = []
        self.visited_urls: set[str] = set()
        self.seen_content: set[str] = set()
        self.failed_urls: list[FailedUrlRow] = []
        self.stats: Stats = {
            "main_records": 0,
            "sub_entries_fetched": 0,
            "circular_refs_skipped": 0,
            "depth_cap_hits": 0,
            "errors": 0,
            "ignored_subentries_skipped": 0,
        }

        self.current_page = start_page
        self.end_page = end_page
        self._load_checkpoint()

    # ------------------------------------------------------------------
    # Checkpoint management
    # ------------------------------------------------------------------
    def _load_checkpoint(self) -> None:
        loaded_any_state = False

        if self.checkpoint_file.exists():
            with self.checkpoint_file.open("r", encoding="utf-8") as handle:
                checkpoint = cast(dict[str, object], json.load(handle))
            loaded_any_state = True

            current_page_value = checkpoint.get("current_page")
            if isinstance(current_page_value, int) and current_page_value >= 1:
                self.current_page = current_page_value

            self.visited_urls = set(cast(list[str], checkpoint.get("visited_urls", [])))
            self.seen_content = set(cast(list[str], checkpoint.get("seen_content", [])))

            loaded_stats = checkpoint.get("stats")
            if isinstance(loaded_stats, dict):
                for key in self.stats:
                    value = loaded_stats.get(key)
                    if isinstance(value, int):
                        self.stats[key] = value

        if self.results_json_file.exists():
            with self.results_json_file.open("r", encoding="utf-8") as handle:
                loaded_records = json.load(handle)
            loaded_any_state = True

            if isinstance(loaded_records, list):
                self.all_records = [
                    cast(ScrapedRecord, record)
                    for record in loaded_records
                    if isinstance(record, dict)
                ]
            elif isinstance(loaded_records, dict):
                # Backward compatibility with the initial wrapper format.
                wrapped_records = loaded_records.get("records", [])
                if isinstance(wrapped_records, list):
                    self.all_records = [
                        cast(ScrapedRecord, record)
                        for record in wrapped_records
                        if isinstance(record, dict)
                    ]

        if loaded_any_state:
            print(
                f"Resumed from checkpoint: page {self.current_page}, "
                f"{len(self.all_records)} records, "
                f"{len(self.visited_urls)} visited URLs"
            )

    def _save_checkpoint(self) -> None:
        checkpoint = {
            "current_page": self.current_page,
            "visited_urls": list(self.visited_urls),
            "seen_content": list(self.seen_content),
            "stats": self.stats,
            "timestamp": datetime.now().isoformat(),
        }
        with self.checkpoint_file.open("w", encoding="utf-8") as handle:
            json.dump(checkpoint, handle, indent=2)

    def _save_results_json(self) -> None:
        with self.results_json_file.open("w", encoding="utf-8") as handle:
            json.dump(self.all_records, handle, indent=2, ensure_ascii=False)

    def _save_failed_urls(self) -> None:
        if not self.failed_urls:
            if self.failed_file.exists():
                self.failed_file.unlink()
            return

        with self.failed_file.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "timestamp",
                    "url",
                    "error",
                    "exception_type",
                    "status_code",
                    "attempts",
                ],
            )
            writer.writeheader()
            writer.writerows(self.failed_urls)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    def fetch(self, url: str) -> Optional[requests.Response]:
        last_error: Optional[requests.RequestException] = None
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                wait_seconds = (2**attempt) * 2
                print(
                    f"      Retry {attempt + 1}/{MAX_RETRIES}: "
                    f"{str(exc)[:80]}, waiting {wait_seconds}s"
                )
                time.sleep(wait_seconds)

        status_code = ""
        exception_type = ""
        error_text = "max retries exceeded"
        if last_error is not None:
            error_text = str(last_error)
            exception_type = type(last_error).__name__
            response = getattr(last_error, "response", None)
            if response is not None and getattr(response, "status_code", None) is not None:
                status_code = str(response.status_code)

        self.failed_urls.append(
            {
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "error": error_text,
                "exception_type": exception_type,
                "status_code": status_code,
                "attempts": str(MAX_RETRIES),
            }
        )
        self.stats["errors"] += 1
        return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------
    @staticmethod
    def extract_field(div: Tag) -> tuple[Optional[str], Optional[str]]:
        field = div.find("span", class_="fld")
        if not isinstance(field, Tag):
            return None, None

        field_name = field.get_text(" ", strip=True).rstrip(":")
        unicode_value = div.find("span", class_="unicode")
        link = div.find("a")

        if isinstance(unicode_value, Tag):
            value = unicode_value.get_text(" ", strip=True)
        elif isinstance(link, Tag):
            value = link.get_text(" ", strip=True)
        else:
            value = div.get_text(" ", strip=True).replace(field.get_text(" ", strip=True), "", 1).strip()

        value = re.sub(r"\s+", " ", value).strip()
        return field_name, value

    @staticmethod
    def extract_sub_url(div: Tag) -> Optional[str]:
        subquery = div.find("div", class_="subquery_link")
        if not isinstance(subquery, Tag):
            return None

        image = subquery.find("img", attrs={"onclick": True})
        if not isinstance(image, Tag):
            return None

        onclick = image.get("onclick", "")
        match = re.search(r"'([^']+)'", str(onclick))
        if not match:
            return None

        return urljoin(BASE_URL + "/cgi-bin/", match.group(1))

    @staticmethod
    def content_hash(data: dict[str, object]) -> Optional[str]:
        filtered = {key: value for key, value in data.items() if not key.startswith("_")}
        if not filtered:
            return None
        serialized = json.dumps(filtered, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def basename_from_url(url: str) -> Optional[str]:
        encoded_match = re.search(r"basename=%2[fF]([^&]+)", url)
        if encoded_match:
            decoded = encoded_match.group(1).replace("%2f", "/").replace("%2F", "/")
            parts = [part for part in decoded.split("/") if part]
            return parts[-1] if parts else None

        plain_match = re.search(r"basename=/([^&]+)", url)
        if plain_match:
            parts = [part for part in plain_match.group(1).split("/") if part]
            return parts[-1] if parts else None

        return None

    def should_follow(self, url: str) -> str:
        if url in self.visited_urls:
            return "skip"

        basename = self.basename_from_url(url)
        if not basename:
            return "skip"

        basename_lower = basename.lower()
        if basename_lower in MAIN_BASENAMES:
            return "skip"
        if basename_lower in IGNORED_BASENAMES:
            return "skip"
        if basename_lower in FOLLOW_BASENAMES:
            return "follow"
        if basename_lower in CAPTURE_ONLY_BASENAMES:
            return "capture"
        return "skip"

    def should_omit_field(self, div: Tag, field_name: str) -> bool:
        if field_name != "Afroasiatic etymology":
            return False
        sub_url = self.extract_sub_url(div)
        if not sub_url:
            return False
        basename = self.basename_from_url(sub_url)
        return bool(basename and basename.lower() in IGNORED_BASENAMES)

    # ------------------------------------------------------------------
    # Recursive sub-entry scraping
    # ------------------------------------------------------------------
    def scrape_sub_entry(
        self, url: str, depth: int, recurse: bool = True
    ) -> ScrapedRecord:
        indent = "  " * depth

        if depth > MAX_DEPTH:
            self.stats["depth_cap_hits"] += 1
            return {"_depth_capped": True, "_url": url, "_depth": depth}

        if url in self.visited_urls:
            self.stats["circular_refs_skipped"] += 1
            return {"_circular_reference": True, "_url": url, "_depth": depth}

        basename = self.basename_from_url(url) or "unknown"
        self.visited_urls.add(url)

        label = f"{indent}Depth {depth} [{basename}]"
        if not recurse:
            label += " (capture-only)"
        print(label)

        response = self.fetch(url)
        if not response:
            return {"_error": "fetch_failed", "_url": url, "_depth": depth}

        soup = BeautifulSoup(response.text, "html.parser")
        record = soup.find("div", class_="results_record")
        if not isinstance(record, Tag):
            return {"_error": "no_record", "_url": url, "_depth": depth}

        entry: ScrapedRecord = {"_url": url, "_depth": depth, "_basename": basename}

        for div in record.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue
            field_name, value = self.extract_field(div)
            if field_name:
                if self.should_omit_field(div, field_name):
                    continue
                entry[field_name] = value

        content_hash = self.content_hash(entry)
        if content_hash:
            if content_hash in self.seen_content:
                self.stats["circular_refs_skipped"] += 1
                return {
                    "_circular_reference": True,
                    "_url": url,
                    "_depth": depth,
                    "_content_hash": content_hash,
                }
            self.seen_content.add(content_hash)
            entry["_content_hash"] = content_hash

        self.stats["sub_entries_fetched"] += 1

        if not recurse:
            return entry

        for div in record.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue

            sub_url = self.extract_sub_url(div)
            if not sub_url:
                continue

            action = self.should_follow(sub_url)
            if action == "skip":
                basename = self.basename_from_url(sub_url)
                if basename and basename.lower() in IGNORED_BASENAMES:
                    self.stats["ignored_subentries_skipped"] += 1
                else:
                    self.stats["circular_refs_skipped"] += 1
                continue

            child_recurse = action == "follow"
            sub_data = self.scrape_sub_entry(sub_url, depth + 1, recurse=child_recurse)

            if not sub_data.get("_circular_reference") and not sub_data.get("_depth_capped"):
                if "_sub_entries" not in entry:
                    entry["_sub_entries"] = []
                cast(list[ScrapedRecord], entry["_sub_entries"]).append(sub_data)

            time.sleep(REQUEST_DELAY + (depth * DEPTH_DELAY_INCREMENT))

        return entry

    # ------------------------------------------------------------------
    # Main page scraping
    # ------------------------------------------------------------------
    def scrape_record(
        self, record_div: Tag, page_num: int, record_num: int
    ) -> ScrapedRecord:
        print(f"\n[Page {page_num}, Record {record_num}]")

        record: ScrapedRecord = {
            "_page": page_num,
            "_record_num": record_num,
            "_sub_entries": [],
        }

        for div in record_div.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue
            field_name, value = self.extract_field(div)
            if field_name:
                if self.should_omit_field(div, field_name):
                    continue
                record[field_name] = value

        content_hash = self.content_hash(record)
        if content_hash:
            record["_content_hash"] = content_hash
            self.seen_content.add(content_hash)

        self.stats["main_records"] += 1

        for div in record_div.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue

            sub_url = self.extract_sub_url(div)
            if not sub_url:
                continue

            action = self.should_follow(sub_url)
            if action == "skip":
                basename = self.basename_from_url(sub_url)
                if basename and basename.lower() in IGNORED_BASENAMES:
                    self.stats["ignored_subentries_skipped"] += 1
                else:
                    self.stats["circular_refs_skipped"] += 1
                continue

            child_recurse = action == "follow"
            sub_data = self.scrape_sub_entry(sub_url, depth=1, recurse=child_recurse)
            if not sub_data.get("_circular_reference") and not sub_data.get("_depth_capped"):
                cast(list[ScrapedRecord], record["_sub_entries"]).append(sub_data)

            time.sleep(REQUEST_DELAY)

        return record

    def scrape_page(self, page_num: int) -> list[ScrapedRecord]:
        if page_num == 1:
            url = START_URL
        else:
            first = 1 + (page_num - 1) * RECORDS_PER_PAGE
            url = f"{START_URL}&first={first}"

        print(f"\n{'=' * 72}")
        print(f"PAGE {page_num}/{TOTAL_PAGES}")
        print(f"{'=' * 72}")

        response = self.fetch(url)
        if not response:
            print(f"Failed to fetch page {page_num}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        records = soup.find_all("div", class_="results_record")
        print(f"Found {len(records)} records")

        page_records: list[ScrapedRecord] = []
        for record_num, record_div in enumerate(records, 1):
            if not isinstance(record_div, Tag):
                continue
            try:
                page_records.append(self.scrape_record(record_div, page_num, record_num))
            except Exception as exc:
                print(f"Error on record {record_num}: {exc}")
                self.stats["errors"] += 1

        time.sleep(REQUEST_DELAY)
        return page_records

    def scrape_all(self) -> None:
        print(f"Starting scrape from: {START_URL}")
        print(f"Page range: {self.current_page}..{self.end_page}")
        print("Output format: aligned with the PIET / Sino-Tibetan Starling scrapers")
        print("Proto-Afroasiatic handling: detected and skipped\n")

        for page in range(self.current_page, self.end_page + 1):
            self.current_page = page
            page_records = self.scrape_page(page)
            self.all_records.extend(page_records)

            self._save_results_json()
            self._save_checkpoint()
            self._save_failed_urls()

            print(
                f"\nProgress: {len(self.all_records)} records, "
                f"{len(self.visited_urls)} visited URLs, "
                f"{self.stats['ignored_subentries_skipped']} ignored Proto-Afroasiatic sub-links"
            )

        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

        print(f"\n{'=' * 72}")
        print("COMPLETE")
        print(f"Saved JSON: {self.results_json_file}")
        print(f"Saved failed URL log: {self.failed_file}")
        print(f"{'=' * 72}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Starling Semitic etymology while ignoring Proto-Afroasiatic sub-entries."
    )
    parser.add_argument("start_page", nargs="?", type=int, default=1)
    parser.add_argument("--start-page", dest="start_page_opt", type=int)
    parser.add_argument("--end-page", type=int, default=TOTAL_PAGES)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_page = args.start_page_opt if args.start_page_opt is not None else args.start_page
    if start_page < 1:
        raise SystemExit("start page must be >= 1")
    if args.end_page < start_page:
        raise SystemExit("end page must be >= start page")

    scraper = SemiticEtymologyScraper(
        start_page=start_page,
        end_page=args.end_page,
        output_dir=Path(args.output_dir),
    )
    scraper.scrape_all()


if __name__ == "__main__":
    main()
