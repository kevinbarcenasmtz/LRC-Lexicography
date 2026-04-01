"""
Sino-Tibetan Etymology Scraper for StarlingDB
===============================================
Full recursive scrape of https://starlingdb.org Sino-Tibetan database (stibet).
2823 records across 142 pages. Estimated ~6,350 requests (~1.8 hours at 1s delay).

Sub-entry scope:
  FOLLOW recursively:
  - Chinese (bigchina) -> historical phonology, radicals, Japanese/Vietnamese readings
  - Chinese dialectal data (doc) -> modern dialect readings across ~20 points
  - Kiranti (kiret) -> Proto-Kiranti + daughter languages (Limbu, Dumi, Kulung, Yamphu)

  CAPTURE flat (no child recursion):
  - Sino-Caucasian (sccet) -> macro-comparative links (Proto-SC, North Caucasian,
    Basque, Borean values captured but their sub-trees are NOT followed to avoid
    unbounded expansion into North Caucasian, Borean, Nostratic, etc.)

  SKIP:
  - Back-links to stibet from within sub-entries (circular)
  - Any database not in the whitelist

Checkpoint support: saves progress after each page. Resume by re-running.
Usage:
    python scrape_sinotibetan.py                         # start from page 1
    python scrape_sinotibetan.py 50                      # resume from page 50
    python scrape_sinotibetan.py --output-formats json xlsx
    python scrape_sinotibetan.py --print-screen-command  # print detached run command

Based on the proven Dravidian StarlingDB scraper architecture.
"""

import argparse
import json
import os
import re
import sys
import time
import hashlib
import csv
import shlex
from datetime import datetime
from typing import Optional, cast
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
START_URL = "https://starlingdb.org/cgi-bin/response.cgi?root=config&basename=%2fdata%2fsintib%2fstibet"
BASE_URL = "https://starlingdb.org"
TOTAL_PAGES = 142
TOTAL_RECORDS = 2823
RECORDS_PER_PAGE = 20
MAX_DEPTH = 5
REQUEST_DELAY = 1.0
DEPTH_DELAY_INCREMENT = 0.1
MAX_RETRIES = 3

OUTPUT_DIR = os.path.join("data", "sino-tibetan")
CHECKPOINT_FILENAME = "checkpoint.json"
RESULTS_JSON_FILENAME = "sinotibetan_complete.json"
RESULTS_XLSX_FILENAME = "sinotibetan_complete.xlsx"
FAILED_FILENAME = "failed_urls.csv"
LOG_FILENAME = "sinotibetan_scrape.log"
XLSX_EXCLUDED_FIELDS = {"_content_hash", "_url"}

# Basenames to follow recursively (Sino-Tibetan family + Chinese dialectal)
FOLLOW_BASENAMES = {
    "bigchina",  # Chinese historical phonology
    "doc",  # Chinese dialectal data
    "dialet",  # Chinese dialectal data (alt path)
    "kiret",  # Proto-Kiranti
    "limet",  # Limbu
    "dumet",  # Dumi
    "kulet",  # Kulung
    "yamet",  # Yamphu
}

# Basenames to capture at depth=1 only (don't recurse into their sub-trees)
CAPTURE_ONLY_BASENAMES = {
    "sccet",  # Sino-Caucasian -- capture comparison but skip Borean/NC daughters
}

# The main database -- links back here are always circular
MAIN_BASENAMES = {"stibet"}


ScrapedRecord = dict[str, object]
Stats = dict[str, int]
FailedUrlRow = dict[str, str]


class SinoTibetanScraper:
    def __init__(
        self,
        start_page: int = 1,
        output_dir: str = OUTPUT_DIR,
        output_formats: Optional[set[str]] = None,
        max_records: Optional[int] = None,
    ):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_formats = output_formats or {"json", "xlsx"}
        self.checkpoint_file = os.path.join(self.output_dir, CHECKPOINT_FILENAME)
        self.results_json_file = os.path.join(self.output_dir, RESULTS_JSON_FILENAME)
        self.results_xlsx_file = os.path.join(self.output_dir, RESULTS_XLSX_FILENAME)
        self.failed_file = os.path.join(self.output_dir, FAILED_FILENAME)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "LRC-Lexicography-Research/1.0 (academic linguistic research)"
            }
        )

        self.all_records: list[ScrapedRecord] = []
        self.visited_urls: set[str] = set()
        self.seen_content: set[str] = set()
        self.failed_urls: list[FailedUrlRow] = []
        self.max_records = max_records
        self.stats: Stats = {
            "main_records": 0,
            "sub_entries_fetched": 0,
            "circular_refs_skipped": 0,
            "depth_cap_hits": 0,
            "errors": 0,
        }

        self.current_page = start_page
        self._load_checkpoint()

    # ------------------------------------------------------------------
    # Checkpoint management
    # ------------------------------------------------------------------
    def _load_checkpoint(self) -> None:
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                cp = cast(dict[str, object], json.load(f))
            current_page_value = cp.get("current_page")
            if isinstance(current_page_value, int) and current_page_value >= 1:
                self.current_page = current_page_value
            self.visited_urls = set(cast(list[str], cp.get("visited_urls", [])))
            self.seen_content = set(cast(list[str], cp.get("seen_content", [])))
            loaded_stats = cp.get("stats")
            if isinstance(loaded_stats, dict):
                for key in self.stats:
                    value = loaded_stats.get(key)
                    if isinstance(value, int):
                        self.stats[key] = value

            if os.path.exists(self.results_json_file):
                with open(self.results_json_file, "r", encoding="utf-8") as f:
                    loaded_records = json.load(f)
                if isinstance(loaded_records, list):
                    self.all_records = [
                        cast(ScrapedRecord, rec)
                        for rec in loaded_records
                        if isinstance(rec, dict)
                    ]

            print(
                f"Resumed from checkpoint: page {self.current_page}, "
                f"{len(self.all_records)} records, "
                f"{len(self.visited_urls)} visited URLs"
            )

    def _save_checkpoint(self) -> None:
        cp = {
            "current_page": self.current_page,
            "visited_urls": list(self.visited_urls),
            "seen_content": list(self.seen_content),
            "stats": self.stats,
            "timestamp": datetime.now().isoformat(),
        }
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(cp, f, indent=2)

    def _save_results_json(self) -> None:
        with open(self.results_json_file, "w", encoding="utf-8") as f:
            json.dump(self.all_records, f, indent=2, ensure_ascii=False)

    def _serialize_cell_value(self, value: object) -> object:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)

    def _flatten_entry(
        self, entry: ScrapedRecord, parent_path: str = "", index: int = 1
    ) -> list[dict[str, object]]:
        tree_path = f"{parent_path}.{index}" if parent_path else str(index)
        row: dict[str, object] = {"_tree_path": tree_path}
        sub_entries: list[ScrapedRecord] = []

        for key, value in entry.items():
            if key == "_sub_entries":
                if isinstance(value, list):
                    for child in value:
                        if isinstance(child, dict):
                            sub_entries.append(cast(ScrapedRecord, child))
                continue
            if key in XLSX_EXCLUDED_FIELDS:
                continue
            row[key] = self._serialize_cell_value(value)

        rows = [row]
        for child_index, child in enumerate(sub_entries, 1):
            rows.extend(
                self._flatten_entry(child, parent_path=tree_path, index=child_index)
            )
        return rows

    def _save_results_xlsx(self) -> None:
        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError(
                "XLSX export requested but pandas is not available in this environment."
            ) from exc

        rows: list[dict[str, object]] = []
        for top_index, record in enumerate(self.all_records, 1):
            rows.extend(self._flatten_entry(record, parent_path="", index=top_index))
        frame = pd.DataFrame(rows)
        frame.to_excel(self.results_xlsx_file, index=False)

    def _save_failed_urls(self) -> None:
        if not self.failed_urls:
            return
        with open(self.failed_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
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
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                last_error = e
                wait = (2**attempt) * 2
                print(
                    f"      Retry {attempt + 1}/{MAX_RETRIES}: {str(e)[:60]}, waiting {wait}s"
                )
                time.sleep(wait)

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
        """Extract (field_name, value_text) from a results_record child div."""
        fld = div.find("span", class_="fld")
        if not isinstance(fld, Tag):
            return None, None
        name = fld.get_text(strip=True).rstrip(":")
        val_span = div.find("span", class_="unicode")
        # Also grab linked text (for fields like Chinese that have <a> wrappers)
        link = div.find("a")
        if isinstance(val_span, Tag):
            value = val_span.get_text(strip=True)
        elif isinstance(link, Tag):
            value = link.get_text(strip=True)
        else:
            value = div.get_text(strip=True).replace(fld.get_text(), "", 1).strip()
        return name, value

    @staticmethod
    def extract_sub_url(div: Tag) -> Optional[str]:
        """Extract the sub-entry URL from a subquery_link div."""
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

    @staticmethod
    def content_hash(data: dict[str, object]) -> Optional[str]:
        """Hash non-metadata fields for dedup."""
        filtered = {k: v for k, v in data.items() if not k.startswith("_")}
        if not filtered:
            return None
        raw = json.dumps(filtered, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def basename_from_url(url: str) -> Optional[str]:
        """Extract the database basename (e.g. 'stibet', 'bigchina') from a StarlingDB URL."""
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

    def should_follow(self, url: str) -> str:
        """Determine how to handle a sub-entry URL.
        Returns: 'follow' (recurse), 'capture' (scrape but no recursion), 'skip'
        """
        if url in self.visited_urls:
            return "skip"
        basename = self.basename_from_url(url)
        if not basename:
            return "skip"
        if basename.lower() in MAIN_BASENAMES:
            return "skip"
        if basename.lower() in FOLLOW_BASENAMES:
            return "follow"
        if basename.lower() in CAPTURE_ONLY_BASENAMES:
            return "capture"
        return "skip"

    # ------------------------------------------------------------------
    # Recursive sub-entry scraping
    # ------------------------------------------------------------------
    def scrape_sub_entry(
        self, url: str, depth: int, recurse: bool = True
    ) -> ScrapedRecord:
        """Scrape a sub-entry page.
        If recurse=False, capture fields only (no child link following).
        """
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

        resp = self.fetch(url)
        if not resp:
            return {"_error": "fetch_failed", "_url": url, "_depth": depth}

        soup = BeautifulSoup(resp.text, "html.parser")
        record = soup.find("div", class_="results_record")
        if not isinstance(record, Tag):
            return {"_error": "no_record", "_url": url, "_depth": depth}

        entry: ScrapedRecord = {"_url": url, "_depth": depth, "_basename": basename}

        for div in record.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue
            field_name, value = self.extract_field(div)
            if field_name:
                entry[field_name] = value

        ch = self.content_hash(entry)
        if ch:
            if ch in self.seen_content:
                self.stats["circular_refs_skipped"] += 1
                return {
                    "_circular_reference": True,
                    "_url": url,
                    "_depth": depth,
                    "_content_hash": ch,
                }
            self.seen_content.add(ch)
            entry["_content_hash"] = ch

        self.stats["sub_entries_fetched"] += 1

        if not recurse:
            return entry

        # Follow child links based on scope rules
        for div in record.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue
            field_name, _ = self.extract_field(div)
            if not field_name:
                continue

            sub_url = self.extract_sub_url(div)
            if not sub_url:
                continue

            action = self.should_follow(sub_url)
            if action == "skip":
                self.stats["circular_refs_skipped"] += 1
                continue

            child_recurse = action == "follow"
            sub_data = self.scrape_sub_entry(sub_url, depth + 1, recurse=child_recurse)

            if not sub_data.get("_circular_reference") and not sub_data.get(
                "_depth_capped"
            ):
                if "_sub_entries" not in entry:
                    entry["_sub_entries"] = []
                sub_entries = cast(list[ScrapedRecord], entry["_sub_entries"])
                sub_entries.append(sub_data)

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
                record[field_name] = value

        ch = self.content_hash(record)
        if ch:
            record["_content_hash"] = ch
            self.seen_content.add(ch)

        self.stats["main_records"] += 1

        for div in record_div.find_all("div", recursive=False):
            if not isinstance(div, Tag):
                continue
            field_name, _ = self.extract_field(div)
            if not field_name:
                continue

            sub_url = self.extract_sub_url(div)
            if not sub_url:
                continue

            action = self.should_follow(sub_url)
            if action == "skip":
                self.stats["circular_refs_skipped"] += 1
                continue

            child_recurse = action == "follow"
            sub_data = self.scrape_sub_entry(sub_url, depth=1, recurse=child_recurse)
            if not sub_data.get("_circular_reference") and not sub_data.get(
                "_depth_capped"
            ):
                sub_entries = cast(list[ScrapedRecord], record["_sub_entries"])
                sub_entries.append(sub_data)

            time.sleep(REQUEST_DELAY)

        return record

    def scrape_page(
        self, page_num: int, max_page_records: Optional[int] = None
    ) -> list[ScrapedRecord]:
        if page_num == 1:
            url = START_URL
        else:
            first = 1 + (page_num - 1) * RECORDS_PER_PAGE
            url = f"{START_URL}&first={first}"

        resp = self.fetch(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        records = soup.find_all("div", class_="results_record")
        page_data: list[ScrapedRecord] = []

        for i, rec_div in enumerate(records, 1):
            if isinstance(max_page_records, int) and len(page_data) >= max_page_records:
                break
            if not isinstance(rec_div, Tag):
                continue
            rec = self.scrape_record(rec_div, page_num, i)
            page_data.append(rec)

        return page_data

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------
    def scrape_all(self) -> None:
        print(f"Sino-Tibetan StarlingDB Scraper")
        print(f"{'=' * 60}")
        print(f"Start URL: {START_URL}")
        print(f"Total: {TOTAL_RECORDS} records, {TOTAL_PAGES} pages")
        print(f"Strategy: Full recursive (all sub-entries)")
        print(f"Max depth: {MAX_DEPTH}")
        print(f"Starting from page: {self.current_page}")
        if self.max_records is not None:
            print(f"Max records this run: {self.max_records}")
        print(f"{'=' * 60}\n")

        for page in range(self.current_page, TOTAL_PAGES + 1):
            self.current_page = page
            remaining: Optional[int] = None
            if isinstance(self.max_records, int):
                remaining = self.max_records - len(self.all_records)
                if remaining <= 0:
                    break
            page_records = self.scrape_page(page, max_page_records=remaining)
            self.all_records.extend(page_records)

            self._save_results_json()
            self.current_page = page + 1
            self._save_checkpoint()

            pct = (page / TOTAL_PAGES) * 100
            print(
                f"\n--- Page {page}/{TOTAL_PAGES} ({pct:.1f}%) | "
                f"{len(self.all_records)} records | "
                f"{self.stats['sub_entries_fetched']} sub-entries | "
                f"{len(self.visited_urls)} URLs visited ---"
            )

            if isinstance(self.max_records, int) and len(self.all_records) >= self.max_records:
                print(
                    f"Reached max records limit ({self.max_records}); stopping early."
                )
                break

            time.sleep(REQUEST_DELAY)

        self._save_failed_urls()

        if "xlsx" in self.output_formats:
            self._save_results_xlsx()

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

        print(f"\n{'=' * 60}")
        print(f"COMPLETE")
        print(f"{'=' * 60}")
        print(f"Main records: {self.stats['main_records']}")
        print(f"Sub-entries fetched: {self.stats['sub_entries_fetched']}")
        print(f"Circular refs skipped: {self.stats['circular_refs_skipped']}")
        print(f"Depth cap hits: {self.stats['depth_cap_hits']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Total URLs visited: {len(self.visited_urls)}")
        if "json" in self.output_formats:
            print(f"JSON Output: {self.results_json_file}")
        if "xlsx" in self.output_formats:
            print(f"XLSX Output: {self.results_xlsx_file}")
        if self.failed_urls:
            print(f"Failed URL report: {self.failed_file}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Sino-Tibetan StarlingDB records with checkpoint support."
    )
    parser.add_argument(
        "start_page",
        nargs="?",
        type=int,
        default=1,
        help="Page to start scraping from (default: 1).",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help=f"Directory for outputs/checkpoint (default: {OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--output-formats",
        nargs="+",
        choices=["json", "xlsx"],
        default=["json", "xlsx"],
        help="Output formats to export at completion (default: json xlsx).",
    )
    parser.add_argument(
        "--session-name",
        default="sino_tibetan",
        help="Session name when generating a detached screen command.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after scraping this many main records (for smoke tests).",
    )
    parser.add_argument(
        "--print-screen-command",
        action="store_true",
        help="Print a detached `screen` command for this run and exit.",
    )
    args = parser.parse_args(argv)
    if args.start_page < 1:
        parser.error("start_page must be >= 1")
    if args.max_records is not None and args.max_records < 1:
        parser.error("--max-records must be >= 1")
    return args


def build_screen_command(
    script_path: str,
    start_page: int,
    output_dir: str,
    output_formats: list[str],
    session_name: str,
    max_records: Optional[int],
) -> str:
    format_args = " ".join(output_formats)
    normalized_script_path = script_path.replace("\\", "/")
    run_cmd = (
        f"python {shlex.quote(normalized_script_path)} {start_page} "
        f"--output-dir {shlex.quote(output_dir)} --output-formats {format_args}"
    )
    if isinstance(max_records, int):
        run_cmd += f" --max-records {max_records}"
    log_path = os.path.join(output_dir, LOG_FILENAME).replace("\\", "/")
    return (
        f"screen -S {shlex.quote(session_name)} -dm bash -lc "
        f"\"{run_cmd} > {shlex.quote(log_path)} 2>&1\""
    )


if __name__ == "__main__":
    cli_args = parse_args(sys.argv[1:])
    if cli_args.print_screen_command:
        print(
            build_screen_command(
                script_path=__file__,
                start_page=cli_args.start_page,
                output_dir=cli_args.output_dir,
                output_formats=cli_args.output_formats,
                session_name=cli_args.session_name,
                max_records=cli_args.max_records,
            )
        )
        sys.exit(0)

    scraper = SinoTibetanScraper(
        start_page=cli_args.start_page,
        output_dir=cli_args.output_dir,
        output_formats=set(cli_args.output_formats),
        max_records=cli_args.max_records,
    )
    scraper.scrape_all()
