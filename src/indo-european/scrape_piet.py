"""
Proto-Indo-European Etymology Scraper for StarlingDB
=====================================================
Full recursive scrape of https://starlingdb.org Proto-IE database (piet).
3178 records across 159 pages. Estimated ~8,134 requests (~2.3 hours at 1s delay).

Sub-entry scope (Strategy 3):
  FOLLOW recursively:
  - Baltic (baltet) -> Proto-Baltic, Lithuanian, Lettish, Old Prussian
  - Germanic (germet) -> Proto-Germanic + 17 daughter language attestations
  - Vasmer/Slavic (vasmer) -> Slavic etymological data (Vasmer's dictionary)
  - Pokorny/References (pokorny) -> Pokorny's IE dictionary entries

  CAPTURE flat (no child recursion):
  - Nostratic (nostret) -> macro-comparative links (Eurasiatic, Altaic, Uralic,
    Dravidian values captured but children GLOBET/ALTET/ESQET are NOT followed
    to avoid unbounded expansion into Borean, Altaic, Eskimo-Aleut trees)

  SKIP:
  - Back-links to piet from within sub-entries (circular)
  - GLOBET (Borean), ALTET (Altaic), ESQET (Eskimo-Aleut) and any other
    database not in the whitelist

Checkpoint support: saves progress after each page. Resume by re-running.

Usage:
    python scrape_piet.py                         # start from page 1
    python scrape_piet.py 50                      # resume from page 50
    python scrape_piet.py --output-formats json xlsx
    python scrape_piet.py --print-screen-command  # print detached run command

Based on the proven Sino-Tibetan / Dravidian StarlingDB scraper architecture.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import sys
import time
from datetime import datetime
from typing import Literal, Optional, cast
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

START_URL = (
    "https://starlingdb.org/cgi-bin/response.cgi?"
    "root=config&basename=%2fdata%2fie%2fpiet"
)
BASE_URL = "https://starlingdb.org"
TOTAL_PAGES = 159
TOTAL_RECORDS = 3178
RECORDS_PER_PAGE = 20
MAX_DEPTH = 5
REQUEST_DELAY = 1.0
DEPTH_DELAY_INCREMENT = 0.1
MAX_RETRIES = 3

OUTPUT_DIR = os.path.join("data", "indo-european")
CHECKPOINT_FILENAME = "checkpoint.json"
RESULTS_JSON_FILENAME = "piet_complete.json"
RESULTS_XLSX_FILENAME = "piet_complete.xlsx"
FAILED_FILENAME = "failed_urls.csv"
LOG_FILENAME = "piet_scrape.log"
RUN_STATS_FILENAME = "piet_run_stats.json"

XLSX_EXCLUDED_FIELDS = {"_content_hash", "_url"}

# Basenames to follow recursively (IE daughter family databases)
FOLLOW_BASENAMES = {
    "baltet",   # Baltic etymologies
    "germet",   # Germanic etymologies
    "vasmer",   # Slavic etymologies (Vasmer's dictionary)
    "pokorny",  # Pokorny's IE etymological dictionary
}

# Basenames to capture at depth=1 only (don't recurse into their sub-trees)
CAPTURE_ONLY_BASENAMES = {
    "nostret",  # Nostratic -- capture comparison but skip Borean/Altaic/Eskimo
}

# The main database -- links back here are always circular
MAIN_BASENAMES = {"piet"}

ScrapedRecord = dict[str, object]
Stats = dict[str, int]
FailedUrlRow = dict[str, str]
FollowAction = Literal["follow", "capture", "skip"]


class PIETScraper:

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
        self.log_file = os.path.join(self.output_dir, LOG_FILENAME)
        self.run_stats_file = os.path.join(self.output_dir, RUN_STATS_FILENAME)

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "LRC-Lexicography-Research/1.0 (academic linguistic research)"}
        )

        self.all_records: list[ScrapedRecord] = []
        self.visited_urls: set[str] = set()
        self.seen_content: set[str] = set()
        self.failed_urls: list[FailedUrlRow] = []
        self.max_records = max_records
        self.page_failure_detected = False
        self.run_started_at = datetime.now()
        self.logger = self._configure_logging()

        self.stats: Stats = {
            "main_records": 0,
            "sub_entries_fetched": 0,
            "circular_refs_skipped": 0,
            "depth_cap_hits": 0,
            "errors": 0,
        }
        self.current_page = start_page
        self._load_checkpoint()

    def _configure_logging(self) -> logging.Logger:
        logger = logging.getLogger(f"PIETScraper:{self.output_dir}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        if logger.handlers:
            return logger

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
        return logger

    def _log_info(self, message: str) -> None:
        self.logger.info(message)

    def _log_warning(self, message: str) -> None:
        self.logger.warning(message)

    def _log_error(self, message: str) -> None:
        self.logger.error(message)

    def _quarantine_corrupt_file(self, path: str, reason: str) -> None:
        if not os.path.exists(path):
            return
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantined_path = f"{path}.corrupt.{stamp}"
        try:
            shutil.move(path, quarantined_path)
            self._log_warning(
                f"Quarantined {path} to {quarantined_path} ({reason})."
            )
        except OSError as exc:
            self._log_error(
                f"Failed to quarantine {path} after {reason}: {exc}."
            )

    # ------------------------------------------------------------------
    # Checkpoint management
    # ------------------------------------------------------------------

    def _load_checkpoint(self) -> None:
        if not os.path.exists(self.checkpoint_file):
            return

        cp: dict[str, object]
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                parsed_cp = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            self._quarantine_corrupt_file(self.checkpoint_file, f"checkpoint load failed: {exc}")
            return
        if not isinstance(parsed_cp, dict):
            self._quarantine_corrupt_file(self.checkpoint_file, "checkpoint content is not an object")
            return
        cp = cast(dict[str, object], parsed_cp)

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
            try:
                with open(self.results_json_file, "r", encoding="utf-8") as f:
                    loaded_records = json.load(f)
            except (OSError, json.JSONDecodeError) as exc:
                self._quarantine_corrupt_file(self.results_json_file, f"results load failed: {exc}")
                loaded_records = []
            if isinstance(loaded_records, list):
                self.all_records = [
                    cast(ScrapedRecord, rec)
                    for rec in loaded_records
                    if isinstance(rec, dict)
                ]

        self._log_info(
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

    # ------------------------------------------------------------------
    # XLSX export
    # ------------------------------------------------------------------

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
                "XLSX export requested but pandas is not available."
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
                    "timestamp", "url", "error",
                    "exception_type", "status_code", "attempts",
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
                wait = (2 ** attempt) * 2
                self._log_warning(
                    f"Retry {attempt + 1}/{MAX_RETRIES}: {str(e)[:120]}, waiting {wait}s"
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

        self.failed_urls.append({
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "error": error_text,
            "exception_type": exception_type,
            "status_code": status_code,
            "attempts": str(MAX_RETRIES),
        })
        self.stats["errors"] += 1
        return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def extract_field(div: Tag) -> tuple[Optional[str], Optional[str]]:
        fld = div.find("span", class_="fld")
        if not isinstance(fld, Tag):
            return None, None
        name = fld.get_text(strip=True).rstrip(":")
        val_span = div.find("span", class_="unicode")
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
        filtered = {k: v for k, v in data.items() if not k.startswith("_")}
        if not filtered:
            return None
        raw = json.dumps(filtered, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @staticmethod
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

    def should_follow(self, url: str) -> FollowAction:
        """Returns 'follow', 'capture', or 'skip'."""
        if url in self.visited_urls:
            return "skip"
        basename = self.basename_from_url(url)
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
        self._log_info(label)

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
                    "_circular_reference": True, "_url": url,
                    "_depth": depth, "_content_hash": ch,
                }
            self.seen_content.add(ch)
            entry["_content_hash"] = ch

        self.stats["sub_entries_fetched"] += 1

        if not recurse:
            return entry

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
        self._log_info(f"[Page {page_num}, Record {record_num}]")

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

            if not sub_data.get("_circular_reference") and not sub_data.get("_depth_capped"):
                cast(list[ScrapedRecord], record["_sub_entries"]).append(sub_data)

            time.sleep(REQUEST_DELAY)

        return record

    def scrape_page(
        self, page_num: int, max_page_records: Optional[int] = None
    ) -> Optional[list[ScrapedRecord]]:
        if page_num == 1:
            url = START_URL
        else:
            first = 1 + (page_num - 1) * RECORDS_PER_PAGE
            url = f"{START_URL}&first={first}"

        resp = self.fetch(url)
        if not resp:
            return None

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

    def _save_run_summary(
        self, status: str, run_completed: bool, last_page_attempted: int
    ) -> None:
        finished_at = datetime.now()
        summary = {
            "status": status,
            "run_completed": run_completed,
            "started_at": self.run_started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round((finished_at - self.run_started_at).total_seconds(), 2),
            "last_page_attempted": last_page_attempted,
            "next_page": self.current_page,
            "stats": self.stats,
            "records_written": len(self.all_records),
            "visited_urls": len(self.visited_urls),
            "output_dir": self.output_dir,
            "outputs": {
                "json": self.results_json_file if "json" in self.output_formats else None,
                "xlsx": self.results_xlsx_file if "xlsx" in self.output_formats else None,
                "failed_urls": self.failed_file if self.failed_urls else None,
            },
            "config": {
                "start_url": START_URL,
                "total_pages": TOTAL_PAGES,
                "max_depth": MAX_DEPTH,
                "max_retries": MAX_RETRIES,
                "request_delay": REQUEST_DELAY,
                "depth_delay_increment": DEPTH_DELAY_INCREMENT,
                "max_records": self.max_records,
            },
        }
        with open(self.run_stats_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def scrape_all(self) -> None:
        self._log_info("Proto-Indo-European StarlingDB Scraper")
        self._log_info(f"{'=' * 60}")
        self._log_info(f"Start URL: {START_URL}")
        self._log_info(f"Total: {TOTAL_RECORDS} records, {TOTAL_PAGES} pages")
        self._log_info("Strategy: Main + all sub-entries (nostret capture-only)")
        self._log_info(f"Max depth: {MAX_DEPTH}")
        self._log_info(f"Starting from page: {self.current_page}")
        if self.max_records is not None:
            self._log_info(f"Max records this run: {self.max_records}")
        self._log_info(f"{'=' * 60}")

        run_completed = False
        stopped_by_max_records = False
        last_page_attempted = self.current_page

        for page in range(self.current_page, TOTAL_PAGES + 1):
            self.current_page = page
            last_page_attempted = page
            remaining: Optional[int] = None
            if isinstance(self.max_records, int):
                remaining = self.max_records - len(self.all_records)
                if remaining <= 0:
                    stopped_by_max_records = True
                    break

            page_records = self.scrape_page(page, max_page_records=remaining)
            if page_records is None:
                self.page_failure_detected = True
                self._log_error(
                    f"Failed to fetch page {page}. Preserving checkpoint at current page."
                )
                self._save_checkpoint()
                break
            self.all_records.extend(page_records)

            self._save_results_json()
            self.current_page = page + 1
            self._save_checkpoint()

            pct = (page / TOTAL_PAGES) * 100
            self._log_info(
                f"\n--- Page {page}/{TOTAL_PAGES} ({pct:.1f}%) | "
                f"{len(self.all_records)} records | "
                f"{self.stats['sub_entries_fetched']} sub-entries | "
                f"{len(self.visited_urls)} URLs visited ---"
            )

            if isinstance(self.max_records, int) and len(self.all_records) >= self.max_records:
                self._log_info(
                    f"Reached max records limit ({self.max_records}); stopping early."
                )
                stopped_by_max_records = True
                break

            time.sleep(REQUEST_DELAY)
        else:
            run_completed = True

        self._save_failed_urls()
        if "xlsx" in self.output_formats:
            self._save_results_xlsx()

        if run_completed and not self.page_failure_detected and not stopped_by_max_records and os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            self._log_info("Run completed successfully; removed checkpoint file.")
        else:
            self._log_info(
                f"Checkpoint retained at {self.checkpoint_file} for resumability."
            )

        status = "completed" if run_completed else "partial"
        if self.page_failure_detected:
            status = "failed"
        self._save_run_summary(status, run_completed, last_page_attempted)

        self._log_info(f"\n{'=' * 60}")
        self._log_info("RUN SUMMARY")
        self._log_info(f"{'=' * 60}")
        self._log_info(f"Status: {status}")
        self._log_info(f"Main records: {self.stats['main_records']}")
        self._log_info(f"Sub-entries fetched: {self.stats['sub_entries_fetched']}")
        self._log_info(f"Circular refs skipped: {self.stats['circular_refs_skipped']}")
        self._log_info(f"Depth cap hits: {self.stats['depth_cap_hits']}")
        self._log_info(f"Errors: {self.stats['errors']}")
        self._log_info(f"Total URLs visited: {len(self.visited_urls)}")
        if "json" in self.output_formats:
            self._log_info(f"JSON Output: {self.results_json_file}")
        if "xlsx" in self.output_formats:
            self._log_info(f"XLSX Output: {self.results_xlsx_file}")
        if self.failed_urls:
            self._log_info(f"Failed URL report: {self.failed_file}")
        self._log_info(f"Run summary JSON: {self.run_stats_file}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Proto-IE StarlingDB records with checkpoint support."
    )
    parser.add_argument(
        "start_page", nargs="?", type=int, default=1,
        help="Page to start scraping from (default: 1).",
    )
    parser.add_argument(
        "--output-dir", default=OUTPUT_DIR,
        help=f"Directory for outputs/checkpoint (default: {OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--output-formats", nargs="+", choices=["json", "xlsx"],
        default=["json", "xlsx"],
        help="Output formats to export at completion (default: json xlsx).",
    )
    parser.add_argument(
        "--session-name", default="piet_scrape",
        help="Session name when generating a detached screen command.",
    )
    parser.add_argument(
        "--max-records", type=int, default=None,
        help="Stop after scraping this many main records (for smoke tests).",
    )
    parser.add_argument(
        "--print-screen-command", action="store_true",
        help="Print a detached Linux `screen` command for this run and exit (use nohup if screen is unavailable).",
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
        f"mkdir -p {shlex.quote(output_dir)} && "
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
        print(build_screen_command(
            script_path=__file__,
            start_page=cli_args.start_page,
            output_dir=cli_args.output_dir,
            output_formats=cli_args.output_formats,
            session_name=cli_args.session_name,
            max_records=cli_args.max_records,
        ))
        sys.exit(0)

    scraper = PIETScraper(
        start_page=cli_args.start_page,
        output_dir=cli_args.output_dir,
        output_formats=set(cli_args.output_formats),
        max_records=cli_args.max_records,
    )
    scraper.scrape_all()
