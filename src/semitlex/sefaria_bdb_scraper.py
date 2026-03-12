"""Production-grade scraper for Sefaria BDB using official APIs."""

from __future__ import annotations

import csv
import json
import random
import re
import time
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from sefaria_config import API_CONFIG, HEADERS, SCRAPER_CONFIG
from sefaria_words_client import SefariaWordsClient


def utc_now_iso() -> str:
    """UTC timestamp helper."""
    return datetime.now(timezone.utc).isoformat()


def normalize_whitespace(value: str) -> str:
    """Normalize whitespace for stable plain-text output."""
    return re.sub(r"\s+", " ", value).strip()


def ref_to_headword(ref_value: str) -> str:
    """Extract headword from refs that look like 'BDB, ...'."""
    if "," not in ref_value:
        return ref_value.strip()
    _, rhs = ref_value.split(",", 1)
    return rhs.strip()


def parse_segments_to_plain_text(segments: Sequence[str]) -> str:
    """Convert HTML text segments from /api/texts into plain text."""
    plain_segments: List[str] = []
    for segment in segments:
        soup = BeautifulSoup(segment, "html.parser")
        plain_segments.append(normalize_whitespace(soup.get_text(separator=" ")))
    return "\n\n".join([segment for segment in plain_segments if segment])


def extract_citations_from_segments(segments: Sequence[str]) -> List[str]:
    """Extract citation refs from anchor tags `data-ref`."""
    citations: List[str] = []
    seen: Set[str] = set()
    for segment in segments:
        soup = BeautifulSoup(segment, "html.parser")
        for anchor in soup.find_all("a"):
            ref = anchor.get("data-ref") if isinstance(anchor, Tag) else None
            if isinstance(ref, str) and ref and ref not in seen:
                seen.add(ref)
                citations.append(ref)
    return citations


def flatten_senses(
    senses: Sequence[Dict[str, Any]],
    headword: str,
    ref_value: str,
    entry_common: Dict[str, Any],
    path: str = "",
) -> List[Dict[str, Any]]:
    """Recursively flatten nested BDB senses into semantic rows."""
    rows: List[Dict[str, Any]] = []
    for idx, sense in enumerate(senses, start=1):
        if not isinstance(sense, dict):
            continue
        current_path = f"{path}.{idx}" if path else str(idx)
        grammar = sense.get("grammar", {})
        grammar_form = ""
        if isinstance(grammar, dict):
            grammar_form = str(
                grammar.get("verbal_stem")
                or grammar.get("binyan_form")
                or grammar.get("morphology")
                or ""
            )
        form = str(sense.get("form") or grammar_form or "")

        row = {
            "headword": headword,
            "ref": ref_value,
            "sense_path": current_path,
            "ordinal": str(entry_common.get("ordinal", "")),
            "morphology": str(
                sense.get("morphology")
                or entry_common.get("morphology", "")
                or entry_common.get("words_morphology", "")
            ),
            "definition": str(sense.get("definition", "")),
            "form": form,
            "pre_num": str(sense.get("pre_num", "")),
            "num": str(sense.get("num", "")),
            "note": str(sense.get("note", "")),
            "occurences": str(
                sense.get("occurences") or entry_common.get("occurrences", "")
            ),
            "strong_numbers": entry_common.get("strong_numbers", ""),
            "twot": entry_common.get("twot", ""),
            "gk": entry_common.get("gk", ""),
            "rid": str(entry_common.get("rid", "")),
            "prev": str(entry_common.get("prev", "")),
            "next": str(entry_common.get("next", "")),
            "citations": entry_common.get("citations", ""),
            "text_plain": entry_common.get("text_plain", ""),
        }
        rows.append(row)

        nested = sense.get("senses")
        if isinstance(nested, list) and nested:
            rows.extend(flatten_senses(nested, headword, ref_value, entry_common, current_path))
    return rows


@dataclass
class FailedRef:
    """Tracks failed reference fetches."""

    ref: str
    timestamp: str
    status_code: str
    error: str


class SefariaBdbScraper:
    """Scrapes BDB entries via the Sefaria Text API and enriches via Words API."""

    def __init__(
        self,
        output_dir: Optional[str] = None,
        subset_size: Optional[int] = None,
        full_run: bool = False,
        request_delay_seconds: Optional[float] = None,
    ):
        self.output_dir = Path(output_dir or SCRAPER_CONFIG["default_output_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.words_client = SefariaWordsClient(self.session)

        self.base_url = API_CONFIG["base_url"]
        self.start_ref = SCRAPER_CONFIG["bdb_start_ref"]
        self.end_ref = SCRAPER_CONFIG["bdb_end_ref"]
        self.timeout_seconds = SCRAPER_CONFIG["timeout_seconds"]
        self.max_retries = SCRAPER_CONFIG["max_retries"]
        self.retry_base_wait_seconds = SCRAPER_CONFIG["retry_base_wait_seconds"]
        self.backoff_multiplier = SCRAPER_CONFIG["backoff_multiplier"]
        self.request_delay_seconds = (
            request_delay_seconds
            if request_delay_seconds is not None
            else SCRAPER_CONFIG["request_delay_seconds"]
        )
        if full_run:
            self.subset_size = None
        else:
            self.subset_size = (
                subset_size if subset_size is not None else SCRAPER_CONFIG["subset_size"]
            )

        self.checkpoint_path = self.output_dir / SCRAPER_CONFIG["checkpoint_filename"]
        self.output_json_path = self.output_dir / SCRAPER_CONFIG["output_json_filename"]
        self.output_xlsx_path = self.output_dir / SCRAPER_CONFIG["output_xlsx_filename"]
        self.failed_refs_path = self.output_dir / SCRAPER_CONFIG["failed_refs_filename"]
        self.run_metadata_path = self.output_dir / SCRAPER_CONFIG["run_metadata_filename"]

        self.entries: List[Dict[str, Any]] = []
        self.seen_refs: Set[str] = set()
        self.failed_refs: List[FailedRef] = []
        self.current_ref: Optional[str] = self.start_ref
        self.started_at = utc_now_iso()
        self.index_payload: Optional[Dict[str, Any]] = None

        self.load_checkpoint()

    def _api_get_json(self, url: str) -> Dict[str, Any]:
        """GET JSON with retry/backoff."""
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    return payload
                return {"payload": payload}
            except requests.RequestException as err:
                last_err = err
                if attempt == self.max_retries - 1:
                    raise
                wait = self.retry_base_wait_seconds * (self.backoff_multiplier**attempt)
                jitter = random.uniform(0.1, 0.6)
                time.sleep(wait + jitter)
        raise RuntimeError(f"Unexpected request retry flow for URL: {url}") from last_err

    def ping_index(self) -> Dict[str, Any]:
        """Ping BDB index endpoint and cache result."""
        index_url = f"{self.base_url}{API_CONFIG['index_endpoint']}"
        payload = self._api_get_json(index_url)
        self.index_payload = payload
        return payload

    def fetch_text_entry(self, ref_value: str) -> Dict[str, Any]:
        """Fetch one BDB text entry by ref."""
        encoded_ref = quote(ref_value, safe="")
        endpoint = API_CONFIG["texts_endpoint_template"].format(ref=encoded_ref)
        url = f"{self.base_url}{endpoint}"
        return self._api_get_json(url)

    def build_entry_record(self, text_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create unified entry record from text payload + words enrichment."""
        ref_value = str(text_payload.get("ref", "")).strip()
        headword = ref_to_headword(ref_value)

        text_segments = text_payload.get("text", [])
        if not isinstance(text_segments, list):
            text_segments = []
        html_segments = [str(segment) for segment in text_segments]
        text_plain = parse_segments_to_plain_text(html_segments)
        citations = extract_citations_from_segments(html_segments)

        words_bdb: Optional[Dict[str, Any]] = None
        words_error: Optional[str] = None
        try:
            words_bdb = self.words_client.fetch_bdb_entry(headword)
        except requests.RequestException as err:
            words_error = str(err)

        version_title = text_payload.get("versionTitle", "")
        version_source = text_payload.get("versionSource", "")

        return {
            "ref": ref_value,
            "heRef": text_payload.get("heRef", ""),
            "headword": headword,
            "text": text_plain,
            "text_html_segments": html_segments,
            "text_html": "\n".join(html_segments),
            "citations": citations,
            "prev": text_payload.get("prev"),
            "next": text_payload.get("next"),
            "sectionRef": text_payload.get("sectionRef"),
            "versionTitle": version_title,
            "versionSource": version_source,
            "words_api": words_bdb,
            "words_api_error": words_error,
        }

    def flatten_for_xlsx(self) -> pd.DataFrame:
        """Flatten entries into lexicon-oriented rows suitable for xlsx."""
        rows: List[Dict[str, Any]] = []
        for entry in self.entries:
            ref_value = str(entry.get("ref", ""))
            headword = str(entry.get("headword", ""))
            words_api = entry.get("words_api")
            citations = "; ".join(entry.get("citations", []))
            text_plain = str(entry.get("text", ""))

            if not isinstance(words_api, dict):
                rows.append(
                    {
                        "headword": headword,
                        "ref": ref_value,
                        "sense_path": "",
                        "ordinal": "",
                        "morphology": "",
                        "definition": text_plain,
                        "form": "",
                        "pre_num": "",
                        "num": "",
                        "note": "",
                        "occurences": "",
                        "strong_numbers": "",
                        "twot": "",
                        "gk": "",
                        "rid": "",
                        "prev": str(entry.get("prev", "")),
                        "next": str(entry.get("next", "")),
                        "citations": citations,
                        "text_plain": text_plain,
                    }
                )
                continue

            strong_numbers = words_api.get("strong_numbers", [])
            twot = words_api.get("TWOT", [])
            gk = words_api.get("GK", [])

            entry_common = {
                "ordinal": words_api.get("ordinal", ""),
                "morphology": words_api.get("morphology", ""),
                "words_morphology": (
                    words_api.get("content", {}).get("morphology", "")
                    if isinstance(words_api.get("content"), dict)
                    else ""
                ),
                "occurrences": words_api.get("occurrences", ""),
                "strong_numbers": "; ".join([str(v) for v in strong_numbers]),
                "twot": "; ".join([str(v) for v in twot]),
                "gk": "; ".join([str(v) for v in gk]),
                "rid": words_api.get("rid", ""),
                "prev": entry.get("prev", ""),
                "next": entry.get("next", ""),
                "citations": citations,
                "text_plain": text_plain,
            }

            content = words_api.get("content", {})
            senses = content.get("senses", []) if isinstance(content, dict) else []
            if isinstance(senses, list) and senses:
                rows.extend(flatten_senses(senses, headword, ref_value, entry_common))
            else:
                rows.append(
                    {
                        "headword": headword,
                        "ref": ref_value,
                        "sense_path": "",
                        "ordinal": str(entry_common["ordinal"]),
                        "morphology": str(entry_common["words_morphology"]),
                        "definition": text_plain,
                        "form": "",
                        "pre_num": "",
                        "num": "",
                        "note": "",
                        "occurences": str(entry_common["occurrences"]),
                        "strong_numbers": str(entry_common["strong_numbers"]),
                        "twot": str(entry_common["twot"]),
                        "gk": str(entry_common["gk"]),
                        "rid": str(entry_common["rid"]),
                        "prev": str(entry_common["prev"]),
                        "next": str(entry_common["next"]),
                        "citations": citations,
                        "text_plain": text_plain,
                    }
                )

        return pd.DataFrame(rows)

    def save_failed_refs(self) -> None:
        """Write failed refs to CSV."""
        if not self.failed_refs:
            return
        with open(self.failed_refs_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=["ref", "timestamp", "status_code", "error"]
            )
            writer.writeheader()
            for failed in self.failed_refs:
                writer.writerow(
                    {
                        "ref": failed.ref,
                        "timestamp": failed.timestamp,
                        "status_code": failed.status_code,
                        "error": failed.error,
                    }
                )

    def save_run_metadata(self) -> None:
        """Persist run metadata in a small JSON sidecar."""
        metadata = {
            "started_at": self.started_at,
            "finished_at": utc_now_iso(),
            "subset_size": self.subset_size,
            "request_delay_seconds": self.request_delay_seconds,
            "total_entries": len(self.entries),
            "failed_refs": len(self.failed_refs),
            "start_ref": self.start_ref,
            "end_ref": self.end_ref,
            "current_ref": self.current_ref,
        }
        with open(self.run_metadata_path, "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, ensure_ascii=False, indent=2)

    def save_outputs(self) -> None:
        """Write JSON + XLSX + failed refs + metadata."""
        payload = {
            "metadata": {
                "source": "Sefaria API",
                "lexicon": "BDB",
                "scraped_at": utc_now_iso(),
                "started_at": self.started_at,
                "total_entries": len(self.entries),
                "subset_size": self.subset_size,
                "request_delay_seconds": self.request_delay_seconds,
                "version_title": (
                    self.entries[-1].get("versionTitle", "") if self.entries else ""
                ),
                "version_source": (
                    self.entries[-1].get("versionSource", "") if self.entries else ""
                ),
            },
            "entries": self.entries,
        }
        with open(self.output_json_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

        flattened = self.flatten_for_xlsx()
        flattened.to_excel(self.output_xlsx_path, index=False)

        self.save_failed_refs()
        self.save_run_metadata()

    def save_checkpoint(self) -> None:
        """Persist restart checkpoint."""
        checkpoint = {
            "current_ref": self.current_ref,
            "entries_count": len(self.entries),
            "seen_refs": sorted(self.seen_refs),
            "started_at": self.started_at,
            "subset_size": self.subset_size,
            "request_delay_seconds": self.request_delay_seconds,
        }
        with open(self.checkpoint_path, "w", encoding="utf-8") as handle:
            json.dump(checkpoint, handle, ensure_ascii=False, indent=2)

    def load_checkpoint(self) -> None:
        """Load checkpoint and previous output if present."""
        if not self.checkpoint_path.exists():
            return
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as handle:
                checkpoint = json.load(handle)
            if isinstance(checkpoint, dict):
                self.current_ref = checkpoint.get("current_ref") or self.start_ref
                seen_refs = checkpoint.get("seen_refs", [])
                if isinstance(seen_refs, list):
                    self.seen_refs = {str(ref) for ref in seen_refs}
                self.started_at = str(checkpoint.get("started_at", self.started_at))
                # Restore subset_size only when the constructor left it at the
                # config default (i.e. the caller did not explicitly pass one).
                # Use a sentinel so we can distinguish "key absent" from "key=null"
                # (null means the original run was a full run → restore as None).
                _MISSING = object()
                saved_subset = checkpoint.get("subset_size", _MISSING)
                if self.subset_size == SCRAPER_CONFIG["subset_size"] and saved_subset is not _MISSING:
                    self.subset_size = int(saved_subset) if isinstance(saved_subset, int) else None

            if self.output_json_path.exists():
                with open(self.output_json_path, "r", encoding="utf-8") as handle:
                    output_payload = json.load(handle)
                entries = output_payload.get("entries", [])
                if isinstance(entries, list):
                    self.entries = entries
                    for entry in entries:
                        if isinstance(entry, dict):
                            ref_value = entry.get("ref")
                            if isinstance(ref_value, str) and ref_value:
                                self.seen_refs.add(ref_value)
        except (OSError, json.JSONDecodeError):
            # Ignore malformed checkpoint and start fresh.
            self.current_ref = self.start_ref
            self.entries = []
            self.seen_refs = set()

    def should_stop_subset(self) -> bool:
        """Return True when subset limit is reached."""
        return self.subset_size is not None and len(self.entries) >= self.subset_size

    def scrape(self) -> Dict[str, Any]:
        """Run scrape loop from current_ref, following `next` links."""
        self.ping_index()
        if not self.current_ref:
            self.current_ref = self.start_ref

        while self.current_ref:
            if self.should_stop_subset():
                break
            if self.current_ref in self.seen_refs:
                break

            try:
                text_payload = self.fetch_text_entry(self.current_ref)
            except requests.RequestException as err:
                self.failed_refs.append(
                    FailedRef(
                        ref=self.current_ref,
                        timestamp=utc_now_iso(),
                        status_code=getattr(getattr(err, "response", None), "status_code", ""),
                        error=str(err),
                    )
                )
                print(
                    f"[WARN] Failed to fetch ref '{self.current_ref}' after retries: {err}. "
                    "Saving checkpoint — re-run to resume from this ref."
                )
                self.save_outputs()
                self.save_checkpoint()
                break

            entry_record = self.build_entry_record(text_payload)
            self.entries.append(entry_record)
            self.seen_refs.add(self.current_ref)

            next_ref = text_payload.get("next")
            entry_ref = normalize_whitespace(str(entry_record.get("ref", "")))
            if entry_ref == normalize_whitespace(self.end_ref):
                self.current_ref = None
                break
            self.current_ref = str(next_ref) if isinstance(next_ref, str) else None

            if self.should_stop_subset():
                break

            if len(self.entries) % SCRAPER_CONFIG["checkpoint_every_n_entries"] == 0:
                self.save_outputs()
                self.save_checkpoint()

            time.sleep(self.request_delay_seconds)

        else:
            # Loop exited because current_ref became None (no `next` link) without
            # ever matching end_ref — warn so misconfigured end_ref is obvious.
            if self.current_ref is None and self.entries:
                last_ref = self.entries[-1].get("ref", "")
                if normalize_whitespace(last_ref) != normalize_whitespace(self.end_ref):
                    print(
                        f"[WARN] Traversal ended (no `next` link) but end_ref "
                        f"'{self.end_ref}' was never matched. Last ref: '{last_ref}'."
                    )

        self.save_outputs()
        self.save_checkpoint()
        return {
            "entries": len(self.entries),
            "failed_refs": len(self.failed_refs),
            "output_json": str(self.output_json_path),
            "output_xlsx": str(self.output_xlsx_path),
            "failed_csv": str(self.failed_refs_path),
            "checkpoint": str(self.checkpoint_path),
            "current_ref": self.current_ref,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sefaria BDB via API")
    parser.add_argument(
        "--subset-size",
        type=int,
        default=None,
        help="Override subset size for testing (e.g. 3).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full scrape (equivalent to subset_size=None).",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=None,
        help="Override per-entry delay in seconds.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for JSON/XLSX/checkpoint files.",
    )
    args = parser.parse_args()

    subset_size: Optional[int]
    if args.full:
        subset_size = None
    else:
        subset_size = args.subset_size

    scraper = SefariaBdbScraper(
        output_dir=args.output_dir,
        subset_size=subset_size,
        full_run=args.full,
        request_delay_seconds=args.request_delay,
    )
    summary = scraper.scrape()
    # Keep Windows terminals safe by avoiding non-ASCII output encoding issues.
    print(json.dumps(summary, indent=2))
