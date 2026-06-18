"""
Triage / prioritize DED <-> Starling mismatch rows for manual review.

Why this exists
----------------
A full validation run leaves thousands of mismatch rows in
``tree_validation_output/tree_validation_results.csv``. Scrolling through
them one at a time in Excel doesn't surface the signal that matters most: a
single parser bug often fails many rows at once. This script groups the
backlog so a review session can spot that signal quickly:

  - One DED# with many failing languages -> usually a local/HTML-specific
    bug confined to that paragraph (e.g. an attestation hidden inside an
    outer <b> block).
  - One language repeated across many DED#s -> usually a systemic bug (e.g.
    the historical Gondi inline-sigil bug, which fired 2-5 rows each across
    80+ different DED numbers -- unremarkable DED-by-DED, huge in aggregate).

The default `--primary ded_language` cross-tab surfaces both at once.

Relation to other scripts in this directory
---------------------------------------------
- ``starling_tree_validator.py``
      Produces ``tree_validation_results.csv`` and
      ``validation_audit_report.xlsx``. This script re-derives the same
      "is this row an issue" predicate from ``build_validation_audit_frames``
      by reusing its row index, rather than its column-trimmed output, so
      the triage queue keeps context columns (full Proto lineage, Burrow
      paragraph text) that the audit report's `row_issues` sheet drops.
- ``review_ledger.py``
      Cross-session record of which (DED, language) pairs have already been
      triaged. This script anti-joins those out of the queue by default.

Usage
-----
    python triage_mismatches.py tree_validation_output/tree_validation_results.csv \\
        --primary ded_language --exclude-reviewed \\
        --ledger data/dravidian/burrow_ded/review_ledger.json \\
        --output tree_validation_output/triage_queue.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from burrow_entry_parser import BurrowEntryParser
from review_ledger import load_ledger, reviewed_keys
from starling_tree_validator import build_validation_audit_frames

_DED_COL = "Validation DED #"
_LANG_COL = "Starling language"
_BRANCH_COL = "Validation branch label"
_NOTE_COL = "Validation note"

_entry_parser = BurrowEntryParser()


def _clean_ded_series(series: pd.Series) -> pd.Series:
    """Normalize the DED column to plain integer strings: 47.0 / '0047' -> '47'."""
    return series.map(lambda v: _entry_parser.clean_ded_number(str(v)) if pd.notna(v) else "")


def load_issue_rows(results_path: str, source: str = "results") -> pd.DataFrame:
    """Load the mismatch backlog with full row context.

    source="results": `results_path` is `tree_validation_results.csv` (the
        full row dump from `starling_tree_validator.py`). The issue
        predicate is re-derived via `build_validation_audit_frames` --
        Match starts with "No" / "Language only", or a meaning mismatch --
        but rows are sliced from the *original* full-column dataframe using
        that function's row index, not its trimmed `row_issues` output, so
        columns like "Proto lineage" and "Burrow paragraph text" survive.
    source="audit": `results_path` is `validation_audit_report.xlsx`; reads
        its already-filtered (and column-trimmed) `row_issues` sheet
        directly, no re-derivation needed.
    """
    if source == "audit":
        return pd.read_excel(results_path, sheet_name="row_issues")

    if source != "results":
        raise ValueError(f"Unknown source {source!r}; expected 'results' or 'audit'")

    df = pd.read_csv(results_path, encoding="utf-8-sig")
    frames = build_validation_audit_frames(df)
    issue_index = frames["row_issues"].index
    return df.loc[issue_index].copy()


def _grouped(df: pd.DataFrame, group_cols: list[str], ded_in_group: bool) -> pd.DataFrame:
    work = df.copy()
    if ded_in_group:
        work["__ded"] = _clean_ded_series(work[_DED_COL])
        group_cols = ["__ded" if c == _DED_COL else c for c in group_cols]

    grouped = (
        work.groupby(group_cols, dropna=False)
        .agg(
            row_count=(_NOTE_COL, "size"),
            example_note=(_NOTE_COL, lambda s: next((n for n in s if isinstance(n, str) and n), "")),
        )
        .reset_index()
        .sort_values("row_count", ascending=False)
        .reset_index(drop=True)
    )
    if ded_in_group:
        grouped.rename(columns={"__ded": _DED_COL}, inplace=True)
    return grouped


def group_by_ded(df: pd.DataFrame) -> pd.DataFrame:
    """Rollup by DED number alone -- surfaces local/HTML-specific bugs."""
    return _grouped(df, [_DED_COL], ded_in_group=True)


def group_by_language(df: pd.DataFrame) -> pd.DataFrame:
    """Rollup by Starling language alone -- surfaces systemic bugs."""
    return _grouped(df, [_LANG_COL], ded_in_group=False)


def group_by_ded_and_language(df: pd.DataFrame) -> pd.DataFrame:
    """Cross-tab of DED# x language -- surfaces both bug shapes at once."""
    return _grouped(df, [_DED_COL, _LANG_COL], ded_in_group=True)


def build_priority_queue(
    df: pd.DataFrame,
    primary: str = "ded_language",
    min_rows: int = 1,
    language_filter: Optional[str] = None,
    branch_filter: Optional[str] = None,
    exclude_reviewed: bool = True,
    ledger_path: Optional[str] = None,
) -> pd.DataFrame:
    """Build a ranked review queue from issue rows.

    `primary` selects the grouping view: "ded", "language", or
    "ded_language" (the default cross-tab). `language_filter` /
    `branch_filter` are case-insensitive substring filters applied to
    "Starling language" / "Validation branch label" before grouping.
    `exclude_reviewed` anti-joins (DED, language) pairs already recorded in
    the ledger at `ledger_path` (required when excluding).
    """
    work = df.copy()

    if language_filter:
        work = work[work[_LANG_COL].astype(str).str.contains(language_filter, case=False, na=False)]
    if branch_filter:
        work = work[work[_BRANCH_COL].astype(str).str.contains(branch_filter, case=False, na=False)]

    if exclude_reviewed:
        if not ledger_path:
            raise ValueError("ledger_path is required when exclude_reviewed=True")
        ledger = load_ledger(ledger_path)
        reviewed = reviewed_keys(ledger)
        if reviewed:
            ded_clean = _clean_ded_series(work[_DED_COL])
            whole_ded_reviewed = {ded for ded, lang in reviewed if lang is None}
            lang_pair_reviewed = {(ded, lang) for ded, lang in reviewed if lang is not None}

            is_reviewed_row = ded_clean.isin(whole_ded_reviewed) | pd.Series(
                list(zip(ded_clean, work[_LANG_COL])), index=work.index
            ).isin(lang_pair_reviewed)
            work = work[~is_reviewed_row]

    if primary == "ded":
        queue = group_by_ded(work)
    elif primary == "language":
        queue = group_by_language(work)
    elif primary == "ded_language":
        queue = group_by_ded_and_language(work)
    else:
        raise ValueError(f"Unknown primary {primary!r}; expected 'ded', 'language', or 'ded_language'")

    if min_rows > 1:
        queue = queue[queue["row_count"] >= min_rows].reset_index(drop=True)

    return queue


def write_queue(df: pd.DataFrame, output_path: str, fmt: str = "csv") -> None:
    """Write the queue. `fmt` is "csv", "xlsx", or "json"."""
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
    elif fmt == "xlsx":
        df.to_excel(out_path, index=False, engine="openpyxl")
    elif fmt == "json":
        df.to_json(out_path, orient="records", indent=2, force_ascii=False)
    else:
        raise ValueError(f"Unknown format {fmt!r}; expected 'csv', 'xlsx', or 'json'")


def _infer_format(output_path: str) -> str:
    suffix = Path(output_path).suffix.lower()
    if suffix == ".xlsx":
        return "xlsx"
    if suffix == ".json":
        return "json"
    return "csv"


def main() -> None:
    # Language names carry diacritics (e.g. "Koḍagu"); force UTF-8 stdout so
    # printing the queue preview doesn't crash under the Windows console codepage.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Prioritize DED <-> Starling mismatch rows for manual triage."
    )
    parser.add_argument(
        "results_path",
        help="Path to tree_validation_results.csv (--source results) or "
        "validation_audit_report.xlsx (--source audit)",
    )
    parser.add_argument("--source", choices=["results", "audit"], default="results")
    parser.add_argument("--primary", choices=["ded", "language", "ded_language"], default="ded_language")
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--language", default=None, help="Substring filter on Starling language")
    parser.add_argument("--branch", default=None, help="Substring filter on Validation branch label")

    reviewed_group = parser.add_mutually_exclusive_group()
    reviewed_group.add_argument("--exclude-reviewed", dest="exclude_reviewed", action="store_true")
    reviewed_group.add_argument("--include-reviewed", dest="exclude_reviewed", action="store_false")
    parser.set_defaults(exclude_reviewed=True)

    parser.add_argument("--ledger", default=None, help="Path to review_ledger.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=["csv", "xlsx", "json"], default=None)
    args = parser.parse_args()

    if args.exclude_reviewed and not args.ledger:
        parser.error("--ledger is required unless --include-reviewed is passed")

    df = load_issue_rows(args.results_path, source=args.source)
    print(f"Loaded {len(df)} issue rows from {args.results_path} (source={args.source})")

    queue = build_priority_queue(
        df,
        primary=args.primary,
        min_rows=args.min_rows,
        language_filter=args.language,
        branch_filter=args.branch,
        exclude_reviewed=args.exclude_reviewed,
        ledger_path=args.ledger,
    )

    fmt = args.format or _infer_format(args.output)
    write_queue(queue, args.output, fmt=fmt)
    print(f"Wrote {len(queue)} groups (primary={args.primary}) to {args.output}")

    if len(queue):
        print("\nTop of queue:")
        print(queue.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
