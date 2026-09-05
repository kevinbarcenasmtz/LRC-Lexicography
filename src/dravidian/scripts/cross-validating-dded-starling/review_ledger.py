"""
Cross-session review-decision ledger for the DED <-> Starling triage workflow.

Why this exists
----------------
The mismatch backlog in ``data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.csv``
gets reviewed across scattered sessions over months. Without persistence, a
fresh ``triage_mismatches.py`` run would resurface DED numbers that were
already triaged and resolved (either fixed in the parser, or confirmed as a
genuine lexicographic divergence worth keeping for the thesis). This module
is the single read/write surface for that persistent record.

The ledger is a flat JSON file (``data/dravidian/burrow_ded/review_ledger.json``
by convention, co-located with the corpus it annotates) keyed by DED number,
with an optional per-language sub-key for finer-grained decisions:

    {
      "schema_version": 1,
      "entries": {
        "63": {
          "status": "parser_bug_fixed",
          "note": "Pattern E recovered the Ko. attestation; reparsed 2026-06-18.",
          "reviewed_at": "2026-06-18T14:02:00",
          "reviewed_by": "unspecified",
          "languages": {
            "Maria Gondi": {"status": "genuine_divergence", "note": "...", ...}
          }
        }
      }
    }

A whole-DED decision (no ``language``) is treated as settling every row for
that DED number, regardless of language. A language-scoped decision only
settles that one (DED, language) pair.

Usage (CLI)
-----------
    python review_ledger.py record --ded 63 --status parser_bug_fixed \\
        --note "Pattern E recovered the Ko. attestation" \\
        --ledger data/dravidian/burrow_ded/review_ledger.json

    python review_ledger.py record --ded 63 --language "Maria Gondi" \\
        --status genuine_divergence --note "Burrow drops final vowel" \\
        --ledger data/dravidian/burrow_ded/review_ledger.json

    python review_ledger.py status --ded 63 \\
        --ledger data/dravidian/burrow_ded/review_ledger.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from textnorm import clean_ded_number

_SCHEMA_VERSION = 1

# Status vocabulary (plan section D). Kept as a frozenset rather than an Enum
# since the ledger is plain JSON and callers (the CLI, the triage skill)
# pass these around as strings.
STATUS_VALUES = frozenset(
    {
        "parser_bug_fixed",
        "genuine_divergence",
        "needs_more_info",
        "not_a_bug_wontfix",
    }
)

def _clean_ded(ded_number: Any) -> str:
    """Normalize a DED number to the validator's key semantics.

    Uses the shared validation-layer cleaner so ledger keys align with the
    validator's DED indexing ("0047" / 47.0 / "4896(a)" all collapse to the
    key the validator emits). When the cleaner yields None (missing value, or
    Starling's literal-"0" no-correspondence sentinel) fall back to the
    stripped literal so historical/archival keys -- notably the "0" entry
    documenting the sentinel bug itself -- stay reachable instead of
    collapsing to "".
    """
    cleaned = clean_ded_number(ded_number)
    return cleaned if cleaned is not None else str(ded_number).strip()


def load_ledger(path: str | Path) -> Dict[str, Any]:
    """Load the ledger, returning an empty-but-valid structure if it doesn't exist yet."""
    p = Path(path)
    if not p.exists():
        return {"schema_version": _SCHEMA_VERSION, "entries": {}}
    with p.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    data.setdefault("schema_version", _SCHEMA_VERSION)
    data.setdefault("entries", {})
    return data


def save_ledger(ledger: Dict[str, Any], path: str | Path) -> None:
    """Write the ledger via temp-file-then-replace so a crash mid-write can't corrupt it."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(p.parent), prefix=f".{p.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="\n") as f:
            json.dump(ledger, f, ensure_ascii=False, indent=2)
        os.replace(tmp_name, p)
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def record_decision(
    ledger: Dict[str, Any],
    ded_number: Any,
    status: str,
    note: str = "",
    language: Optional[str] = None,
    reviewed_by: str = "unspecified",
) -> Dict[str, Any]:
    """Record a review decision in-place on `ledger` and return it.

    A whole-DED decision (language=None) settles every row for that DED
    number. A language-scoped decision settles only that one (DED, language)
    pair, nested under "languages".
    """
    if status not in STATUS_VALUES:
        raise ValueError(f"Unknown status {status!r}; expected one of {sorted(STATUS_VALUES)}")

    ded_key = _clean_ded(ded_number)
    entries = ledger.setdefault("entries", {})
    entry = entries.setdefault(ded_key, {})

    record = {
        "status": status,
        "note": note,
        "reviewed_at": datetime.now().isoformat(timespec="seconds"),
        "reviewed_by": reviewed_by,
    }

    if language:
        languages = entry.setdefault("languages", {})
        languages[language] = record
    else:
        entry.update(record)

    return ledger


def is_reviewed(ledger: Dict[str, Any], ded_number: Any, language: Optional[str] = None) -> bool:
    """True if this (DED, language) pair has a recorded decision.

    A whole-DED decision counts as reviewed for every language under that
    DED, since it settled the entry as a whole.
    """
    ded_key = _clean_ded(ded_number)
    entry = ledger.get("entries", {}).get(ded_key)
    if not entry:
        return False
    if "status" in entry:
        return True
    if language:
        return language in entry.get("languages", {})
    return False


def reviewed_keys(ledger: Dict[str, Any]) -> Set[Tuple[str, Optional[str]]]:
    """All reviewed (ded, language) pairs, used to anti-join the triage queue.

    A whole-DED decision yields (ded, None), which callers should treat as
    matching any row for that DED regardless of language. A language-scoped
    decision yields (ded, language).
    """
    keys: Set[Tuple[str, Optional[str]]] = set()
    for ded_key, entry in ledger.get("entries", {}).items():
        if "status" in entry:
            keys.add((ded_key, None))
        for language in entry.get("languages", {}):
            keys.add((ded_key, language))
    return keys


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _cli_record(args: argparse.Namespace) -> None:
    ledger = load_ledger(args.ledger)
    record_decision(
        ledger,
        ded_number=args.ded,
        status=args.status,
        note=args.note or "",
        language=args.language,
        reviewed_by=args.reviewed_by,
    )
    save_ledger(ledger, args.ledger)

    ded_key = _clean_ded(args.ded)
    scope = f"DED {ded_key} / {args.language}" if args.language else f"DED {ded_key}"
    print(f"Recorded: {scope} -> {args.status}")
    print(f"Ledger: {args.ledger}")


def _cli_status(args: argparse.Namespace) -> None:
    ledger = load_ledger(args.ledger)
    ded_key = _clean_ded(args.ded)
    entry = ledger.get("entries", {}).get(ded_key)
    if not entry:
        print(f"DED {ded_key}: not reviewed")
        return

    print(f"DED {ded_key}:")
    top_level = {k: v for k, v in entry.items() if k != "languages"}
    if top_level:
        print(f"  status:       {top_level.get('status', '')}")
        print(f"  note:         {top_level.get('note', '')}")
        print(f"  reviewed_at:  {top_level.get('reviewed_at', '')}")
        print(f"  reviewed_by:  {top_level.get('reviewed_by', '')}")
    else:
        print("  (no whole-DED decision)")

    languages = entry.get("languages", {})
    if languages:
        print("  languages:")
        for lang, rec in languages.items():
            print(f"    {lang}: {rec.get('status', '')} ({rec.get('reviewed_at', '')})")
            if rec.get("note"):
                print(f"      note: {rec['note']}")


def main() -> None:
    # Language abbreviations / Starling dialect names contain diacritics
    # (e.g. "Koḍ.", "Mandla Gondi (Williamson)"); force UTF-8 stdout so this
    # doesn't crash under the default Windows console codepage.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Record or query DED review decisions in the cross-session triage ledger."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    record_p = sub.add_parser(
        "record", help="Record a review decision for a DED number (optionally scoped to one language)."
    )
    record_p.add_argument("--ded", required=True, help="DED number, e.g. 63")
    record_p.add_argument("--status", required=True, choices=sorted(STATUS_VALUES))
    record_p.add_argument("--note", default="", help="Free-text note explaining the decision")
    record_p.add_argument(
        "--language",
        default=None,
        help="Restrict the decision to one Starling language (e.g. 'Maria Gondi'); "
        "omit for a whole-DED decision",
    )
    record_p.add_argument("--reviewed-by", default="unspecified")
    record_p.add_argument("--ledger", required=True, help="Path to review_ledger.json")
    record_p.set_defaults(func=_cli_record)

    status_p = sub.add_parser("status", help="Show the recorded review status for a DED number.")
    status_p.add_argument("--ded", required=True)
    status_p.add_argument("--ledger", required=True, help="Path to review_ledger.json")
    status_p.set_defaults(func=_cli_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
