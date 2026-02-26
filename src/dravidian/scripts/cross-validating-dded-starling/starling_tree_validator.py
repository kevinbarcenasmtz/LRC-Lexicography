"""
Tree-based Starling-to-Burrow DED paragraph validator.
Parses the hierarchical Starling JSON, builds a family tree per record,
validates each branch that carries a "Number in DED" against Burrow corpus
attestations, and outputs a rich xlsx report.
Proto-Dravidian (top-level root, no DED number) is reported on but NOT
validated -- it sits above the DED entirely. The attestation ceiling is
the highest proto node that actually carries a DED number.
Usage:
    python starling_tree_validator.py starling_complete_data.json \
        --corpus validation_output/burrow_cache/burrow_corpus.json
    python starling_tree_validator.py starling_complete_data.json \
        --corpus validation_output/burrow_cache/burrow_corpus.json \
        --test 10
"""

from __future__ import annotations
import argparse
import json
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd
from burrow_entry_parser import LanguageAttestation
from dialect_mapping import match_languages, starling_to_burrow, diagnostic_report

_METADATA_KEYS = {
    "Meaning",
    "Notes",
    "Number in DED",
    "Number in CVOTGD",
    "Additional forms",
    "Dravidian etymology",
    "South Dravidian etymology",
    "Gondwan etymology",
}
_DERIVATIVE_SUFFIXES = (" meaning", " derivates", " derivatives")


def _is_proto_key(key: str) -> bool:
    return key.startswith("Proto-") or key.startswith("Proto ")


def _is_language_key(key: str) -> bool:
    if key.startswith("_") or key in _METADATA_KEYS:
        return False
    if _is_proto_key(key):
        return False
    key_lower = key.lower()
    if any(key_lower.endswith(s) for s in _DERIVATIVE_SUFFIXES):
        return False
    if "etymology" in key_lower or "dialectal forms" in key_lower:
        return False
    return bool(key) and key[0].isupper()


def _extract_headword(value: str) -> str:
    """Extract headword before any quoted gloss."""
    if not value:
        return ""
    quote_pos = value.find('"')
    if quote_pos > 0:
        return value[:quote_pos].strip().rstrip(",;")
    return value.strip()


def _normalize_for_match(text: str) -> str:
    base = (
        text.replace("*", "")
        .replace("-", " ")
        .replace("(", " ")
        .replace(")", " ")
        .strip()
        .lower()
    )
    decomposed = unicodedata.normalize("NFKD", base)
    filtered = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(filtered.split())


def _clean_ded_number(raw: Any) -> Optional[str]:
    """Normalize a DED number to a plain integer string: '0047' -> '47'."""
    if raw is None:
        return None
    try:
        return str(int(float(str(raw).strip())))
    except (ValueError, TypeError):
        s = str(raw).strip()
        return s if s else None


@dataclass
class LanguageEntry:
    """A single leaf-language attestation extracted from a Starling node."""

    language: str
    headword: str
    headword_raw: str
    meaning: str
    source_node_label: str


@dataclass
class TreeNode:
    """A node in the Starling etymon tree."""

    label: str
    headword: str
    meaning: str
    ded_number: Optional[str] = None
    is_proto: bool = True
    depth: int = 0
    language_entries: List[LanguageEntry] = field(default_factory=list)
    children: List[TreeNode] = field(default_factory=list)

    def all_language_entries(self) -> List[LanguageEntry]:
        """Recursively collect all leaf language entries under this node."""
        entries = list(self.language_entries)
        for child in self.children:
            entries.extend(child.all_language_entries())
        return entries


@dataclass
class ValidationResult:
    """Validation result for one language entry within one branch."""

    record_num: int
    pd_headword: str
    pd_meaning: str
    branch_label: str
    branch_headword: str
    ded_number: Optional[str]
    language: str
    starling_headword: str
    matched: bool = False
    burrow_headword: str = ""
    burrow_gloss: str = ""
    burrow_language_abbrev: str = ""
    match_type: str = ""
    match_confidence: float = 0.0
    branch_status: str = ""
    notes: str = ""


def _parse_node(data: Dict[str, Any], depth: int = 0) -> TreeNode:
    """Parse a Starling JSON object into a TreeNode."""
    proto_key = None
    proto_headword = ""

    for key, val in data.items():
        if _is_proto_key(key) and isinstance(val, str) and val.strip():
            if proto_key is None:  # ← FIX: only set if not already set
                proto_key = key
                proto_headword = val

    meaning = str(data.get("Meaning", "") or "")
    ded_number = _clean_ded_number(data.get("Number in DED"))

    node = TreeNode(
        label=proto_key or "unknown",
        headword=proto_headword,
        meaning=meaning,
        ded_number=ded_number,
        is_proto=proto_key is not None,
        depth=depth,
    )

    for key, val in data.items():
        if not _is_language_key(key):
            continue
        if not isinstance(val, str) or not val.strip():
            continue
        hw_raw = val.strip()
        hw = _extract_headword(hw_raw)
        if not hw:
            continue
        lang_meaning = str(data.get(f"{key} meaning", "") or "")
        node.language_entries.append(
            LanguageEntry(
                language=key,
                headword=hw,
                headword_raw=hw_raw,
                meaning=lang_meaning,
                source_node_label=node.label,
            )
        )

    for sub in data.get("_sub_entries", []):
        if not isinstance(sub, dict) or sub.get("_circular_reference"):
            continue
        node.children.append(_parse_node(sub, depth=depth + 1))

    return node


def load_burrow_corpus(
    corpus_path: str,
) -> Dict[str, List[LanguageAttestation]]:
    """
    Load the patched Burrow corpus JSON. Filters to DEDR entries only
    (skips Appendix). Returns attestations grouped by DED number string.
    """
    with open(corpus_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    by_ded: Dict[str, List[LanguageAttestation]] = defaultdict(list)
    skipped = 0
    for entry in entries:
        if entry.get("edition", "DEDR") == "Appendix":
            skipped += 1
            continue
        ded_raw = entry.get("ded_number")
        if ded_raw is None:
            continue
        ded_str = _clean_ded_number(ded_raw)
        if not ded_str or str(ded_raw).startswith("App."):
            skipped += 1
            continue
        for att_data in entry.get("attestations", []):
            try:
                by_ded[ded_str].append(LanguageAttestation(**att_data))
            except TypeError:
                continue
    print(
        f"Burrow corpus: {len(entries)} entries, "
        f"{skipped} appendix skipped, "
        f"{len(by_ded)} unique DEDR paragraphs indexed"
    )
    return dict(by_ded)


def _match_entry(
    entry: LanguageEntry,
    burrow_atts: List[LanguageAttestation],
    strict: bool = False,
) -> Tuple[bool, str, float, Optional[LanguageAttestation], str]:
    """
    Try to match a Starling language entry against Burrow attestations.
    Returns (matched, match_type, confidence, best_attestation, notes).
    """
    starling_norm = _normalize_for_match(entry.headword)

    best_match_result = None
    best_att = None
    best_headword_match = False

    for att in burrow_atts:
        lang_match = match_languages(att.language_abbrev, entry.language, strict=strict)

        if not lang_match.matched:
            continue

        if (
            not best_match_result
            or lang_match.confidence > best_match_result.confidence
        ):
            best_match_result = lang_match
            best_att = att

        for bhw in att.headwords:
            bhw_norm = _normalize_for_match(bhw)
            if bhw_norm == starling_norm:
                return True, "exact", lang_match.confidence, att, lang_match.notes
            if (bhw_norm in starling_norm or starling_norm in bhw_norm) and min(
                len(bhw_norm), len(starling_norm)
            ) >= 2:
                best_headword_match = True

    if best_headword_match and best_att:
        return (
            True,
            "substring",
            best_match_result.confidence,
            best_att,
            best_match_result.notes,
        )

    if best_match_result and best_match_result.matched and best_att:
        notes = (
            f"Language matched ({best_att.language_abbrev} = {entry.language}, "
            f"conf: {best_match_result.confidence:.2f}) but headword mismatch"
        )
        if best_match_result.notes:
            notes += f"; {best_match_result.notes}"
        return False, "language_only", best_match_result.confidence, best_att, notes

    return False, "none", 0.0, None, "No language match found"


def _validate_branch(
    branch: TreeNode,
    record_num: int,
    pd_headword: str,
    pd_meaning: str,
    burrow_by_ded: Dict[str, List[LanguageAttestation]],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a proto-branch (which has a DED number) against Burrow."""
    ded = branch.ded_number
    all_entries = branch.all_language_entries()
    if not all_entries:
        return []

    burrow_atts = burrow_by_ded.get(ded, []) if ded else []
    burrow_langs = {att.language_name for att in burrow_atts}

    results: List[ValidationResult] = []
    matched_count = 0

    for entry in all_entries:
        vr = ValidationResult(
            record_num=record_num,
            pd_headword=pd_headword,
            pd_meaning=pd_meaning,
            branch_label=branch.label,
            branch_headword=branch.headword,
            ded_number=ded,
            language=entry.language,
            starling_headword=entry.headword,
        )

        if not ded:
            vr.notes = "Branch has no DED number"
            results.append(vr)
            continue

        if not burrow_atts:
            vr.notes = f"DED {ded} not found in Burrow corpus"
            results.append(vr)
            continue

        matched, match_type, confidence, att, notes = _match_entry(
            entry, burrow_atts, strict=strict
        )

        vr.matched = matched
        vr.match_type = match_type
        vr.match_confidence = confidence
        vr.notes = notes

        if att:
            vr.burrow_headword = ", ".join(att.headwords)
            vr.burrow_gloss = att.gloss[:200]
            vr.burrow_language_abbrev = att.language_abbrev

        if not matched and match_type != "language_only" and burrow_atts:
            vr.notes = (
                f"{entry.language} not in DED {ded}; "
                f"Burrow has: {', '.join(sorted(burrow_langs))}"
            )

        if matched:
            matched_count += 1

        results.append(vr)

    total = len(all_entries)
    if not ded:
        status = "no_ded_number"
    elif not burrow_atts:
        status = "ded_not_in_corpus"
    elif matched_count == total:
        status = "fully_attested"
    elif matched_count > 0:
        status = f"partially_attested ({matched_count}/{total})"
    else:
        status = "not_attested"

    for vr in results:
        vr.branch_status = status

    return results


def validate_record(
    record: Dict[str, Any],
    burrow_by_ded: Dict[str, List[LanguageAttestation]],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a single top-level Starling record (one etymon)."""
    tree = _parse_node(record, depth=0)
    record_num = record.get("_record_num", 0)
    pd_headword = tree.headword
    pd_meaning = tree.meaning

    results: List[ValidationResult] = []

    def _walk(node: TreeNode) -> None:
        # If this node has a DED number and is not root, validate it
        if node.ded_number and node.depth > 0:
            # Validate only DIRECT language entries (not descendants)
            if node.language_entries:
                results.extend(
                    _validate_branch_direct(
                        node,
                        record_num,
                        pd_headword,
                        pd_meaning,
                        burrow_by_ded,
                        strict=strict,
                    )
                )

        # ALWAYS recurse to children to find nested branches
        for child in node.children:
            _walk(child)

        # Report orphan language entries (node has no DED, not root)
        if not node.ded_number and node.depth > 0 and node.language_entries:
            for entry in node.language_entries:
                results.append(
                    ValidationResult(
                        record_num=record_num,
                        pd_headword=pd_headword,
                        pd_meaning=pd_meaning,
                        branch_label=node.label,
                        branch_headword=node.headword,
                        ded_number=None,
                        language=entry.language,
                        starling_headword=entry.headword,
                        branch_status="no_ded_number",
                        notes="Parent branch has no DED number",
                    )
                )

    _walk(tree)
    return results


def _validate_branch_direct(
    branch: TreeNode,
    record_num: int,
    pd_headword: str,
    pd_meaning: str,
    burrow_by_ded: Dict[str, List[LanguageAttestation]],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a proto-branch using ONLY its direct language entries (not descendants)."""
    ded = branch.ded_number
    direct_entries = branch.language_entries  # Only direct children, not recursive

    if not direct_entries:
        return []

    burrow_atts = burrow_by_ded.get(ded, []) if ded else []
    burrow_langs = {att.language_name for att in burrow_atts}

    results: List[ValidationResult] = []
    matched_count = 0

    for entry in direct_entries:
        vr = ValidationResult(
            record_num=record_num,
            pd_headword=pd_headword,
            pd_meaning=pd_meaning,
            branch_label=branch.label,
            branch_headword=branch.headword,
            ded_number=ded,
            language=entry.language,
            starling_headword=entry.headword,
        )

        if not ded:
            vr.notes = "Branch has no DED number"
            results.append(vr)
            continue

        if not burrow_atts:
            vr.notes = f"DED {ded} not found in Burrow corpus"
            results.append(vr)
            continue

        matched, match_type, confidence, att, notes = _match_entry(
            entry, burrow_atts, strict=strict
        )

        vr.matched = matched
        vr.match_type = match_type
        vr.match_confidence = confidence
        vr.notes = notes

        if att:
            vr.burrow_headword = ", ".join(att.headwords)
            vr.burrow_gloss = att.gloss[:200]
            vr.burrow_language_abbrev = att.language_abbrev

        if not matched and match_type != "language_only" and burrow_atts:
            vr.notes = (
                f"{entry.language} not in DED {ded}; "
                f"Burrow has: {', '.join(sorted(burrow_langs))}"
            )

        if matched:
            matched_count += 1

        results.append(vr)

    # Roll up branch status based on direct entries only
    total = len(direct_entries)
    if not ded:
        status = "no_ded_number"
    elif not burrow_atts:
        status = "ded_not_in_corpus"
    elif matched_count == total:
        status = "fully_attested"
    elif matched_count > 0:
        status = f"partially_attested ({matched_count}/{total})"
    else:
        status = "not_attested"

    for vr in results:
        vr.branch_status = status

    return results


def results_to_dataframe(results: List[ValidationResult]) -> pd.DataFrame:
    rows = []
    for vr in results:
        if vr.matched:
            match_display = f"Yes ({vr.match_type}, {vr.match_confidence:.2f})"
        elif vr.match_type == "language_only":
            match_display = f"Language only (conf: {vr.match_confidence:.2f})"
        elif vr.ded_number:
            match_display = "No"
        else:
            match_display = "N/A (no DED)"

        rows.append(
            {
                "Record #": vr.record_num,
                "PD headword": vr.pd_headword,
                "PD meaning": vr.pd_meaning,
                "Branch": vr.branch_label,
                "Branch headword": vr.branch_headword,
                "DED #": vr.ded_number or "",
                "Language": vr.language,
                "Starling headword": vr.starling_headword,
                "Matched": match_display,
                "Confidence": vr.match_confidence,
                "Burrow lang": vr.burrow_language_abbrev,
                "Burrow headword": vr.burrow_headword,
                "Burrow gloss": vr.burrow_gloss,
                "Branch status": vr.branch_status,
                "Notes": vr.notes,
            }
        )
    return pd.DataFrame(rows)


def generate_summary(results: List[ValidationResult]) -> Dict[str, Any]:
    total = len(results)
    matched = sum(1 for r in results if r.matched)
    with_ded = sum(1 for r in results if r.ded_number)

    branch_keys: Dict[tuple, str] = {}
    for r in results:
        bk = (r.record_num, r.branch_label, r.ded_number)
        if bk not in branch_keys:
            branch_keys[bk] = r.branch_status

    branch_statuses: Dict[str, int] = defaultdict(int)
    for status in branch_keys.values():
        branch_statuses[status] += 1

    confidence_dist: Dict[str, int] = defaultdict(int)
    for r in results:
        if r.matched:
            conf_bucket = f"{int(r.match_confidence * 100 / 10) * 10}%-{int(r.match_confidence * 100 / 10 + 1) * 10}%"
            confidence_dist[conf_bucket] += 1

    return {
        "total_language_entries": total,
        "entries_with_ded": with_ded,
        "entries_without_ded": total - with_ded,
        "entries_matched": matched,
        "entry_match_rate": round(matched / with_ded * 100, 1) if with_ded else 0,
        "unique_branches": len(branch_keys),
        "branch_status_breakdown": dict(branch_statuses),
        "confidence_distribution": dict(confidence_dist),
        "records_processed": len({r.record_num for r in results}),
    }


def coverage_analysis(
    results: List[ValidationResult],
    burrow_by_ded: Dict[str, List[LanguageAttestation]],
) -> pd.DataFrame:
    """
    Per-DED-paragraph: which languages does Burrow have that Starling
    doesn't, and vice versa. Useful for spotting asymmetric coverage.
    """
    starling_by_ded: Dict[str, Set[str]] = defaultdict(set)
    for r in results:
        if r.ded_number:
            starling_by_ded[r.ded_number].add(r.language)

    rows = []
    for ded in sorted(starling_by_ded, key=lambda x: int(x) if x.isdigit() else 0):
        burrow_atts = burrow_by_ded.get(ded, [])
        burrow_langs = {att.language_name for att in burrow_atts}
        starling_langs = starling_by_ded[ded]

        matched_s, matched_b = set(), set()
        for sl in starling_langs:
            for bl in burrow_langs:
                lang_match = match_languages(bl, sl, strict=False)
                if lang_match.matched:
                    matched_s.add(sl)
                    matched_b.add(bl)

        only_starling = starling_langs - matched_s
        only_burrow = burrow_langs - matched_b

        rows.append(
            {
                "DED #": ded,
                "Starling langs": len(starling_langs),
                "Burrow langs": len(burrow_langs),
                "Matched": len(matched_s),
                "Only in Starling": "; ".join(sorted(only_starling)) or "",
                "Only in Burrow": "; ".join(sorted(only_burrow)) or "",
            }
        )
    return pd.DataFrame(rows)


def run_validation(
    starling_path: str,
    corpus_path: str,
    output_dir: str = "tree_validation_output",
    test_records: Optional[int] = None,
    strict: bool = False,
) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("DIALECT MAPPING INVENTORY")
    print("=" * 70)
    print(diagnostic_report())
    print("\n" + "=" * 70)

    print(f"\nLoading Starling data: {starling_path}")
    with open(starling_path, "r", encoding="utf-8-sig") as f:
        starling_data = json.load(f)
    records = starling_data.get("records", [])
    print(f"Loaded {len(records)} records")

    if test_records:
        records = records[:test_records]
        print(f"TEST MODE: first {test_records} records only")

    burrow_by_ded = load_burrow_corpus(corpus_path)

    all_results: List[ValidationResult] = []
    for i, record in enumerate(records):
        all_results.extend(validate_record(record, burrow_by_ded, strict=strict))
        if (i + 1) % 100 == 0 or i == len(records) - 1:
            print(
                f"  {i + 1}/{len(records)} records, "
                f"{len(all_results)} entries validated"
            )

    df = results_to_dataframe(all_results)
    xlsx_path = out_dir / "tree_validation_results.xlsx"
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    csv_path = out_dir / "tree_validation_results.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\nEntry results:  {xlsx_path}")

    cov_df = coverage_analysis(all_results, burrow_by_ded)
    cov_path = out_dir / "coverage_by_ded_paragraph.xlsx"
    cov_df.to_excel(cov_path, index=False, engine="openpyxl")
    print(f"Coverage sheet: {cov_path}")

    summary = generate_summary(all_results)
    summary_path = out_dir / "tree_validation_summary.json"
    with open(summary_path, "w", encoding="utf-8-sig") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 60}")
    print("TREE VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Records processed:    {summary['records_processed']}")
    print(f"Language entries:      {summary['total_language_entries']}")
    print(f"  With DED number:    {summary['entries_with_ded']}")
    print(f"  Without DED number: {summary['entries_without_ded']}")
    print(f"  Matched in Burrow:  {summary['entries_matched']}")
    if summary["entries_with_ded"]:
        print(f"  Match rate:         {summary['entry_match_rate']}%")
    print(f"\nBranches:             {summary['unique_branches']}")
    for status, count in sorted(summary["branch_status_breakdown"].items()):
        print(f"  {status}: {count}")

    if summary["confidence_distribution"]:
        print(f"\nMatch confidence distribution:")
        for bucket, count in sorted(summary["confidence_distribution"].items()):
            print(f"  {bucket}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tree-based Starling-to-Burrow DED paragraph validator"
    )
    parser.add_argument(
        "starling_json",
        help="Path to starling_complete_data.json",
    )
    parser.add_argument(
        "--corpus",
        required=True,
        help="Path to burrow_corpus.json (patched with edition tags)",
    )
    parser.add_argument(
        "--output-dir",
        default="tree_validation_output",
    )
    parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        help="Process only the first N records",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Use strict language matching (no fuzzy matching)",
    )
    args = parser.parse_args()

    run_validation(
        starling_path=args.starling_json,
        corpus_path=args.corpus,
        output_dir=args.output_dir,
        test_records=args.test,
        strict=args.strict,
    )


if __name__ == "__main__":
    main()
