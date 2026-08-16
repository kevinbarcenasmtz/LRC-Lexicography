"""
Reporting layer for tree validation: canonical-transcription columns
(DEDR-canonical policy, 2026-08-16) and the dataframe/summary/audit
builders consumed by run_validation and triage_mismatches.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from dialect_mapping import match_languages
from textnorm import normalize_for_match
from validation_models import BurrowParagraph, ValidationResult


# Canonical-transcription policy (2026-08-16, Kevin + Todd): where a Starling
# reflex and its matched Burrow/DEDR attestation are the SAME form written in
# different notation, the DEDR spelling is canonical; Starling is retained as a
# variant. This governs the "Transcription status" / "Canonical *" columns only
# — it does NOT change matching (no new "Yes" matches) and needs no corpus regen.
#
# _CANONICAL_CORE_FOLD is the deliberately CONSERVATIVE confusable set used to
# decide whether an unmatched ("language only") row is a pure notation variant
# (safe to adopt DEDR's spelling) rather than a genuinely different/absent form
# (which must keep Starling's headword — swapping it would replace a real reflex
# with an unrelated form). Only the unambiguous IPA-vs-diaeresis / glottal pairs
# are folded here; broader pairs (ẓ↔r̤, ch↔c) are intentionally left out pending a
# linguistic ruling, so they stay classified as divergent_form for now.
_CANONICAL_CORE_FOLD = {
    0x0268: "i", 0x0197: "i",  # ɨ, Ɨ  (Burrow's ï reduces to i under NFKD)
    0x026B: "l", 0x0142: "l",  # ɫ, ł
    0x0292: "z", 0x03B6: "z",  # ʒ, ζ (greek zeta, Burrow's glyph)
    0x0294: "", 0x02C0: "", 0x2019: "", 0x0027: "",  # glottal ʔ/ˀ/’/' vs bare
}


def _transcription_key(text: str, core_fold: bool = False) -> str:
    """Normalized key for transcription-equivalence tests. With ``core_fold`` the
    conservative confusable set is applied on top of the shipped matcher folds."""
    if core_fold:
        text = text.translate(_CANONICAL_CORE_FOLD)
    return normalize_for_match(text)


def _canonical_burrow_form(
    starling_headword: str, burrow_form: str, core_fold: bool
) -> Optional[str]:
    """Return the DEDR spelling of ``starling_headword`` if the two are the same
    form under notation-folding, else None.

    Compares the whole Burrow string first (so parenthetical multi-forms like
    ``aḍï- (aḍïp-, aḍït-)`` match ``aḍɨ- (aḍɨp-, aḍɨt-)`` as one unit), then falls
    back to each comma-separated form (for genuine multi-lexeme Burrow lists).
    """
    key = _transcription_key(starling_headword, core_fold)
    if not key:
        return None
    burrow_form = burrow_form.strip()
    if burrow_form and _transcription_key(burrow_form, core_fold) == key:
        return burrow_form
    for piece in (p.strip() for p in burrow_form.split(",")):
        if piece and _transcription_key(piece, core_fold) == key:
            return piece
    return None


def _canonical_fields(vr: "ValidationResult") -> Tuple[str, str, str]:
    """(canonical_headword, canonical_source, transcription_status) for one row.

    Policy: DEDR is the canonical transcription wherever it attests the form.
    - matched rows: DEDR spelling is canonical (identical when byte-equal, else a
      notational_variant reconciled by the shipped matcher folds).
    - language-only rows: adopt DEDR's spelling only when the conservative core
      fold proves it's the same form (notational_variant); otherwise it's a
      divergent_form and Starling's headword stands.
    - no usable Burrow form (No / N/A): Starling stands.
    """
    starling = vr.starling_headword
    burrow_form = vr.burrow_headword.strip()
    if not burrow_form:
        status = "no_burrow_match" if vr.ded_number else "no_ded"
        return starling, "starling", status
    if vr.matched:
        canon = _canonical_burrow_form(starling, burrow_form, core_fold=False) or burrow_form
        status = "identical" if canon.strip() == starling.strip() else "notational_variant"
        return canon, "burrow", status
    if vr.match_type == "language_only":
        canon = _canonical_burrow_form(starling, burrow_form, core_fold=True)
        if canon is not None:
            status = "identical" if canon.strip() == starling.strip() else "notational_variant"
            return canon, "burrow", status
        return starling, "starling", "divergent_form"
    return starling, "starling", "no_burrow_match"


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

        canonical_headword, canonical_source, transcription_status = _canonical_fields(vr)

        rows.append(
            {
                "Starling record #": vr.record_num,
                "Validation DED #": vr.ded_number or "",
                "Validation branch label": vr.branch_label,
                "Validation branch form": vr.branch_headword,
                "Record proto form": vr.pd_headword,
                "Record proto meaning": vr.pd_meaning,
                "Starling language": vr.language,
                "Starling lexical headword": vr.starling_headword,
                "Starling lexical meaning": vr.starling_meaning,
                "Starling language source branch": vr.source_node_label,
                "Canonical headword": canonical_headword,
                "Canonical source": canonical_source,
                "Transcription status": transcription_status,
                "Match": match_display,
                "Match confidence": vr.match_confidence,
                "Matched Burrow segment scope": vr.burrow_language_abbrev,
                "Matched Burrow form": vr.burrow_headword,
                "Matched Burrow meaning": vr.burrow_gloss,
                "Matched Burrow parsed segments": vr.burrow_gloss_parsed,
                "Burrow matched source token": vr.burrow_source,
                "Burrow paragraph text": vr.burrow_full_text,
                "Proto ancestry node count": vr.ancestor_proto_count,
                "Branch depth (record tree)": vr.proto_node_depth,
                "Proto label lineage": vr.proto_label_path,
                "Proto form lineage": vr.proto_headword_path,
                "Proto depth lineage": vr.proto_depth_path,
                "Proto lineage": vr.proto_chain,
                "Branch notes (Starling)": vr.branch_notes,
                "Ancestor notes (Starling)": vr.ancestor_notes,
                "Branch additional forms (Starling)": vr.branch_additional_forms,
                "Ancestor additional forms (Starling)": vr.ancestor_additional_forms,
                "Branch attestation status": vr.branch_status,
                "Validation note": vr.notes,
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
    burrow_by_ded: Dict[str, BurrowParagraph],
) -> pd.DataFrame:
    """
    Per-DED paragraph coverage summary.

    Includes:
    - language inventory overlap (Starling vs Burrow)
    - row-level validation outcomes (matched/language-only/unmatched)
    """
    results_by_ded: Dict[str, List[ValidationResult]] = defaultdict(list)
    starling_by_ded: Dict[str, Set[str]] = defaultdict(set)
    for r in results:
        if r.ded_number:
            results_by_ded[r.ded_number].append(r)
            starling_by_ded[r.ded_number].add(r.language)

    rows = []
    for ded in sorted(starling_by_ded, key=lambda x: int(x) if x.isdigit() else 0):
        ded_results = results_by_ded.get(ded, [])
        burrow_atts = burrow_by_ded.get(ded, BurrowParagraph(attestations=[])).attestations
        burrow_lang_labels = {att.language_name for att in burrow_atts}
        starling_langs = starling_by_ded[ded]
        matched_rows = sum(1 for r in ded_results if r.matched)
        language_only_rows = sum(1 for r in ded_results if r.match_type == "language_only")
        unmatched_rows = sum(
            1
            for r in ded_results
            if not r.matched and r.match_type not in {"language_only"}
        )

        matched_s, matched_b = set(), set()
        for sl in starling_langs:
            for att in burrow_atts:
                lang_match = match_languages(att.language_abbrev, sl, strict=False)
                if lang_match.matched:
                    matched_s.add(sl)
                    matched_b.add(att.language_name)

        only_starling = starling_langs - matched_s
        only_burrow = burrow_lang_labels - matched_b

        rows.append(
            {
                "DED #": ded,
                "Starling entry rows": len(ded_results),
                "Matched rows": matched_rows,
                "Language-only rows": language_only_rows,
                "Unmatched rows": unmatched_rows,
                "Row match rate %": round((matched_rows / len(ded_results) * 100), 1)
                if ded_results
                else 0.0,
                "Starling languages (unique)": len(starling_langs),
                "Burrow languages (unique)": len(burrow_lang_labels),
                "Language overlap (unique)": len(matched_s),
                "Only in Starling (langs)": "; ".join(sorted(only_starling)) or "",
                "Only in Burrow (langs)": "; ".join(sorted(only_burrow)) or "",
            }
        )
    return pd.DataFrame(rows)


def _normalize_meaning_text(text: Any) -> str:
    # pd.isna catches NaN/None for missing CSV cells; plain `text or ""` does
    # not, since a float NaN is truthy in Python and would otherwise become
    # the literal string "nan" here, falsely registering as a real meaning.
    if pd.isna(text):
        return ""
    value = str(text).strip().lower()
    value = re.sub(r"\s+", " ", value)
    value = value.rstrip(" .;,:")
    return value


def build_validation_audit_frames(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Build review-focused audit frames from tree_validation_results rows.

    These sheets explicitly answer:
    - which rows failed or are language-only
    - which matched rows still have meaning mismatch
    - which rows are missing Starling meanings
    - branch-level and DED-level rollups for attestation status
    """
    work = df.copy()
    work["__starling_meaning_norm"] = work["Starling lexical meaning"].map(_normalize_meaning_text)
    work["__burrow_meaning_norm"] = work["Matched Burrow meaning"].map(_normalize_meaning_text)

    def _is_meaning_mismatch(row: pd.Series) -> bool:
        sm = row["__starling_meaning_norm"]
        bm = row["__burrow_meaning_norm"]
        if not sm or not bm:
            return False
        return (sm not in bm) and (bm not in sm)

    work["__meaning_mismatch"] = work.apply(_is_meaning_mismatch, axis=1)
    work["__is_yes"] = work["Match"].astype(str).str.startswith("Yes")
    work["__is_language_only"] = work["Match"].astype(str).str.startswith("Language only")
    work["__is_no"] = work["Match"].astype(str).eq("No")
    work["__missing_starling_meaning"] = work["__starling_meaning_norm"].eq("")

    issue_rows = work[
        work["__is_language_only"] | work["__is_no"] | work["__meaning_mismatch"]
    ].copy()
    meaning_mismatch_rows = work[
        work["__is_yes"] & work["__meaning_mismatch"]
    ].copy()
    missing_starling_meaning_rows = work[
        work["__missing_starling_meaning"]
    ].copy()

    branch_rollup = (
        work.groupby(
            ["Starling record #", "Validation DED #", "Validation branch label", "Validation branch form"],
            dropna=False,
        )
        .agg(
            branch_status=("Branch attestation status", "first"),
            total_rows=("Match", "size"),
            matched_rows=("Match", lambda s: int(s.astype(str).str.startswith("Yes").sum())),
            language_only_rows=("Match", lambda s: int(s.astype(str).str.startswith("Language only").sum())),
            no_rows=("Match", lambda s: int((s.astype(str) == "No").sum())),
            meaning_mismatch_rows=("__meaning_mismatch", "sum"),
        )
        .reset_index()
    )

    ded_rollup = (
        work.groupby(["Validation DED #"], dropna=False)
        .agg(
            total_rows=("Match", "size"),
            matched_rows=("Match", lambda s: int(s.astype(str).str.startswith("Yes").sum())),
            language_only_rows=("Match", lambda s: int(s.astype(str).str.startswith("Language only").sum())),
            no_rows=("Match", lambda s: int((s.astype(str) == "No").sum())),
            meaning_mismatch_rows=("__meaning_mismatch", "sum"),
            branches=("Validation branch label", "nunique"),
            records=("Starling record #", "nunique"),
        )
        .reset_index()
    )

    review_cols = [
        "Starling record #",
        "Validation DED #",
        "Validation branch label",
        "Validation branch form",
        "Starling language",
        "Starling lexical headword",
        "Starling lexical meaning",
        "Match",
        "Matched Burrow segment scope",
        "Matched Burrow form",
        "Matched Burrow meaning",
        "Validation note",
    ]

    return {
        "row_issues": issue_rows[review_cols],
        "meaning_mismatches": meaning_mismatch_rows[review_cols],
        "missing_starling_meaning": missing_starling_meaning_rows[review_cols],
        "branch_rollup": branch_rollup,
        "ded_rollup": ded_rollup,
    }
