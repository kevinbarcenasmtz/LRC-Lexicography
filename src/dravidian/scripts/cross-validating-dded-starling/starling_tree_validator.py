"""
Tree-based Starling-to-Burrow DED paragraph validator.
Parses the hierarchical Starling JSON, builds a family tree per record,
validates each branch that carries a "Number in DED" against Burrow corpus
attestations, and outputs a rich xlsx report.
Proto-Dravidian (top-level root, no DED number) is reported on but NOT
validated -- it sits above the DED entirely. The attestation ceiling is
the highest proto node that actually carries a DED number.
Usage:
python .\\src\\dravidian\\scripts\\cross-validating-dded-starling\\starling_tree_validator.py .\\data\\dravidian\\starling\\starling_complete_data_scrape.json --corpus .\\data\\dravidian\\burrow_ded\\burrow_corpus.cleaned.json

"""

from __future__ import annotations
import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from burrow_entry_parser import LanguageAttestation
from dialect_mapping import (
    match_languages,
    diagnostic_report,
    get_inline_abbrevs_for_starling_dialect,
)
from textnorm import (
    clean_ded_number,
    normalize_for_match,
    recover_attestation_gloss_from_full_text,
)
from gloss_extraction import (
    extract_gloss_forms_for_abbrevs,
    extract_id_reference_forms,
    truncate_gloss_before_first_marker,
)
from reporting import (
    build_validation_audit_frames,
    coverage_analysis,
    generate_summary,
    results_to_dataframe,
)
from validation_models import (
    BurrowParagraph,
    LanguageEntry,
    TreeNode,
    ValidationResult,
)

_METADATA_KEYS = {
    "Meaning",
    "Notes",
    "Number in DED",
    "Number in CVOTGD",
    "Additional forms",
    "Additional Forms",
    "Miscellaneous",
    "Dravidian etymology",
    "South Dravidian etymology",
    "Gondwan etymology",
    # Found by full-key audit (2026-08-16, bugfix batch) alongside the
    # Additional Forms / Miscellaneous fix: editorial/reconstruction-note
    # containers that pass the uppercase-first-letter heuristic in
    # _is_language_key but are not language names. "Stems" occurs with its
    # own "Number in DED" at DED 333/445/530 (would otherwise leak a fake
    # always-"No" language row there, same failure mode as Miscellaneous/
    # Additional Forms); "Notes on correspondences" only ever occurs on
    # DED-less sub-entries in the current scrape but is excluded on the
    # same non-language grounds.
    "Stems",
    "Notes on correspondences",
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


def _extract_inline_meaning(value: str) -> str:
    """Extract the quoted gloss embedded in a Starling form string.

    e.g. 'achchÃ„ÂnÃ„Â "to be cut (of one's foot...)"' -> 'to be cut (of one's foot...)'
    Returns empty string when no quoted gloss is present (headword-only values).
    """
    if not value:
        return ""
    open_pos = value.find('"')
    if open_pos < 0:
        return ""
    close_pos = value.rfind('"')
    if close_pos <= open_pos:
        # Unclosed quote Ã¢â‚¬â€ take everything after the opening mark
        return value[open_pos + 1 :].strip()
    return value[open_pos + 1 : close_pos].strip()


def _parse_node(data: Dict[str, Any], depth: int = 0) -> TreeNode:
    """Parse a Starling JSON object into a TreeNode."""
    proto_key = None
    proto_headword = ""

    for key, val in data.items():
        if _is_proto_key(key) and isinstance(val, str) and val.strip():
            if proto_key is None:  # Ã¢â€ Â FIX: only set if not already set
                proto_key = key
                proto_headword = val

    meaning = str(data.get("Meaning", "") or "")
    ded_number = clean_ded_number(data.get("Number in DED"))
    notes = str(data.get("Notes", "") or "")
    additional_forms = str(data.get("Additional forms", "") or "")

    node = TreeNode(
        label=proto_key or "unknown",
        headword=proto_headword,
        meaning=meaning,
        ded_number=ded_number,
        notes=notes,
        additional_forms=additional_forms,
        source_url=str(data.get("_url", "") or ""),
        source_hash=str(data.get("_content_hash", "") or ""),
        is_proto=proto_key is not None,
        depth=depth,
    )

    for key, val in data.items():
        if _is_proto_key(key):
            continue
        if not _is_language_key(key):
            continue
        if not isinstance(val, str) or not val.strip():
            continue
        hw_raw = val.strip()
        hw = _extract_headword(hw_raw)
        if not hw:
            continue
        lang_meaning = (
            str(data.get(f"{key} meaning", "") or "")
            or _extract_inline_meaning(hw_raw)
            or meaning
        )
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
) -> Dict[str, BurrowParagraph]:
    """
    Load the patched Burrow corpus JSON. Filters to DEDR entries only
    (skips Appendix). Returns attestations grouped by DED number string.
    """
    with open(corpus_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    by_ded: Dict[str, BurrowParagraph] = {}
    skipped = 0
    dropped_attestations: List[Tuple[Optional[str], str]] = []
    for entry in entries:
        if entry.get("edition", "DEDR") == "Appendix":
            skipped += 1
            continue
        ded_raw = entry.get("ded_number")
        if ded_raw is None:
            continue
        ded_str = clean_ded_number(ded_raw)
        if not ded_str or str(ded_raw).startswith("App."):
            skipped += 1
            continue

        paragraph = by_ded.setdefault(
            ded_str,
            BurrowParagraph(
                attestations=[],
                raw_html=entry.get("raw_html", ""),
                full_text=entry.get("full_text", ""),
                page=entry.get("page", 0),
            ),
        )
        for att_data in entry.get("attestations", []):
            try:
                att = LanguageAttestation(**att_data)
            except TypeError as exc:
                # A malformed attestation record means real DEDR data would
                # silently vanish from validation (and later be misread as
                # "language missing from Burrow") -- collect and report
                # instead of dropping quietly.
                dropped_attestations.append((ded_str, str(exc)))
                continue
            repaired_gloss = recover_attestation_gloss_from_full_text(
                paragraph.full_text,
                att.language_abbrev,
                att.headwords,
                att.gloss,
            )
            if repaired_gloss:
                att.gloss = repaired_gloss
            paragraph.attestations.append(att)
    print(
        f"Burrow corpus: {len(entries)} entries, "
        f"{skipped} appendix skipped, "
        f"{len(by_ded)} unique DEDR paragraphs indexed"
    )
    if dropped_attestations:
        print(
            f"WARNING: {len(dropped_attestations)} malformed attestation(s) "
            "dropped from the corpus:",
            file=sys.stderr,
        )
        for ded, err in dropped_attestations[:10]:
            print(f"  DED {ded}: {err}", file=sys.stderr)
        if len(dropped_attestations) > 10:
            print(
                f"  ... and {len(dropped_attestations) - 10} more",
                file=sys.stderr,
            )
    return dict(by_ded)


@dataclass
class MatchOutcome:
    """Result of matching one Starling language entry against Burrow attestations."""

    matched: bool
    match_type: str
    confidence: float
    attestation: Optional[LanguageAttestation]
    notes: str
    parsed_gloss: str
    burrow_lang: str = ""
    burrow_form: str = ""
    burrow_gloss: str = ""


def _leading_form_span(segment: str, target_norm: str) -> str:
    """Find the smallest leading word-span of ``segment`` whose normalized
    form contains ``target_norm`` (used to report a tidy ``burrow_form`` for
    a ``gloss_secondary`` match rather than the whole gloss segment).

    Returns "" if no such span exists (caller falls back to the full
    segment text).
    """
    words = segment.split()
    for i in range(1, len(words) + 1):
        span = " ".join(words[:i])
        if target_norm in normalize_for_match(span):
            return span
    return ""


def _match_entry(
    entry: LanguageEntry,
    burrow_atts: List[LanguageAttestation],
    strict: bool = False,
    attestation_full_text: str = "",
) -> MatchOutcome:
    """Try to match a Starling language entry against Burrow attestations."""
    starling_norm = normalize_for_match(entry.headword)

    best_match_result = None
    best_att = None
    best_headword_match = False
    best_headword_match_form = ""
    best_headword_match_gloss = ""
    best_headword_match_is_id_ref = False
    best_headword_match_conf = -1.0
    best_exact_match: Optional[
        Tuple[
            float,
            LanguageAttestation,
            str,
            str,
            str,
            str,
            str,
        ]
    ] = None
    matched_burrow_lang = ""
    matched_burrow_form = ""
    matched_burrow_gloss = ""
    parsed_burrow_segments: List[Dict[str, Any]] = []

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

        att_gloss = recover_attestation_gloss_from_full_text(
            attestation_full_text,
            att.language_abbrev,
            att.headwords,
            att.gloss,
        )

        # entry.language is a specific named dialect of a consolidated
        # Burrow language (e.g. "Kuwi (Fitzgerald)", "Gommu Gondi") whose
        # OWN marker was consumed during parsing as a headword qualifier
        # (see truncate_gloss_before_first_marker) -- so a direct headword
        # match here would otherwise display unrelated dialects' citation
        # text as this dialect's "meaning". Generic/base-language matches
        # (e.g. plain "Gondi") have no inline_abbrevs and are unaffected.
        direct_match_gloss = att_gloss
        if get_inline_abbrevs_for_starling_dialect(entry.language):
            direct_match_gloss = truncate_gloss_before_first_marker(att_gloss)

        candidate_headword_forms: List[Tuple[str, str, bool]] = [
            (hw.strip(), direct_match_gloss, False) for hw in att.headwords if hw.strip()
        ]
        for form, meaning in extract_id_reference_forms(
            att_gloss,
            source_abbrev=att.language_abbrev,
            source_headword=(att.headwords[0] if att.headwords else ""),
            fallback_text=attestation_full_text,
        ):
            candidate_headword_forms.append((form, meaning, True))

        parsed_direct_candidates = [
            _build_parsed_burrow_segment(
                source=att.language_abbrev,
                form=form,
                meaning=meaning,
                used_id=used_id,
                match_type="direct" if not used_id else "id_reference",
            )
            for form, meaning, used_id in candidate_headword_forms
        ]

        for bhw, meaning, used_id in candidate_headword_forms:
            bhw_norm = normalize_for_match(bhw)
            if bhw_norm == starling_norm:
                # Keep direct exact as fallback; inline/dialect extraction is
                # evaluated after the scan and should take precedence.
                if (best_exact_match is None) or (
                    lang_match.confidence >= best_exact_match[0]
                ):
                    best_exact_match = (
                        lang_match.confidence,
                        att,
                        lang_match.notes,
                        _parsed_burrow_segments_to_text(parsed_direct_candidates),
                        att.language_abbrev,
                        bhw,
                        meaning,
                    )
            if (bhw_norm in starling_norm or starling_norm in bhw_norm) and min(
                len(bhw_norm), len(starling_norm)
            ) >= 2:
                if lang_match.confidence >= best_headword_match_conf:
                    best_headword_match = True
                    best_headword_match_conf = lang_match.confidence
                    best_headword_match_form = bhw
                    best_headword_match_gloss = meaning
                    best_headword_match_is_id_ref = used_id

    # For consolidated Burrow entries (e.g. Go.) that encode dialect variants
    # inline in the gloss (e.g. "(Mu.) acc-", "(Tr. W.) askÃ„ÂnÃ„Â"), attempt a
    # dialect-specific form match before falling back to direct matches.
    if best_att and best_match_result and best_match_result.matched:
        inline_abbrevs = get_inline_abbrevs_for_starling_dialect(entry.language)
        if inline_abbrevs:
            primary_hw = best_att.headwords[0] if best_att.headwords else ""
            best_att_gloss = recover_attestation_gloss_from_full_text(
                attestation_full_text,
                best_att.language_abbrev,
                best_att.headwords,
                best_att.gloss,
            )
            gloss_forms = extract_gloss_forms_for_abbrevs(
                primary_hw, best_att_gloss, inline_abbrevs
            )
            parsed_burrow_segments = [
                _build_parsed_burrow_segment(
                    source=best_att.language_abbrev,
                    form=form,
                    meaning=meaning_text,
                    marker=marker,
                    used_id=used_id,
                    match_type="inline",
                )
                for form, used_id, marker, meaning_text in gloss_forms
            ]

            for form, used_id, marker, meaning_text in gloss_forms:
                id_note = ""
                if used_id:
                    id_note = "Source representation is id."
                else:
                    id_note = ""

                form_norm = normalize_for_match(form)
                if form_norm == starling_norm:
                    match_notes = best_match_result.notes
                    matched_burrow_lang = marker or best_att.language_abbrev
                    matched_burrow_form = form
                    matched_burrow_gloss = meaning_text
                    if used_id:
                        match_notes = f"{id_note}; {match_notes}" if match_notes else id_note
                    return MatchOutcome(
                        matched=True,
                        match_type="gloss_dialect_exact",
                        confidence=best_match_result.confidence,
                        attestation=best_att,
                        notes=match_notes,
                        parsed_gloss=_parsed_burrow_segments_to_text(parsed_burrow_segments),
                        burrow_lang=matched_burrow_lang,
                        burrow_form=matched_burrow_form,
                        burrow_gloss=matched_burrow_gloss,
                    )
                if (
                    form_norm in starling_norm or starling_norm in form_norm
                ) and min(len(form_norm), len(starling_norm)) >= 2:
                    match_notes = best_match_result.notes
                    matched_burrow_lang = marker or best_att.language_abbrev
                    matched_burrow_form = form
                    matched_burrow_gloss = meaning_text
                    if used_id:
                        match_notes = f"{id_note}; {match_notes}" if match_notes else id_note
                    return MatchOutcome(
                        matched=True,
                        match_type="gloss_dialect_substring",
                        confidence=best_match_result.confidence,
                        attestation=best_att,
                        notes=match_notes,
                        parsed_gloss=_parsed_burrow_segments_to_text(parsed_burrow_segments),
                        burrow_lang=matched_burrow_lang,
                        burrow_form=matched_burrow_form,
                        burrow_gloss=matched_burrow_gloss,
                    )

    if best_exact_match:
        (
            exact_conf,
            exact_att,
            exact_notes,
            exact_parsed,
            exact_lang,
            exact_form,
            exact_gloss,
        ) = best_exact_match
        return MatchOutcome(
            matched=True,
            match_type="exact",
            confidence=exact_conf,
            attestation=exact_att,
            notes=exact_notes,
            parsed_gloss=exact_parsed,
            burrow_lang=exact_lang,
            burrow_form=exact_form,
            burrow_gloss=exact_gloss,
        )

    if best_headword_match and best_att:
        matched_burrow_lang = best_att.language_abbrev
        matched_burrow_form = best_headword_match_form or ", ".join(best_att.headwords)
        # An empty best_headword_match_gloss can mean either "no gloss
        # available" (fall back to the raw attestation gloss) or "this
        # dialect's marker was deliberately truncated to nothing" (see
        # truncate_gloss_before_first_marker) -- the latter must NOT fall
        # back, or it would re-display the unrelated full blob it was
        # trimmed to avoid.
        matched_burrow_gloss = best_headword_match_gloss or (
            "" if get_inline_abbrevs_for_starling_dialect(entry.language) else best_att.gloss
        )
        matched_burrow_segments = [
            (headword, meaning, False)
            for headword in best_att.headwords
            for meaning in [
                recover_attestation_gloss_from_full_text(
                    attestation_full_text,
                    best_att.language_abbrev,
                    best_att.headwords,
                    best_att.gloss,
                )
            ]
        ] + [
            (form, meaning, True)
            for form, meaning in extract_id_reference_forms(
                recover_attestation_gloss_from_full_text(
                    attestation_full_text,
                    best_att.language_abbrev,
                    best_att.headwords,
                    best_att.gloss,
                ),
                source_abbrev=best_att.language_abbrev,
                source_headword=(best_att.headwords[0] if best_att.headwords else ""),
                fallback_text=attestation_full_text,
            )
        ]
        parsed_burrow_segments = [
            _build_parsed_burrow_segment(
                source=best_att.language_abbrev,
                form=form,
                meaning=meaning,
                used_id=used_id,
                match_type="direct" if not used_id else "id_reference",
            )
            for form, meaning, used_id in matched_burrow_segments
        ]
        return MatchOutcome(
            matched=True,
            match_type="substring",
            confidence=best_match_result.confidence,
            attestation=best_att,
            notes=best_match_result.notes,
            parsed_gloss=_parsed_burrow_segments_to_text(parsed_burrow_segments),
            burrow_lang=matched_burrow_lang,
            burrow_form=matched_burrow_form,
            burrow_gloss=matched_burrow_gloss,
        )

    # Burrow lists several semicolon-separated form-groups per language
    # (e.g. Kuwi "aḍḍe ānai to resist; addu ānai/kīnai to obviate; ..."), but
    # the parser only stores the FIRST group as att.headwords -- so a
    # Starling headword sitting verbatim later in the same attestation's
    # gloss was previously reported as "Language only ... headword
    # mismatch" even though Burrow does attest it. Scan the best
    # attestation's recovered gloss for a semicolon segment containing the
    # Starling headword, as a last resort before giving up on this entry.
    # This must fire ONLY after every match path above has failed, so it
    # can never downgrade an existing verdict.
    if best_att and best_match_result and best_match_result.matched:
        starling_norm_len_ok = len(starling_norm.replace(" ", "")) >= 3 or (
            len(starling_norm.split()) >= 2
        )
        if starling_norm and starling_norm_len_ok:
            secondary_gloss = recover_attestation_gloss_from_full_text(
                attestation_full_text,
                best_att.language_abbrev,
                best_att.headwords,
                best_att.gloss,
            )
            # For a tracked Gondi/Kuwi/Kui dialect, cut off at the first
            # OTHER dialect's inline marker (same treatment as
            # direct_match_gloss above) so this scan can't attribute a
            # sibling dialect's citation form to the wrong Starling entry.
            if get_inline_abbrevs_for_starling_dialect(entry.language):
                secondary_gloss = truncate_gloss_before_first_marker(secondary_gloss)
            for segment in secondary_gloss.split(";"):
                segment = segment.strip()
                if not segment:
                    continue
                segment_norm = normalize_for_match(segment)
                if not segment_norm or starling_norm not in segment_norm:
                    continue
                form_chunk = _leading_form_span(segment, starling_norm) or segment
                form_chunk = form_chunk.strip(" ,;.")
                parsed_burrow_segments = [
                    _build_parsed_burrow_segment(
                        source=best_att.language_abbrev,
                        form=form_chunk,
                        meaning=segment,
                        match_type="gloss_secondary",
                    )
                ]
                return MatchOutcome(
                    matched=True,
                    match_type="gloss_secondary",
                    confidence=best_match_result.confidence,
                    attestation=best_att,
                    notes=best_match_result.notes,
                    parsed_gloss=_parsed_burrow_segments_to_text(parsed_burrow_segments),
                    burrow_lang=best_att.language_abbrev,
                    burrow_form=form_chunk,
                    burrow_gloss=segment,
                )

    if best_match_result and best_match_result.matched and best_att:
        notes = (
            f"Language matched ({best_att.language_abbrev} = {entry.language}, "
            f"conf: {best_match_result.confidence:.2f}) but headword mismatch"
        )
        if best_match_result.notes:
            notes += f"; {best_match_result.notes}"
        # For non-matching headword, still show what was available in the
        # relevant Burrow entry for auditing/inspection.
        parsed_burrow_segments = []
        inline_abbrevs = get_inline_abbrevs_for_starling_dialect(entry.language)
        if inline_abbrevs and best_att:
            primary_hw = best_att.headwords[0] if best_att.headwords else ""
            best_att_gloss = recover_attestation_gloss_from_full_text(
                attestation_full_text,
                best_att.language_abbrev,
                best_att.headwords,
                best_att.gloss,
            )
            inline_candidates = extract_gloss_forms_for_abbrevs(
                primary_hw, best_att_gloss, inline_abbrevs
            )
            if inline_candidates:
                if any(item[1] for item in inline_candidates):
                    id_note = "Source representation is id."
                    notes = f"{id_note}; {notes}" if notes else id_note
                parsed_burrow_segments = [
                    _build_parsed_burrow_segment(
                        source=best_att.language_abbrev,
                        form=form,
                        meaning=meaning_text,
                        marker=marker,
                        used_id=used_id,
                        match_type="inline",
                    )
                    for form, used_id, marker, meaning_text in inline_candidates
                ]
            else:
                parsed_burrow_segments = [
                    _build_parsed_burrow_segment(
                        source=best_att.language_abbrev,
                        form=", ".join(best_att.headwords),
                        meaning=best_att.gloss,
                        match_type="direct",
                    )
                ]
        elif best_att:
            parsed_burrow_segments = [
                _build_parsed_burrow_segment(
                    source=best_att.language_abbrev,
                    form=", ".join(best_att.headwords),
                    meaning=best_att.gloss,
                    match_type="direct",
                )
            ]
        if not matched_burrow_lang and parsed_burrow_segments:
            first_segment = parsed_burrow_segments[0]
            if not matched_burrow_form:
                matched_burrow_form = str(first_segment.get("form", ""))
            if not matched_burrow_gloss:
                matched_burrow_gloss = str(first_segment.get("meaning", ""))
            matched_burrow_lang = (
                str(first_segment.get("marker"))
                if first_segment.get("marker")
                else str(first_segment.get("source", ""))
            )
        return MatchOutcome(
            matched=False,
            match_type="language_only",
            confidence=best_match_result.confidence,
            attestation=best_att,
            notes=notes,
            parsed_gloss=_parsed_burrow_segments_to_text(parsed_burrow_segments),
            burrow_lang=matched_burrow_lang,
            burrow_form=matched_burrow_form,
            burrow_gloss=matched_burrow_gloss,
        )

    return MatchOutcome(
        matched=False,
        match_type="none",
        confidence=0.0,
        attestation=None,
        notes="No language match found",
        parsed_gloss=_parsed_burrow_segments_to_text([]),
    )


def _collect_scope_language_entries(branch: TreeNode) -> List[LanguageEntry]:
    """
    Collect language entries relevant to this branch's DED number.

    Exclude sub-branches that already carry their own DED number so that each
    attestation is validated at the highest DED-bearing proto node.
    """
    entries = list(branch.language_entries)
    for child in branch.children:
        if child.ded_number:
            continue
        entries.extend(_collect_scope_language_entries(child))
    return entries


def _parsed_burrow_segments_to_text(
    segments: List[Dict[str, Any]],
) -> str:
    """Serialize parsed Burrow segments in a strict machine-friendly format."""
    if not segments:
        return ""
    return json.dumps(segments, ensure_ascii=False)


def _build_parsed_burrow_segment(
    source: str,
    form: str,
    meaning: str,
    marker: str = "",
    used_id: bool = False,
    match_type: str = "inline",
) -> Dict[str, Any]:
    """Build a strict parsed Burrow segment."""
    segment: Dict[str, Any] = {
        "source": source,
        "scope": match_type,
        "form": form,
        "meaning": meaning,
        "id_ref": bool(used_id),
    }
    if marker:
        segment["marker"] = marker
    return segment


def _collect_unique_chain_values(
    chain: List[TreeNode], attr: str, include_current: bool = True
) -> str:
    """Collect unique string values from a proto-chain node field in order."""
    values: List[str] = []
    iterable = chain if include_current else chain[:-1]
    for node in iterable:
        value = str(getattr(node, attr, "") or "")
        if value and value not in values:
            values.append(value)
    return "; ".join(values)


def _collect_ancestor_notes(
    chain: List[TreeNode], include_current: bool = True
) -> str:
    return _collect_unique_chain_values(chain, "notes", include_current)


def _collect_ancestor_additional_forms(
    chain: List[TreeNode], include_current: bool = True
) -> str:
    return _collect_unique_chain_values(chain, "additional_forms", include_current)


def _format_proto_chain(chain: List[TreeNode]) -> str:
    return " > ".join(
        f"{node.label} ({node.headword})" for node in chain if node.label and node.headword
    )


def _labels_from_chain(chain: List[TreeNode]) -> str:
    return " > ".join(node.label for node in chain if node.label)


def _headwords_from_chain(chain: List[TreeNode]) -> str:
    return " > ".join(node.headword for node in chain if node.headword)


def _chain_labels_with_depth(chain: List[TreeNode]) -> str:
    return " > ".join(f"{node.depth}:{node.label}" for node in chain if node.label)


def _count_proto_nodes(chain: List[TreeNode]) -> int:
    return sum(1 for node in chain if node.label and node.headword)


def _validate_branch(
    branch: TreeNode,
    record_num: int,
    pd_headword: str,
    pd_meaning: str,
    burrow_by_ded: Dict[str, BurrowParagraph],
    proto_chain: List[TreeNode],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a proto-branch (which has a DED number) against Burrow."""
    ded = branch.ded_number
    all_entries = _collect_scope_language_entries(branch)
    if not all_entries:
        return []

    paragraph = burrow_by_ded.get(ded) if ded else None
    burrow_atts = paragraph.attestations if paragraph else []
    burrow_langs = {att.language_name for att in burrow_atts}
    ancestor_notes = _collect_ancestor_notes(proto_chain, include_current=False)
    ancestor_additional_forms = _collect_ancestor_additional_forms(
        proto_chain, include_current=False
    )
    proto_label_path = _labels_from_chain(proto_chain)
    proto_headword_path = _headwords_from_chain(proto_chain)
    proto_depth_path = _chain_labels_with_depth(proto_chain)

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
            proto_node_depth=branch.depth,
            starling_meaning=entry.meaning,
            source_node_label=entry.source_node_label,
        )

        if not ded:
            vr.notes = "Branch has no DED number"
            results.append(vr)
            continue

        if not burrow_atts:
            vr.notes = f"DED {ded} not found in Burrow corpus"
            results.append(vr)
            continue

        outcome = _match_entry(
            entry,
            burrow_atts,
            strict=strict,
            attestation_full_text=paragraph.full_text if paragraph else "",
        )

        vr.matched = outcome.matched
        vr.match_type = outcome.match_type
        vr.match_confidence = outcome.confidence
        vr.notes = outcome.notes

        att = outcome.attestation
        if att:
            vr.burrow_headword = outcome.burrow_form or ", ".join(att.headwords)
            vr.burrow_gloss = att.gloss
            vr.burrow_language_abbrev = outcome.burrow_lang or att.language_abbrev
            vr.burrow_source = att.source_text
            vr.burrow_gloss_parsed = outcome.parsed_gloss
            # For a tracked Gondi/Kuwi/Kui dialect, outcome.burrow_gloss was
            # always computed by dialect-aware logic in _match_entry (either
            # the gloss_dialect_* extraction or the deliberately-truncated
            # primary-form path -- see truncate_gloss_before_first_marker),
            # which can legitimately be "" (no distinct meaning recoverable
            # for this dialect). Falling back to att.gloss in that case would
            # silently re-display unrelated dialects' full citation text, so
            # use outcome.burrow_gloss unconditionally here. Generic matches
            # (no inline_abbrevs) keep the truthy-check fallback, since "" is
            # ambiguous there (could mean "not computed" rather than
            # "deliberately empty").
            if get_inline_abbrevs_for_starling_dialect(entry.language):
                vr.burrow_gloss = outcome.burrow_gloss
            elif outcome.burrow_gloss:
                vr.burrow_gloss = outcome.burrow_gloss

        if not outcome.matched and outcome.match_type != "language_only" and burrow_atts:
            vr.notes = (
                f"{entry.language} not in DED {ded}; "
                f"Burrow has: {', '.join(sorted(burrow_langs))}"
            )

        if outcome.matched:
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
        vr.proto_chain = _format_proto_chain(proto_chain)
        vr.proto_label_path = proto_label_path
        vr.proto_headword_path = proto_headword_path
        vr.proto_depth_path = proto_depth_path
        vr.branch_notes = branch.notes
        vr.ancestor_notes = ancestor_notes
        vr.branch_additional_forms = branch.additional_forms
        vr.ancestor_additional_forms = ancestor_additional_forms
        vr.ancestor_proto_count = _count_proto_nodes(proto_chain)
        if paragraph:
            vr.burrow_full_text = paragraph.full_text
        # Keep full path metadata available as plain text for xlsx consumers.

    return results


def validate_record(
    record: Dict[str, Any],
    burrow_by_ded: Dict[str, BurrowParagraph],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a single top-level Starling record (one etymon)."""
    tree = _parse_node(record, depth=0)
    record_num = record.get("_record_num", 0)
    pd_headword = tree.headword
    pd_meaning = tree.meaning

    results: List[ValidationResult] = []

    def _walk(node: TreeNode, proto_chain: List[TreeNode]) -> None:
        chain = list(proto_chain)
        if node.is_proto:
            chain.append(node)

        # If this node has a DED number and is not root, validate it
        if node.ded_number and node.depth > 0:
            scoped_entries = _collect_scope_language_entries(node)
            if scoped_entries:
                results.extend(
                    _validate_branch(
                        node,
                        record_num,
                        pd_headword,
                        pd_meaning,
                        burrow_by_ded,
                        chain,
                        strict=strict,
                    )
                )

        # ALWAYS recurse to children to find nested branches
        for child in node.children:
            _walk(child, chain)

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
                        starling_meaning=entry.meaning,
                        source_node_label=entry.source_node_label,
                        branch_status="no_ded_number",
                        notes="Parent branch has no DED number",
                        proto_chain=_format_proto_chain(chain),
                        proto_label_path=_labels_from_chain(chain),
                        proto_headword_path=_headwords_from_chain(chain),
                        proto_depth_path=_chain_labels_with_depth(chain),
                    )
                )

    _walk(tree, [])
    return results


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

    audit_frames = build_validation_audit_frames(df)
    audit_path = out_dir / "validation_audit_report.xlsx"
    with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
        for sheet_name, frame in audit_frames.items():
            frame.to_excel(writer, index=False, sheet_name=sheet_name)
    print(f"Audit report:   {audit_path}")

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
    # Language abbreviations contain diacritics (e.g. "Koḍ.", "Manḍ.").
    # On Windows the default console codec (cp1252) cannot encode these, so
    # force UTF-8 output before any print() calls.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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


