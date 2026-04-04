"""
Tree-based Starling-to-Burrow DED paragraph validator.
Parses the hierarchical Starling JSON, builds a family tree per record,
validates each branch that carries a "Number in DED" against Burrow corpus
attestations, and outputs a rich xlsx report.
Proto-Dravidian (top-level root, no DED number) is reported on but NOT
validated -- it sits above the DED entirely. The attestation ceiling is
the highest proto node that actually carries a DED number.
Usage:
python .\\src\\dravidian\\scripts\\cross-validating-dded-starling\\starling_tree_validator.py .\\data\\dravidian\\starling\\starling_complete_data.json --corpus .\\data\\dravidian\\burrow_ded\\burrow_corpus.cleaned.json

"""

from __future__ import annotations
import argparse
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd
from burrow_entry_parser import LanguageAttestation
from dialect_mapping import (
    match_languages,
    diagnostic_report,
    get_inline_abbrevs_for_starling_dialect,
)

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
    notes: str = ""
    additional_forms: str = ""
    source_url: str = ""
    source_hash: str = ""
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
    proto_node_depth: int = 0
    matched: bool = False
    starling_meaning: str = ""
    source_node_label: str = ""
    burrow_headword: str = ""
    burrow_gloss: str = ""
    burrow_language_abbrev: str = ""
    match_type: str = ""
    match_confidence: float = 0.0
    branch_status: str = ""
    notes: str = ""
    proto_chain: str = ""
    proto_label_path: str = ""
    proto_headword_path: str = ""
    proto_depth_path: str = ""
    branch_notes: str = ""
    ancestor_notes: str = ""
    branch_additional_forms: str = ""
    ancestor_additional_forms: str = ""
    ancestor_proto_count: int = 0
    burrow_full_text: str = ""
    burrow_source: str = ""
    burrow_gloss_parsed: str = ""


@dataclass
class BurrowParagraph:
    """Cached Burrow paragraph grouped by DED number."""

    attestations: List[LanguageAttestation]
    raw_html: str = ""
    full_text: str = ""
    page: int = 0


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
    ded_number = _clean_ded_number(data.get("Number in DED"))
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
                repaired_gloss = _recover_attestation_gloss_from_full_text(
                    paragraph.full_text,
                    att.language_abbrev,
                    (att.headwords[0] if att.headwords else ""),
                    att.gloss,
                )
                if repaired_gloss:
                    att.gloss = repaired_gloss
                paragraph.attestations.append(att)
            except TypeError:
                continue
    print(
        f"Burrow corpus: {len(entries)} entries, "
        f"{skipped} appendix skipped, "
        f"{len(by_ded)} unique DEDR paragraphs indexed"
    )
    return dict(by_ded)


# Common English words that cannot be Dravidian headword forms.
# Used to distinguish a gloss-continuation token from an inline form token.
_GLOSS_STOPWORDS = frozenset({
    "to", "a", "an", "the", "of", "in", "id", "id.", "and", "or", "not",
    "be", "is", "was", "are", "were", "have", "has", "as", "at", "by",
    "for", "its", "it", "this", "that", "also",
})


def _split_attached_to(form: str, meaning_text: str) -> Tuple[str, str]:
    """Split glued form+gloss fragments when parser finds no separator.

    Example: "(Tr. W.)askÄnÄto cut..." should become form="askÄnÄ",
    meaning="to cut ...". We also handle forms like "ask-to cut..." where a
    hyphen remains before the attached gloss marker.
    """
    if not form:
        return form, meaning_text

    # Special case: "(A. Ch. Mu. Ma.)ask-to ..." => form="ask-"
    if len(form) > 3 and form.endswith("-to"):
        return form[:-2], f"to {meaning_text}"

    low = form.lower()
    if low == "to" and meaning_text:
        return form, f"to {meaning_text}"

    if len(low) > 3 and low.endswith("to") and meaning_text:
        base = form[:-2]
        if base and base[-1].isalpha():
            return base, f"to {meaning_text}"
    return form, meaning_text


def _clean_inline_meaning(meaning_text: str) -> str:
    """
    Remove trailing editorial parentheses from inline Burrow meanings while
    preserving lexical gloss content.
    """
    if not meaning_text:
        return meaning_text

    # Trim explanatory tails like "(ask- is pl. action of acc-; Voc. 17)".
    m = re.search(r"\(\s*[^)]*(?:pl\.\s*action|Voc\.)[^)]*\)\s*$", meaning_text, re.IGNORECASE)
    if not m:
        # Also handle truncated tails missing a closing parenthesis.
        m = re.search(r"\(\s*[^)]*(?:pl\.\s*action|Voc\.)[^)]*$", meaning_text, re.IGNORECASE)
    if m:
        meaning_text = meaning_text[: m.start()].rstrip(" ,;")
    return meaning_text.strip()


def _extract_gloss_forms_for_abbrevs(
    primary_headword: str,
    gloss: str,
    target_abbrevs: List[str],
) -> List[Tuple[str, bool, str, str]]:
    """
    Extract headword forms from a Burrow consolidated entry's gloss text for
    specific inline dialect/citation abbreviations.

    Burrow encodes Gondi dialect variants inside a single Go. gloss entry:
        'Go. accÃ„ÂnÃ„Â (Tr.) to be cut; (Mu.) acc- to split; (Tr. W.) askÃ„ÂnÃ„Â ...'

    For target_abbrevs=['W.'] this returns ['askÃ„ÂnÃ„Â'].
    When the matching group has no distinct form token (e.g. '(Tr.) to be cut'),
    falls back to primary_headword since the primary form applies for that source.
    """
    if not gloss or not target_abbrevs:
        return []

    target_set = set(target_abbrevs)
    forms = []

    marker_matches = []
    for marker_match in re.finditer(r"\(([^)]+)\)\s*", gloss):
        marker_text = marker_match.group(1).strip()
        # Keep only dialect/citation marker groups like "Tr.", "Ph.",
        # "Tr. W.", "A. Ch. Mu. Ma." and skip gloss parentheticals.
        if re.match(r"^(?:[A-Za-z]+\.)+(?:\s+[A-Za-z]+\.)*$", marker_text):
            marker_matches.append(marker_match)
    for i, marker_match in enumerate(marker_matches):
        group_text = marker_match.group(1)
        segment_start = marker_match.end()
        next_marker_start = (
            marker_matches[i + 1].start() if i + 1 < len(marker_matches) else len(gloss)
        )
        next_semicolon = gloss.find(";", segment_start)
        if next_semicolon == -1:
            segment_end = next_marker_start
        else:
            segment_end = min(next_marker_start, next_semicolon)

        segment = gloss[segment_start:segment_end].strip().lstrip(",")
        if not segment:
            continue

        parts = segment.split(None, 1)
        following_token = parts[0].rstrip(".,;")
        meaning_text = parts[1].strip() if len(parts) > 1 else ""
        if meaning_text and meaning_text[0] in ")]":
            meaning_text = meaning_text[1:].strip()
        meaning_text = meaning_text.strip(" ;,")
        following_token, meaning_text = _split_attached_to(following_token, meaning_text)
        meaning_text = _clean_inline_meaning(meaning_text)

        normalized_marker = " ".join(part.strip().strip(".") + "." for part in group_text.split())

        # Parse abbreviation tokens from group: "Tr. W." Ã¢â€ â€™ {"Tr.", "W."}
        group_abbrevs: set[str] = set()
        for part in re.split(r"\s+", group_text.strip()):
            part = part.strip()
            if part and not part.endswith("."):
                part += "."
            if part:
                group_abbrevs.add(part)

        if not target_set.isdisjoint(group_abbrevs):
            if following_token.lower() not in _GLOSS_STOPWORDS:
                forms.append((following_token, False, normalized_marker, meaning_text))
            elif primary_headword:
                # No distinct form token for this marker; use the primary
                # headword, but this is not an id. reference.
                forms.append((primary_headword, False, normalized_marker, meaning_text))

    return forms


def _extract_id_reference_from_full_text(
    full_text: str,
    source_abbrev: str,
    source_headword: str,
) -> List[Tuple[str, str]]:
    """
    Recover collapsed "id." formatting from paragraph full text.

    Handles historical cache entries where glosses were rendered without space
    after ';' (e.g., "id.;ac-acroprickly."), producing:
    [("ac-acro", "prickly.")].
    """
    if not full_text or not source_abbrev or not source_headword:
        return []

    normalized = re.sub(r"\s+", " ", full_text).strip()
    marker = f"{source_abbrev.strip()} {source_headword.strip()}"
    marker_re = re.escape(marker)

    # Pattern captures "Malt. acu id.; ..." at the paragraph level.
    m = re.search(
        rf"(?i)(?:^|[\s;]){marker_re}\s+id\.;\s*([^;]*?)(?=\s+(?:DEDS?|DEDR)\s+|$)",
        normalized,
    )
    if not m:
        return []

    segment = m.group(1).strip().rstrip(" ;")
    segment = re.sub(r"\s+DEDS?\b.*$", "", segment, flags=re.IGNORECASE).strip()
    if not segment:
        return []

    if " " in segment:
        form, meaning = segment.split(" ", 1)
        form = form.strip(" ,;")
        meaning = meaning.strip()
        if form and meaning:
            return [(form, meaning)]
    return []


def _recover_attestation_gloss_from_full_text(
    full_text: str,
    source_abbrev: str,
    source_headword: str,
    fallback_gloss: str,
) -> str:
    """
    Recover a fuller attestation gloss from paragraph full_text when cached
    attestation glosses are truncated.
    """
    if not full_text or not source_abbrev or not source_headword:
        return fallback_gloss

    normalized = re.sub(r"\s+", " ", full_text).strip()
    marker = f"{source_abbrev.strip()} {source_headword.strip()}"
    marker_pos = normalized.lower().find(marker.lower())
    if marker_pos < 0:
        return fallback_gloss

    tail = normalized[marker_pos + len(marker) :].strip()
    # Stop at the next top-level language token (e.g. "Malt.", "Ka.") so
    # one attestation does not consume following language segments.
    ignore_tokens = {
        "Tr.",
        "W.",
        "Ph.",
        "Mu.",
        "Ma.",
        "A.",
        "Ch.",
        "Voc.",
        "Cf.",
        "e.g.",
    }
    for m in re.finditer(
        r"\s([A-Z][A-Za-zÀ-ÖØ-öø-ÿĀ-žḀ-ỿ]+\.)\s+\S",
        tail,
    ):
        tok = m.group(1)
        if tok in ignore_tokens:
            continue
        tail = tail[: m.start()].strip()
        break

    tail = re.sub(r"\s+DEDS?\b.*$", "", tail, flags=re.IGNORECASE).strip()
    if not tail:
        return fallback_gloss

    return tail if len(tail) > len(fallback_gloss) else fallback_gloss


def _extract_id_reference_forms(
    gloss: str,
    source_abbrev: str = "",
    source_headword: str = "",
    fallback_text: str = "",
) -> List[Tuple[str, str]]:
    """
    Extract an additional form/meaning pair from glosses that use "id.;".

    Examples from Burrow:
        "id.; ac-acro prickly."
        => [("ac-acro", "prickly.")]
    """
    if not gloss:
        return []

    cleaned = gloss.strip()
    if not cleaned.lower().startswith("id."):
        return []

    if not re.match(r"^id\.\s*;", cleaned, flags=re.IGNORECASE):
        return []

    # Expected shape: "id.; FORM meaning..." (or id.;FORM meaning...)
    m = re.match(r"^id\.\s*;\s*(.+)$", cleaned, flags=re.IGNORECASE)
    if not m:
        return []

    remainder = m.group(1).strip()
    if not remainder:
        return []

    # Use full paragraph text if available; this is the most robust recovery for
    # cache-derived formatting artifacts.
    fallback = _extract_id_reference_from_full_text(
        fallback_text,
        source_abbrev,
        source_headword,
    )
    if fallback:
        return fallback

    if " " in remainder:
        form, meaning = remainder.split(" ", 1)
        form = form.strip(" ,;")
        meaning = meaning.strip()
        if form and meaning:
            meaning = meaning.rstrip(" ;")
            return [(form, meaning)]

    # No explicit boundary after id.; (common when tag-stripped content collapses):
    # e.g. "ac-acroprickly." -> ("ac-acro", "prickly.")
    # Keep a conservative fallback so this doesn't over-fire.
    m2 = re.match(
        r"^([A-Za-zÀ-ÖØ-öø-ÿĀ-žɑ-ʯ\-]+)-([a-zà-öø-ÿɑ-ʯ]{2,})([.;:!?])?$",
        remainder,
    )
    if m2 and not m2.group(1).lower() in _GLOSS_STOPWORDS:
        form = m2.group(1).strip(" ,;")
        meaning = f"{m2.group(2)}{m2.group(3) or ''}"
        if form:
            return [(form, meaning)]

    return []

def _match_entry(
    entry: LanguageEntry,
    burrow_atts: List[LanguageAttestation],
    strict: bool = False,
    attestation_full_text: str = "",
) -> Tuple[
    bool,
    str,
    float,
    Optional[LanguageAttestation],
    str,
    str,
    str,
    str,
    str,
]:
    """
    Try to match a Starling language entry against Burrow attestations.
    Returns (
        matched,
        match_type,
        confidence,
        best_attestation,
        notes,
        parsed_gloss,
        matched_burrow_lang,
        matched_burrow_form,
        matched_burrow_gloss,
    ).
    """
    starling_norm = _normalize_for_match(entry.headword)

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

        att_gloss = _recover_attestation_gloss_from_full_text(
            attestation_full_text,
            att.language_abbrev,
            (att.headwords[0] if att.headwords else ""),
            att.gloss,
        )

        candidate_headword_forms: List[Tuple[str, str, bool]] = [
            (hw.strip(), att_gloss, False) for hw in att.headwords if hw.strip()
        ]
        for form, meaning in _extract_id_reference_forms(
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
            bhw_norm = _normalize_for_match(bhw)
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
            best_att_gloss = _recover_attestation_gloss_from_full_text(
                attestation_full_text,
                best_att.language_abbrev,
                primary_hw,
                best_att.gloss,
            )
            gloss_forms = _extract_gloss_forms_for_abbrevs(
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

                form_norm = _normalize_for_match(form)
                if form_norm == starling_norm:
                    match_notes = best_match_result.notes
                    matched_burrow_lang = marker or best_att.language_abbrev
                    matched_burrow_form = form
                    matched_burrow_gloss = meaning_text
                    if used_id:
                        match_notes = f"{id_note}; {match_notes}" if match_notes else id_note
                    return (
                        True,
                        "gloss_dialect_exact",
                        best_match_result.confidence,
                        best_att,
                        match_notes,
                        _parsed_burrow_segments_to_text(parsed_burrow_segments),
                        matched_burrow_lang,
                        matched_burrow_form,
                        matched_burrow_gloss,
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
                    return (
                        True,
                        "gloss_dialect_substring",
                        best_match_result.confidence,
                        best_att,
                        match_notes,
                        _parsed_burrow_segments_to_text(parsed_burrow_segments),
                        matched_burrow_lang,
                        matched_burrow_form,
                        matched_burrow_gloss,
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
        return (
            True,
            "exact",
            exact_conf,
            exact_att,
            exact_notes,
            exact_parsed,
            exact_lang,
            exact_form,
            exact_gloss,
        )

    if best_headword_match and best_att:
        matched_burrow_lang = best_att.language_abbrev
        matched_burrow_form = best_headword_match_form or ", ".join(best_att.headwords)
        matched_burrow_gloss = best_headword_match_gloss or best_att.gloss
        matched_burrow_segments = [
            (headword, meaning, False)
            for headword in best_att.headwords
            for meaning in [
                _recover_attestation_gloss_from_full_text(
                    attestation_full_text,
                    best_att.language_abbrev,
                    (best_att.headwords[0] if best_att.headwords else ""),
                    best_att.gloss,
                )
            ]
        ] + [
            (form, meaning, True)
            for form, meaning in _extract_id_reference_forms(
                _recover_attestation_gloss_from_full_text(
                    attestation_full_text,
                    best_att.language_abbrev,
                    (best_att.headwords[0] if best_att.headwords else ""),
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
        return (
            True,
            "substring",
            best_match_result.confidence,
            best_att,
            best_match_result.notes,
            _parsed_burrow_segments_to_text(parsed_burrow_segments),
            matched_burrow_lang,
            matched_burrow_form,
            matched_burrow_gloss,
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
            best_att_gloss = _recover_attestation_gloss_from_full_text(
                attestation_full_text,
                best_att.language_abbrev,
                primary_hw,
                best_att.gloss,
            )
            inline_candidates = _extract_gloss_forms_for_abbrevs(
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
        return (
            False,
            "language_only",
            best_match_result.confidence,
            best_att,
            notes,
            _parsed_burrow_segments_to_text(parsed_burrow_segments),
            matched_burrow_lang,
            matched_burrow_form,
            matched_burrow_gloss,
        )

    return (
        False,
        "none",
        0.0,
        None,
        "No language match found",
        _parsed_burrow_segments_to_text([]),
        "",
        "",
        "",
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

        (
            matched,
            match_type,
            confidence,
            att,
            notes,
            parsed_gloss,
            matched_burrow_lang,
            matched_burrow_form,
            matched_burrow_gloss,
        ) = _match_entry(
            entry,
            burrow_atts,
            strict=strict,
            attestation_full_text=paragraph.full_text if paragraph else "",
        )

        vr.matched = matched
        vr.match_type = match_type
        vr.match_confidence = confidence
        vr.notes = notes

        if att:
            vr.burrow_headword = matched_burrow_form or ", ".join(att.headwords)
            vr.burrow_gloss = att.gloss
            vr.burrow_language_abbrev = matched_burrow_lang or att.language_abbrev
            vr.burrow_source = att.source_text
            vr.burrow_gloss_parsed = parsed_gloss
            if matched_burrow_gloss:
                vr.burrow_gloss = matched_burrow_gloss

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


def _validate_branch_direct(
    branch: TreeNode,
    record_num: int,
    pd_headword: str,
    pd_meaning: str,
    burrow_by_ded: Dict[str, BurrowParagraph],
    strict: bool = False,
) -> List[ValidationResult]:
    """Validate a proto-branch using ONLY its direct language entries (not descendants)."""
    ded = branch.ded_number
    direct_entries = branch.language_entries  # Only direct children, not recursive

    if not direct_entries:
        return []

    paragraph = burrow_by_ded.get(ded) if ded else None
    burrow_atts = paragraph.attestations if paragraph else []
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
            starling_meaning=entry.meaning,
        )

        if not ded:
            vr.notes = "Branch has no DED number"
            results.append(vr)
            continue

        if not burrow_atts:
            vr.notes = f"DED {ded} not found in Burrow corpus"
            results.append(vr)
            continue

        (
            matched,
            match_type,
            confidence,
            att,
            notes,
            parsed_gloss,
            matched_burrow_lang,
            matched_burrow_form,
            matched_burrow_gloss,
        ) = _match_entry(
            entry,
            burrow_atts,
            strict=strict,
            attestation_full_text=paragraph.full_text if paragraph else "",
        )

        vr.matched = matched
        vr.match_type = match_type
        vr.match_confidence = confidence
        vr.notes = notes

        if att:
            vr.burrow_headword = matched_burrow_form or ", ".join(att.headwords)
            vr.burrow_gloss = att.gloss
            vr.burrow_language_abbrev = matched_burrow_lang or att.language_abbrev
            vr.burrow_gloss_parsed = parsed_gloss
            if matched_burrow_gloss:
                vr.burrow_gloss = matched_burrow_gloss

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
    value = str(text or "").strip().lower()
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


