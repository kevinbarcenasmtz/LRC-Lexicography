"""
Micro-parsing of Burrow consolidated glosses: per-dialect inline forms
("(Tr. W.) askAnA ..."), "id." cross-references, and citation-noise
filtering. Used by the matching layer in starling_tree_validator.py.
"""

from __future__ import annotations

import re
from typing import List, Tuple


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


# A dialect/citation marker group like "Tr.", "Ph.", "Tr. W.", "A. Ch. Mu.
# Ma." -- as opposed to an ordinary gloss parenthetical. Shared by
# extract_gloss_forms_for_abbrevs and truncate_gloss_before_first_marker.
_DIALECT_MARKER_GROUP_RE = re.compile(r"^(?:[A-Za-z]+\.)+(?:\s+[A-Za-z]+\.)*$")


def truncate_gloss_before_first_marker(gloss: str) -> str:
    """
    Trim a consolidated Burrow entry's gloss at the first dialect/citation
    marker, e.g. Kuwi's "(F.) vegu" form stores its gloss as
    ", (Su. Isr.) vegu (pl. veska), (P.) vergu (pl. verka) id.; (S.) weggu,
    weska dry wood." -- text that genuinely belongs to OTHER dialects'
    citations, not the primary (F.) form being matched here.

    Used only when the primary headword is matched directly (the
    gloss_dialect_* path in _match_entry already extracts a dialect-specific
    meaning and never reaches this function) -- the primary form's own
    marker (e.g. "(F.)") was already consumed during parsing as a headword
    qualifier, so it never appears in the gloss for this function to find;
    there is no way to recover its true distinct meaning, only to stop
    displaying unrelated dialects' text in its place. Returns an empty
    string when there's no text before the first marker (the common case --
    primary forms are often grouped under a single shared "id." at the end),
    which is more honest than showing irrelevant content, and avoids a
    false meaning-mismatch flag (since _is_meaning_mismatch treats an empty
    side as "nothing to compare", not a mismatch).
    """
    for marker_match in re.finditer(r"\(([^)]+)\)\s*", gloss):
        if _DIALECT_MARKER_GROUP_RE.match(marker_match.group(1).strip()):
            return gloss[: marker_match.start()].strip(" ;,")
    return gloss


_INLINE_PAREN_NOISE_RE = re.compile(r"^\([^)]*\)\s*(?:id\.?)?$", re.IGNORECASE)
_INLINE_BARE_WORD_RE = re.compile(r"^[^\s,()]+\.?$")


def _is_inline_citation_noise(meaning_text: str) -> bool:
    """
    True when an inline dialect marker's leftover "meaning" text is really
    just citation noise -- a bare grammatical-number parenthetical like
    "(pl. veska)", a trailing "id." cross-reference, or a second alternate
    headword spelling -- rather than a genuine gloss.

    The hard case is a bare single word with no parens/commas: that shape
    is identical whether it's a leaked Dravidian headword variant (noise,
    e.g. "veẖki") or a real one-word English gloss (real data, e.g.
    "shelter") -- pure syntax can't tell them apart. Burrow's English
    glosses are always plain ASCII; only the transliterated Dravidian forms
    carry diacritics, so a bare word is only treated as noise when it
    contains a non-ASCII character. This fails safe: an ASCII-only leaked
    form (e.g. "ukka") is left unfixed rather than risk discarding a real
    gloss (confirmed against real data: an unguarded bare-word check
    wrongly emptied 364 genuine ASCII glosses out of 457 cases).
    """
    if _INLINE_PAREN_NOISE_RE.match(meaning_text):
        return True
    if _INLINE_BARE_WORD_RE.match(meaning_text) and not meaning_text.isascii():
        return True
    return meaning_text.rstrip(".,; )").lower() == "id"


def extract_gloss_forms_for_abbrevs(
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
        # Keep only dialect/citation marker groups and skip gloss
        # parentheticals.
        if _DIALECT_MARKER_GROUP_RE.match(marker_text):
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

        normalized_marker = " ".join(part.strip().strip(".") + "." for part in group_text.split())

        # Parse abbreviation tokens from group: "Tr. W." Ã¢â€ â€™ {"Tr.", "W."}
        group_abbrevs: set[str] = set()
        for part in re.split(r"\s+", group_text.strip()):
            part = part.strip()
            if part and not part.endswith("."):
                part += "."
            if part:
                group_abbrevs.add(part)

        if target_set.isdisjoint(group_abbrevs):
            continue

        if following_token.lower() in _GLOSS_STOPWORDS:
            # No distinct form token for this marker; use the primary
            # headword, but this is not an id. reference.
            if not primary_headword:
                continue
            _, fallback_meaning = _split_attached_to(following_token, meaning_text)
            fallback_meaning = _clean_inline_meaning(fallback_meaning)
            forms.append((primary_headword, False, normalized_marker, fallback_meaning))
            continue

        used_id = False
        if meaning_text and _is_inline_citation_noise(meaning_text):
            # Citation noise, not real prose -- comparing it against
            # Starling's gloss produces a false meaning mismatch, so treat
            # it as nothing to show instead (same convention as
            # truncate_gloss_before_first_marker).
            used_id = meaning_text.rstrip(".,; )").lower().endswith("id")
            meaning_text = ""
        else:
            following_token, meaning_text = _split_attached_to(following_token, meaning_text)
            meaning_text = _clean_inline_meaning(meaning_text)

        forms.append((following_token, used_id, normalized_marker, meaning_text))

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


def extract_id_reference_forms(
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
