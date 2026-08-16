"""
Shared text-normalization helpers for the DED/Starling cross-validation
pipeline.

These functions were previously duplicated (with "kept in sync" comments)
across starling_tree_validator.py, burrow_entry_parser.py, and
repair_burrow_corpus_glosses.py. Both sides of the pipeline MUST normalize
identically -- a Starling headword and a Burrow attestation only match if
they reduce to the same key -- so the single source of truth lives here.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, List, Optional


def clean_ded_number(raw: Any) -> Optional[str]:
    """Normalize a DED number to a plain integer string: '0047' -> '47'.

    Burrow occasionally suffixes a DED number with a parenthetical letter
    (e.g. "4896(a)"/"4896(b)") to mark a split entry, where Starling keys on
    the plain base number; fold the suffix away so both sides index alike.

    This is the validation/ledger-layer cleaner: keys produced here align with
    the tree validator's DED indexing. The scrape/corpus layer deliberately
    keeps split-entry suffixes distinct (see
    ``BurrowEntryParser.clean_ded_number``) so "4896(a)" and "4896(b)" remain
    separate corpus entries.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    m = re.match(r"^(\d+)\s*\([a-z]\)$", s)
    if m:
        s = m.group(1)
    try:
        cleaned = str(int(float(s)))
    except (ValueError, TypeError):
        return s if s else None
    # Starling uses a literal "0" as its own sentinel for "no Burrow DED
    # correspondence"; Burrow's DED numbering starts at 1, so this is never
    # a real entry and should be treated the same as a missing DED number.
    return cleaned if cleaned != "0" else None

# Burrow marks vowel length with a raised dot after the vowel (te·l = tēl, twa· = twā);
# Starling writes the same length as a macron, already removed by NFKD + strip-combining.
# Two confusable dots occur in the corpus (U+0387 dominant, U+00B7), plus IPA length marks.
LENGTH_DOTS = {ord(c): None for c in "··ːˑ"}

# Starling writes the velar nasal as eng (ŋ); Burrow uses ṅ (n + combining dot
# above), which NFKD reduces to plain "n". Both are notational variants of the
# same phoneme /ŋ/, so fold eng to "n" to reconcile the two orthographies.
ENG_FOLD = {ord("ŋ"): "n", ord("Ŋ"): "n"}  # ŋ, Ŋ -> n

# Starling writes barred i as ɨ (U+0268); Burrow writes the same notational
# variant as ï (i + diaeresis), which NFKD decomposes to plain "i". ɨ has no
# NFKD decomposition of its own and would otherwise survive normalization
# unfolded, breaking matches like Starling "aḍɨ- (aḍɨp-, aḍɨt-)" vs Burrow's
# "aḍï- (aḍïp-, aḍït-)" (DED 79 Kodagu). Part of the project's approved
# conservative transcription fold (2026-08-16 canonical-transcription policy,
# _CANONICAL_CORE_FOLD in reporting.py) -- folded here too so it also governs
# matching, not just the display-only canonical-form columns.
BARRED_I_FOLD = {ord("ɨ"): "i", ord("Ɨ"): "i"}  # ɨ, Ɨ -> i


def normalize_for_match(text: str) -> str:
    """Normalize headwords for robust matching: strip diacritics, stars, hyphens.

    Underscores are removed too: Starling encodes diacritics in ASCII with a
    trailing underscore (``in_r_u`` for Burrow's ``iṉṟu``), so stripping ``_``
    here lets that notation reconcile with Burrow's diacritic forms after NFKD.

    Length dots (Burrow's raised-dot vowel-length mark) are stripped so they
    reconcile with Starling's macron notation -- see ``LENGTH_DOTS``.

    Eng (ŋ) is folded to "n" so Starling's IPA velar-nasal notation reconciles
    with Burrow's ṅ (which NFKD reduces to "n") -- see ``ENG_FOLD``.
    """
    base = (
        text.replace("*", "")
        .replace("_", "")
        .replace("-", " ")
        .replace("(", " ")
        .replace(")", " ")
        .strip()
        .lower()
        .translate(LENGTH_DOTS)
        .translate(ENG_FOLD)
        .translate(BARRED_I_FOLD)
    )
    decomposed = unicodedata.normalize("NFKD", base)
    filtered = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(filtered.split())


def recover_attestation_gloss_from_full_text(
    full_text: str,
    source_abbrev: str,
    source_headwords: List[str],
    fallback_gloss: str,
) -> str:
    """
    Recover a fuller attestation gloss from paragraph full_text when cached
    attestation glosses are truncated.
    """
    headwords = [h.strip() for h in (source_headwords or []) if h and h.strip()]
    if not full_text or not source_abbrev or not headwords:
        return fallback_gloss

    normalized = re.sub(r"\s+", " ", full_text).strip()
    abbrev_esc = re.escape(source_abbrev.strip())
    # Anchor on the FULL comma-separated headword chain, not just the first
    # form -- anchoring on one token alone leaves the remaining alternate
    # spellings (e.g. Ka. "matti, maddi, mar̤ti") dangling in the
    # recovered tail, ahead of the real gloss prose.
    hw_esc = r"\s*,\s*".join(re.escape(h) for h in headwords)
    marker_re = re.compile(
        rf"{abbrev_esc}\s+(?:\([^)]*\)\s*)*{hw_esc}",
        re.IGNORECASE,
    )
    m_marker = marker_re.search(normalized)
    if not m_marker:
        return fallback_gloss

    tail = normalized[m_marker.end() :].strip()
    # Stop at the next top-level language token (e.g. "Malt.", "Ka.") so
    # one attestation does not consume following language segments.
    # Konḍa/Kui/Kuwi are the only language names in the whole inventory
    # written WITHOUT a trailing period, so the main alternative (which
    # requires one) silently walks past them -- added explicitly so
    # attestations followed by one of these three still get bounded
    # correctly.
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
        r"\s([A-Z][A-Za-zÀ-ÖØ-öø-ÿĀ-žḀ-ỿ]+\.|Konḍa|Kui|Kuwi)\s+\S",
        tail,
    ):
        tok = m.group(1)
        if tok in ignore_tokens:
            continue
        # Konḍa/Kui/Kuwi (unlike the period-bearing tokens above) can also
        # appear as an ordinary cross-reference mid-sentence, e.g. DED 3246's
        # "...(cf. Kui trēba; with loss of t-)..." inside Kuwi's OWN gloss --
        # not a new attestation. A real attestation-introducing mention is
        # never preceded by "cf.".
        if tok in ("Konḍa", "Kui", "Kuwi") and re.search(
            r"\bcf\.\s*$", tail[: m.start()], re.IGNORECASE
        ):
            continue
        tail = tail[: m.start()].strip()
        break

    tail = re.sub(r"\s+DEDS?\b.*$", "", tail, flags=re.IGNORECASE).strip()
    if not tail:
        return fallback_gloss

    return tail if len(tail) > len(fallback_gloss) else fallback_gloss
