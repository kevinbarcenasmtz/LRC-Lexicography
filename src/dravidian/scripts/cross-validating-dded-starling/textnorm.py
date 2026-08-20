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

    Burrow occasionally suffixes a DED number to mark a split entry, either
    with a parenthetical letter (e.g. "4896(a)"/"4896(b)") or a bare trailing
    letter (e.g. "3621A", where 3621 = "night" and 3621A = "bug"). Starling
    keys both split halves on the plain base number, so fold the suffix away
    here so both sides index alike -- the loader then merges the split halves'
    attestation pools under the base key, and each Starling branch still only
    matches the forms that textually exist in its half.

    This is the validation/ledger-layer cleaner: keys produced here align with
    the tree validator's DED indexing. The scrape/corpus layer deliberately
    keeps split-entry suffixes distinct (see
    ``BurrowEntryParser.clean_ded_number``) so "4896(a)"/"4896(b)" and "3621A"
    remain separate corpus entries.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    m = re.match(r"^(\d+)\s*(?:\([a-z]\)|[A-Za-z])$", s)
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

# Burrow marks nasalization (and occasionally a short/breve vowel) with a
# SPACING modifier letter placed after the vowel -- ˜ (U+02DC, e.g. cī˜kaṭi,
# ī˜ga) or ˘ (U+02D8, e.g. nū˘vu) -- where Starling uses the COMBINING mark
# (combining tilde U+0303 etc.), which NFKD + strip-combining already removes on
# the Starling side. Strip Burrow's spacing forms too so both sides drop the
# mark consistently and reconcile (same rationale as LENGTH_DOTS -- the feature
# is treated as non-distinctive for matching on BOTH sides, not just one).
SPACING_DIACRITICS = {ord("˜"): None, ord("˘"): None, ord("~"): None}

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

# Glottal stop is transcribed several ways across the two sources: Starling
# writes the full letter ʔ (U+0294); Burrow writes it as a modifier letter ˀ
# (U+02C0, e.g. Maria Gondi oˀ, Kuwi siṭˀ) OR as an apostrophe (rendered as the
# right single quote U+2019, e.g. Kurukh alra'ānā for Starling alraʔānā). These
# are one phoneme in three notations, so fold them all to a single sentinel (ʔ)
# so e.g. alraʔānā and alra'ānā reconcile. A consistent 1:1 fold, so it can
# only merge forms, never split a currently-matching pair (cf. the ẓ/r̤ and ŋ
# folds). The modifier/quote characters are not combining, so folding here in
# the pre-NFKD translate chain is safe.
GLOTTAL_FOLD = {
    ord("ˀ"): "ʔ",  # U+02C0 modifier letter glottal stop
    ord("ʼ"): "ʔ",  # U+02BC modifier letter apostrophe (glottalization)
    ord("’"): "ʔ",  # ' right single quote (Burrow's rendered apostrophe)
    ord("'"): "ʔ",  # U+0027 apostrophe
}

# Two IPA vowel letters where the sources pick different codepoints for the same
# phone: Starling writes the mid-central vowel as ǝ (U+01DD turned e), Burrow as
# ə (U+0259 schwa, DED 190 əyb/ǝyb); Starling writes the open-o as ɔ (U+0254,
# DED 3498 dɔṇḍE) where Burrow writes plain o (doṇḍE). Neither ǝ nor ə nor ɔ has
# an NFKD decomposition, so they would otherwise survive normalization unfolded.
# Fold each to the Burrow spelling. A 1:1 fold -- can only merge forms, never
# split a currently-matching pair (cf. the ẓ/r̤, ŋ, and barred-i folds).
IPA_VOWEL_FOLD = {ord("ǝ"): "ə", ord("ɔ"): "o"}  # U+01DD -> schwa, open-o -> o

# Toda's velarized lateral and its voiced sibilant are each written with
# different codepoints by the two sources. Starling writes the lateral as ɫ
# (U+026B, l + middle tilde); Burrow writes the SAME Toda phoneme variously as
# ł (U+0142, l + stroke), ḷ (U+1E37 -> "l" after NFKD), or plain l -- so the
# only common target reconciling all of Burrow's spellings is plain "l".
# Likewise Starling writes the sibilant as ʒ (U+0292 ezh) where Burrow writes ζ
# (U+03B6 Greek zeta) or plain z, so fold both to "z". Each character is 100%
# Toda-confined in each dataset (ɫ/ʒ Starling-only, ł/ζ Burrow-only), so these
# 1:1 folds only merge Toda forms -- never split a currently-matching pair in
# any language (cf. the ẓ/r̤, ŋ, and barred-i folds).
TODA_CONSONANT_FOLD = {
    ord("ɫ"): "l",  # U+026B  Starling Toda lateral   -> l
    ord("ł"): "l",  # U+0142  Burrow Toda lateral      -> l
    ord("ʒ"): "z",  # U+0292  ezh, Starling Toda       -> z
    ord("ζ"): "z",  # U+03B6  Greek zeta, Burrow Toda  -> z
}


def normalize_for_match(text: str) -> str:
    """Normalize headwords for robust matching: strip diacritics, stars, hyphens.

    Underscores are removed too: Starling encodes diacritics in ASCII with a
    trailing underscore (``in_r_u`` for Burrow's ``iṉṟu``), so stripping ``_``
    here lets that notation reconcile with Burrow's diacritic forms after NFKD.

    Length dots (Burrow's raised-dot vowel-length mark) are stripped so they
    reconcile with Starling's macron notation -- see ``LENGTH_DOTS``. Burrow's
    spacing nasalization/breve modifiers are stripped for the same reason --
    see ``SPACING_DIACRITICS``.

    Eng (ŋ) is folded to "n" so Starling's IPA velar-nasal notation reconciles
    with Burrow's ṅ (which NFKD reduces to "n") -- see ``ENG_FOLD``.

    Glottal stop is folded to a single sentinel so Starling's ʔ reconciles with
    Burrow's modifier ˀ and its apostrophe notation -- see ``GLOTTAL_FOLD``.

    Toda's lateral (Starling ɫ / Burrow ł) and sibilant (Starling ʒ / Burrow ζ)
    are folded to plain "l"/"z" so the two sources' Toda orthographies reconcile
    -- see ``TODA_CONSONANT_FOLD``.

    Hyphens are removed outright (not turned into a space): Starling marks the
    root/suffix boundary of a citation form with an internal hyphen (Brahui
    ``hamp-ing``, ``all-ī``, ``ir-aṭ``) that Burrow writes joined (``hamping``,
    ``allī``, ``iraṭ``). Deleting the hyphen -- rather than leaving a space the
    joined Burrow form can never match -- reconciles the two conventions.
    """
    # Burrow writes the tense/morphology parenthetical with spaces where Starling
    # uses commas -- Burrow "(-pp- -tt-)" vs Starling "(-pp-, -tt-)". Drop commas
    # that sit INSIDE a parenthetical only, so the two reconcile, while a top-level
    # comma-separated headword list (a genuine form separator -- the same
    # distinction the DravidiLex import makes, commit 703f775) is left intact.
    text = re.sub(r"\(([^()]*)\)", lambda m: "(" + m.group(1).replace(",", "") + ")", text)
    # Burrow occasionally leaves a stray space around the "/" that separates
    # alternative stems within one citation form -- Burrow "nolt-/ noṭ-" vs
    # Starling "nolt-/noṭ-". Collapse whitespace around "/" so the two reconcile
    # (recurs across Kannada/Kui/Telugu/Naiki/Kota/Kodagu/Muria/Toda).
    text = re.sub(r"\s*/\s*", "/", text)
    base = (
        text.replace("*", "")
        .replace("_", "")
        .replace("-", "")
        .replace("(", " ")
        .replace(")", " ")
        .strip()
        .lower()
        .translate(LENGTH_DOTS)
        .translate(SPACING_DIACRITICS)
        .translate(ENG_FOLD)
        .translate(BARRED_I_FOLD)
        .translate(GLOTTAL_FOLD)
        .translate(IPA_VOWEL_FOLD)
        .translate(TODA_CONSONANT_FOLD)
    )
    decomposed = unicodedata.normalize("NFKD", base)
    # Retroflex zh (Tamil/Malayalam ழ, /ɻ/): Starling writes it ẓ (z + U+0323
    # combining dot below), Burrow writes it r̤ (r + U+0324 combining diaeresis
    # below) -- the same phoneme in two transliteration conventions. Fold both
    # to a single sentinel HERE, after NFKD but before combining marks are
    # stripped: once the marks are gone the two would read as incompatible "z"
    # vs "r" and never match (e.g. DED 11 akaẓ/akar̤, DED 84 aṭa-maẓa/aṭa-mar̤a).
    # Deliberately surgical -- r + U+0323 (ṛ) is a DIFFERENT phoneme and is left
    # untouched; only the exact zh sequences are folded.
    decomposed = decomposed.replace("ẓ", "ẓ").replace("r̤", "ẓ")
    filtered = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(filtered.split())


def _looks_like_form_token(token: str) -> bool:
    """Heuristic: a transliterated Dravidian headword token vs an English
    gloss word. Burrow's English glosses are plain ASCII; the transliterated
    forms carry a diacritic, a raised length-dot, or a morpheme hyphen. (Same
    signal used by gloss_extraction._is_inline_citation_noise.)"""
    t = token.strip().strip(",;.()[]")
    if not t:
        return False
    if not t.isascii():
        return True
    if "-" in t or "·" in token or "·" in token:
        return True
    return False


def antecedent_is_multiform(gloss: str) -> bool:
    """True when a resolved antecedent gloss lists more than one form-entry.

    Burrow separates the form-entries of one language section with ``;`` --
    ``head-form meaning; second-form meaning; ...`` -- while commas stay inside
    a single sense list. A section is *multi-form* when a ``;`` segment after
    the first begins with a transliterated Dravidian form token.

    An ``id.`` (idem) reference points at the meaning of ONE specific antecedent
    form, not the whole section. When the antecedent has a single form the
    reference is unambiguous and can be expanded by whole-copy; when it has
    several forms the correct antecedent is editorial, not positional (Burrow's
    convention -- a compound/derived cognate may match a non-head sense), so no
    string rule resolves it reliably. This predicate flags that ambiguous case
    so the ``id.`` resolvers can leave the literal ``id.`` in place (faithful to
    Burrow) rather than glue in the wrong meaning. See
    ``docs/dravidian_validator_progress.md`` s6 (id.-expansion over-glue).
    """
    segments = [s.strip() for s in (gloss or "").split(";") if s.strip()]
    for segment in segments[1:]:
        tokens = segment.split()
        if tokens and _looks_like_form_token(tokens[0]):
            return True
    return False


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
