"""
Enhanced Burrow entry parser.

Extracts detailed language attestations from full DED entries.

Handles multiple HTML structures found on the DSAL site:
  Pattern A: <b><i>Lang.</i> headword</b>       (bold wraps both)
  Pattern B: <i>Lang.</i> <b>headword</b>        (italic lang, bold headword)
  Pattern C: <i><b>Lang.</b></i> <b>headword</b> (bold inside italic for lang)
  Pattern D: <i>Lang</i> <b>headword</b>         (no period, e.g. Konḍa, Kui)
  Pattern E: <i>Lang.</i> headword               (plain-text headword, no <b> wrapper;
                                                   e.g. inside <b><i>fem.</i> … <i>Ko.</i> aṛy</b>)

Optional qualifiers like (S.2), (A.), (Tr.), (F) may appear between
the language abbreviation and the headword.

Language name resolution delegates to dialect_mapping as the single
source of truth for Starling-to-Burrow mappings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import re
import unicodedata

from bs4 import BeautifulSoup
from bs4.element import Tag


@dataclass
class LanguageAttestation:
    """Single language form within a Burrow entry."""

    language_abbrev: str
    language_name: str
    headwords: List[str]
    gloss: str
    source_text: str


# Burrow abbreviations that are NOT language markers.
# Compared against the period/paren-stripped form produced by _is_valid_lang,
# so entries are stored bare (e.g. "Voc", not "Voc.").
_INVALID_LANG_ABBREVS = {
    "Voc",
    "CDIAL",
    "DED",
    "DEDS",
    "DEDR",
    "Turner",
    "Cf",
    "Skt",
    "Pkt",
    "OIA",
    "MIA",
    "IA",
    "H",
    "Mar",
    "Pers",
    # Pattern E false positives that the length guard does not catch
    # (see docs/issues/issue_pattern_e_false_positives.md).
    # Section-heading artifact and short bibliographic abbreviations.
    "Language",
    "Gramm",
    "Divy",
    "Nachträge",
    "Uṇ",
    # Sanskrit / Old Kannada text and lexicon titles (≤10 chars, so not
    # caught by the length guard; the longer ones are also covered by it).
    "Mahāpūrāṇa",
    "Mahāpurāṇa",
    "Śabdaratnākara",
    "Yaśastilaka",
    "Jasaharacariu",
    # Botanical genus names (too short for the length guard). The first four
    # are from the issue doc's table; the rest were surfaced by the v5 reparse
    # diff (same class of false positive).
    "Ficus",
    "Oxalis",
    "Physalis",
    "Tribulum",
    "Anaphilis",
    "Avicennia",
    "Leucas",
    "Oryza",
    "Phlomis",
    "Phoenix",
    "Polygala",
    "Stromatens",
}

# Character class for language abbreviation characters (including diacritics)
_LANG_CHAR = r"[A-ZÀ-ÖØ-öø-ÿĀ-žḀ-ỹ]" r"[a-zÀ-ÖØ-öø-ÿĀ-žḀ-ỹ.()]*"

# Leading lettered sub-entry marker glued onto the first language of a
# sub-entry inside the same <i> tag, e.g. "<i>(a) Ta.</i>". The DSAL HTML
# attaches "(a) "/"(b) "/... to the sub-entry-initial language, producing
# abbreviations like "(a) Ta." that would otherwise fail _is_valid_lang.
_SUBENTRY_MARKER_RE = re.compile(r"^\(\s*[a-z]\s*\)\s*")

# Inline (non-capturing) version of the sub-entry marker, consumed right after
# the opening <i> tag in the patterns below so that group(1) captures the bare
# language abbreviation. The DSAL HTML glues "(a) "/"(b) "/... onto the
# sub-entry-initial language (e.g. "<b><i>(a) Ta.</i> oru</b>"); without this,
# Pattern A's _LANG_CHAR -- which must start uppercase -- skips the whole span
# and Tamil is silently dropped. The class is deliberately narrow (a single
# parenthesised lowercase letter) so it cannot swallow real abbreviations or
# between-language qualifiers like "(S.2)"/"(Tr.)".
_OPT_SUBENTRY = r"(?:\(\s*[a-z]\s*\)\s*)?"

# Compiled patterns for language markers in order of specificity.
# Each yields (lang_abbrev, headword_text, match_object).
_PATTERNS = [
    # Pattern A: <b><i>Lang.</i> headword</b>
    re.compile(
        r"<b><i>" + _OPT_SUBENTRY + r"(" + _LANG_CHAR + r"\.?)</i>\s+([^<]+)</b>",
        re.DOTALL,
    ),
    # Pattern C: <i><b>Lang.</b></i> <b>headword</b>
    re.compile(
        r"<i><b>" + _OPT_SUBENTRY + r"(" + _LANG_CHAR + r"\.?)</b></i>"
        r"\s*(?:\([^)]*\)\s*)*"
        r"<b>([^<]+)</b>",
        re.DOTALL,
    ),
    # Pattern B/D: <i>Lang.</i> ... <b>headword</b>
    # Allows optional qualifiers like (S.2), (A.), (Tr.) between lang and headword
    re.compile(
        r"<i>" + _OPT_SUBENTRY + r"(" + _LANG_CHAR + r"\.?)</i>"
        r"\s*(?:\([^)]*\)\s*)*" r"<b>([^<]+)</b>",
        re.DOTALL,
    ),
    # Pattern E: <i>Lang.</i> plain-text-headword (no <b> wrapper on headword)
    # Handles cases where a grammatical qualifier (fem., pl., etc.) opens an outer
    # <b> block and the language marker + headword appear as plain text inside it,
    # e.g. <b><i>fem.</i> aṭiyātti. <i>Ko.</i> aṛy</b>
    # Negative lookbehind avoids re-matching <b><i>Lang.</i> headword</b> (Pattern A).
    re.compile(
        r"(?<!<b>)<i>" + _OPT_SUBENTRY + r"(" + _LANG_CHAR + r"\.?)</i>"
        r"\s*(?:\([^)]*\)\s*)*"
        r"([^\s<;(][^<;(]*?)(?=\s*[;<(]|\s*</?[bi])",
        re.DOTALL,
    ),
]


def _normalize_language(lang_abbrev: str) -> str:
    """
    Convert a Burrow abbreviation to its full language name.

    Delegates to dialect_mapping.normalize_burrow for known abbreviations.
    Falls back to stripping the trailing period.
    """
    try:
        from dialect_mapping import normalize_burrow

        result = normalize_burrow(lang_abbrev)
        if result != lang_abbrev:
            return result
    except ImportError:
        pass

    return lang_abbrev.rstrip(".")


def _clean_lang_abbrev(raw: str) -> str:
    """Normalize whitespace and strip a leading sub-entry marker (e.g. "(a) ")."""
    return _SUBENTRY_MARKER_RE.sub("", raw.strip()).strip()


def _is_valid_lang(abbrev: str) -> bool:
    """Check if an abbreviation looks like a real language marker.

    Pattern E matches plain-text headwords with no <b> wrapper, so without
    these guards it picks up capitalised non-language tokens (text titles,
    botanical names, bibliographic abbreviations). See
    docs/issues/issue_pattern_e_false_positives.md.
    """
    # Strip trailing periods AND parentheses so "Gramm.)" -> "Gramm",
    # "Uṇ." -> "Uṇ", "Divy." -> "Divy" all match the block-list and so a
    # stray ")" left in the abbreviation by the source HTML is ignored.
    clean = abbrev.strip().rstrip(".)(").strip()
    if not clean:
        return False
    if clean in _INVALID_LANG_ABBREVS:
        return False
    # Real Dravidian abbreviations are 2–6 chars; long tokens are text titles
    # or species strings (e.g. "Wrightiaantidysenterica" is 23 chars).
    if len(clean) > 10:
        return False
    if not clean[0].isupper():
        return False
    return True


# Burrow marks vowel length with a raised dot after the vowel (te·l = tEl, twa· = twA);
# Starling writes the same length as a macron, already removed by NFKD + strip-combining.
# Two confusable dots occur in the corpus (U+0387 dominant, U+00B7), plus IPA length marks.
# Kept in sync with starling_tree_validator._LENGTH_DOTS.
_LENGTH_DOTS = {ord(c): None for c in "\u00b7\u0387\u02d0\u02d1"}

# Starling writes the velar nasal as eng (\u014b); Burrow uses \u1e45 (n + combining dot
# above), which NFKD reduces to plain "n". Both are notational variants of the
# same phoneme /\u014b/, so fold eng to "n" to reconcile the two orthographies.
# Kept in sync with starling_tree_validator._ENG_FOLD.
_ENG_FOLD = {ord("\u014b"): "n", ord("\u014a"): "n"}  # \u014b, \u014a -> n


def _normalize_for_match(text: str) -> str:
    """Normalize headwords for robust matching: strip diacritics, stars, hyphens.

    Underscores are removed too: Starling encodes diacritics in ASCII with a
    trailing underscore (``in_r_u`` for Burrow's ``iṉṟu``), so stripping ``_``
    here lets that notation reconcile with Burrow's diacritic forms after NFKD.

    Length dots (Burrow's raised-dot vowel-length mark) are stripped so they
    reconcile with Starling's macron notation -- see ``_LENGTH_DOTS``.

    Eng (ŋ) is folded to "n" so Starling's IPA velar-nasal notation reconciles
    with Burrow's ṅ (which NFKD reduces to "n") -- see ``_ENG_FOLD``.
    """
    base = (
        text.replace("*", "")
        .replace("_", "")
        .replace("-", " ")
        .replace("(", " ")
        .replace(")", " ")
        .strip()
        .lower()
        .translate(_LENGTH_DOTS)
        .translate(_ENG_FOLD)
    )
    decomposed = unicodedata.normalize("NFKD", base)
    filtered = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(filtered.split())


@dataclass
class _LangSpan:
    """Internal: a language marker found in entry HTML with its position."""

    lang_abbrev: str
    headword_text: str
    start: int
    end: int


class BurrowEntryParser:
    """
    Parse detailed Burrow entries from page HTML.
    Extracts individual language attestations with headwords and glosses.
    """

    def clean_ded_number(self, ded_str: str) -> str:
        """Clean DED number: strip leading zeros. '045' -> '45', '0047' -> '47'."""
        try:
            return str(int(float(ded_str)))
        except (ValueError, TypeError):
            return str(ded_str).strip()

    def extract_page_url(
        self, search_result_html: str, ded_number: str
    ) -> Optional[str]:
        """Extract page URL from search results for a given DED number."""
        soup = BeautifulSoup(search_result_html, "html.parser")
        raw_results = soup.find_all("div", class_="hw_result")
        results: List[Tag] = [r for r in raw_results if isinstance(r, Tag)]

        for result_div in results:
            blockquote = result_div.find("blockquote")
            if not isinstance(blockquote, Tag):
                continue

            full_text = blockquote.get_text(strip=True)
            if not full_text.startswith(str(ded_number)):
                continue
            if len(full_text) <= len(str(ded_number)) + 5:
                continue

            for link in result_div.find_all("a"):
                if not isinstance(link, Tag):
                    continue
                href_val = link.get("href")
                if isinstance(href_val, str) and "page=" in href_val:
                    return href_val
        return None

    # ------------------------------------------------------------------ #
    # Core extraction
    # ------------------------------------------------------------------ #

    def _find_all_lang_spans(self, entry_html: str) -> List[_LangSpan]:
        """
        Find all language attestation spans in entry HTML using multiple
        regex patterns. De-duplicates by start position (earlier pattern wins).
        """
        seen_starts: dict[int, _LangSpan] = {}

        for pattern in _PATTERNS:
            for m in pattern.finditer(entry_html):
                lang_abbrev = _clean_lang_abbrev(m.group(1))
                headword_text = m.group(2).strip()

                if not _is_valid_lang(lang_abbrev):
                    continue

                # De-duplicate: keep the first pattern that matched this position
                if m.start() not in seen_starts:
                    seen_starts[m.start()] = _LangSpan(
                        lang_abbrev=lang_abbrev,
                        headword_text=headword_text,
                        start=m.start(),
                        end=m.end(),
                    )

        return sorted(seen_starts.values(), key=lambda s: s.start)

    def _extract_gloss(
        self, entry_html: str, span: _LangSpan, next_span: Optional[_LangSpan]
    ) -> str:
        """Extract gloss text from after a headword to the next language marker."""
        start_pos = span.end
        if next_span:
            end_pos = next_span.start
        else:
            # Look for end markers
            end_pos = len(entry_html)
            for marker in [" / ", " DED", " DEDS", " DEDR", " <i>CDIAL"]:
                marker_pos = entry_html.find(marker, start_pos)
                if 0 < marker_pos < end_pos:
                    end_pos = marker_pos

        gloss_html = entry_html[start_pos:end_pos]
        gloss_soup = BeautifulSoup(gloss_html, "html.parser")
        text = gloss_soup.get_text(" ", strip=True)
        return " ".join(text.split())

    def parse_language_sections(
        self, html_content: str, ded_number: Optional[str] = None
    ) -> List[LanguageAttestation]:
        """
        Parse full entry HTML to extract language attestations.

        Two structures:
        1. Direct query: <div class="hw_result"><blockquote>...</blockquote></div>
        2. Page query: <div class="hw_result"><div><number>N</number>...</div>...</div>

        If ded_number provided, finds the specific entry within page results.
        """
        soup = BeautifulSoup(html_content, "html.parser")

        result_div_raw = soup.find("div", class_="hw_result")
        if not isinstance(result_div_raw, Tag):
            return []
        result_div: Tag = result_div_raw

        blockquote = result_div.find("blockquote")
        if isinstance(blockquote, Tag):
            entry_html = str(blockquote)
        else:
            raw_nested_divs = result_div.find_all("div", recursive=False)
            nested_divs: List[Tag] = [d for d in raw_nested_divs if isinstance(d, Tag)]
            if not nested_divs:
                return []

            if ded_number:
                clean_ded = self.clean_ded_number(ded_number)
                entry_html = None
                for div in nested_divs:
                    number_tag = div.find("number")
                    if isinstance(number_tag, Tag):
                        if number_tag.get_text(strip=True) == clean_ded:
                            entry_html = str(div)
                            break
                if entry_html is None:
                    return []
            else:
                entry_html = str(nested_divs[0])

        spans = self._find_all_lang_spans(entry_html)
        if not spans:
            return []

        attestations: List[LanguageAttestation] = []

        for i, span in enumerate(spans):
            next_span = spans[i + 1] if i + 1 < len(spans) else None

            headwords = [hw.strip() for hw in span.headword_text.split(",")]
            headwords = [
                hw for hw in headwords if hw and len(hw) > 1 and not hw.startswith("(")
            ]
            if not headwords:
                continue

            gloss_text = self._extract_gloss(entry_html, span, next_span)
            lang_name = _normalize_language(span.lang_abbrev)

            attestations.append(
                LanguageAttestation(
                    language_abbrev=span.lang_abbrev,
                    language_name=lang_name,
                    headwords=headwords,
                    gloss=gloss_text[:200],
                    source_text=f"{span.lang_abbrev} {', '.join(headwords)}",
                )
            )

        # Resolve lexicographic "id." glosses (idem = same as previous attestation).
        last_real_gloss = ""
        for att in attestations:
            g = att.gloss.strip()
            if g.lower() == "id.":
                if last_real_gloss:
                    att.gloss = last_real_gloss
            elif g.lower().startswith("id."):
                # e.g. "id.; extra note" → "<prev gloss>; extra note"
                if last_real_gloss:
                    suffix = g[3:].lstrip(";").strip()
                    att.gloss = f"{last_real_gloss}; {suffix}" if suffix else last_real_gloss
                last_real_gloss = att.gloss
            else:
                last_real_gloss = att.gloss

        return attestations

    # ------------------------------------------------------------------ #
    # Matching (used by older validation pipelines)
    # ------------------------------------------------------------------ #

    def find_matching_attestation(
        self,
        attestations: List[LanguageAttestation],
        starling_language: str,
        starling_headword: str,
    ) -> Optional[LanguageAttestation]:
        """
        Find matching attestation for a StarlingDB entry.
        Uses dialect_mapping.match_languages for language resolution.
        """
        try:
            from dialect_mapping import match_languages
        except ImportError:
            return None

        starling_hw_norm = _normalize_for_match(starling_headword)

        for att in attestations:
            result = match_languages(att.language_abbrev, starling_language)
            if not result.matched:
                continue

            for burrow_hw in att.headwords:
                burrow_hw_norm = _normalize_for_match(burrow_hw)
                if burrow_hw_norm == starling_hw_norm:
                    return att
                if (
                    burrow_hw_norm in starling_hw_norm
                    or starling_hw_norm in burrow_hw_norm
                ):
                    return att

        return None


if __name__ == "__main__":
    parser = BurrowEntryParser()

    # Test with DED 45 page-512 style HTML (Pattern B: separate italic/bold)
    sample_b = """
    <div class='hw_result'>
    <div>
    <number>45</number> <b><i>Ta.</i> toṉṉai</b> cup made of plantain or other leaf.
    <i>Ma.</i> <b>donna</b> cup made out of a leaf, for brahmans to drink pepper-water, etc.
    <i>Ka.</i> <b>donne, jonne</b> leaf-cup.
    <i>Tu.</i> <b>donnè</b> cup made of plantain leaves, etc.
    <i>Te.</i> <b>donne</b> cup made of leaves.
    <i>Ga.</i> (S.²) <b>dona</b> leaf-cup.
    <i>Go.</i> (A.) <b>ḍona</b> id. (<i>Voc.</i> 1613).
    <i>Konḍa</i> <b>done</b> id.
    <i>Manḍ.</i> <b>duna</b> id.
    <i>Kui</i> <b>ḍono</b>, (P.) <b>ḍoho</b> id.; <b>ḍoo</b> balance word in <b>kali ḍoo</b> leaf-cup.
    <i>Kuwi</i> (F) <b>dunnō</b> (Su.) <b>dono</b> id.; (Isr.) <b>ṭono</b> cup-like container made of leaves.
    / Turner, <i>CDIAL</i>, no. 6641, <b>dróṇa-</b> (e.g. <b>H. donā,</b> Mar. <b>ḍoṇā</b> leaf-cup).
    DED(S) 2913.
    </div>
    </div>
    """

    # Test with DED 63 style (Pattern A + C mixed)
    sample_a = """
    <div class='hw_result'>
    <div>
    <number>63</number> <b><i>Ta.</i> aṭaṅku (aṭaṅki-)</b> to submit, be subdued.
    <b><i>Ma.</i> aṭaṅṅuka</b> to be pressed down.
    <b><i>Ko.</i> aṛg- (aṛgy-)</b> to stop, be obedient.
    <b><i>Ka.</i> aḍaṅgu, aḍagu</b> to hide, be concealed.
    <i><b>Koḍ.</b></i> <b>aḍak- (aḍaki-)</b> to hold in closed hands.
    <b><i>Tu.</i> aḍēvuni, aḍēyuni</b> to be concealed, hide.
    <b><i>Te.</i> aḍãgu, aḍagu</b> to yield, submit.
    <b><i>Kol.</i> ḍāṅg- (ḍāṅkt-)</b> to hide.
    <b><i>Kur.</i> aṛknā</b> to knead.
    <b><i>Malt.</i> aṛge, aṛgese</b> to press down.
    DED (S, N) 56(a).
    </div>
    </div>
    """

    # Test with DED 46 (Pattern A: everything in bold)
    sample_c = """
    <div class='hw_result'>
    <div>
    <number>46</number> <b><i>Go.</i> accānā</b> (Tr.) to be cut.
    <b><i>Malt.</i> asye</b> to chisel. ?
    <b><i>Ka.</i> haccu, heccu</b> to cut in pieces.
    DEDS 7.
    </div>
    </div>
    """

    # Test Pattern E: <i>Lang.</i> plain-text headword inside outer <b> opened by fem.
    sample_e = """
    <div class='hw_result'>
    <div>
    <number>72</number> <b><i>Ta.</i> aṭi</b> foot (of a person).
    <b><i>fem.</i> aṭiyātti. <i>Ko.</i> aṛy</b> foot (measure);
    <b>ac</b> place below; <b>acgaṛ</b> place beneath an object.
    <b><i>Ma.</i> aṭi</b> foot, lower part.
    </div>
    </div>
    """

    test_cases = [
        ("DED 45 (mixed patterns B/D)", sample_b, "45"),
        ("DED 63 (patterns A/C)", sample_a, "63"),
        ("DED 46 (pattern A only)", sample_c, "46"),
        ("DED 72 (pattern E: plain-text headword inside fem. block)", sample_e, "72"),
    ]

    for label, html, ded in test_cases:
        print(f"\n{'=' * 70}")
        print(f"TESTING: {label}")
        print(f"{'=' * 70}")

        attestations = parser.parse_language_sections(html, ded)
        print(f"Extracted {len(attestations)} attestations:\n")

        for att in attestations:
            print(f"  {att.language_abbrev:<10} ({att.language_name})")
            print(f"    Headwords: {', '.join(att.headwords)}")
            if att.gloss:
                print(f"    Gloss: {att.gloss[:80]}...")
            print()
