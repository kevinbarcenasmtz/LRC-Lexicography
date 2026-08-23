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
  Pattern F: <i>Lang.</i></b> headword</b>      (lang marker closes a stray
                                                  outer <b> opened by the
                                                  previous language's leftover
                                                  gloss text; headword is in
                                                  a fresh <b> right after)

Optional qualifiers like (S.2), (A.), (Tr.), (F) may appear between
the language abbreviation and the headword.

Language name resolution delegates to dialect_mapping as the single
source of truth for Starling-to-Burrow mappings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import html
import re

from bs4 import BeautifulSoup
from bs4.element import Tag

from textnorm import antecedent_is_multiform, normalize_for_match


# Single-character headwords are normally noise (stray captured letters) and are
# filtered out during extraction. The sole legitimate exception is a bare vowel
# grapheme, which is the actual headword of the deictic/pronominal base entries
# (Ta. a "remoter base", DED 1; Ta. i "proximate base", DED 410; Ta. u, DED 557;
# etc.). Without this whitelist the entry's own headword language -- usually the
# Ta./Ma. that defines it -- is dropped wholesale, because its entire headword
# chain reduces to single vowels that the length guard would otherwise discard.
_VOWEL_HEADWORDS = frozenset("aāiīuūeēoō")


def _split_headword_chain(text: str) -> List[str]:
    """Split a Burrow headword chain on top-level commas only.

    Burrow lists conjugation-stem alternants inside parentheses right after
    a headword, e.g. "aṭu (-pp-, -tt-)" -- a naive comma split treats the
    comma inside the parens as a chain separator too, producing bogus
    headwords "aṭu (-pp-" and "-tt-)" (surfaced as the matched form for
    DED 79 Ta. in the audit report). Only split on a comma at paren depth 0,
    so the parenthetical group stays attached to its headword.
    """
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


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

# Character class for a language abbreviation's first letter -- reused on its
# own (not just inside _LANG_CHAR below) by _HEADWORD_SPAN_ACROSS_NESTED to
# recognise the start of a DIFFERENT language's <i> marker.
_LANG_CHAR_FIRST = r"[A-ZÀ-ÖØ-öø-ÿĀ-žḀ-ỹ]"

# Character class for language abbreviation characters (including diacritics)
_LANG_CHAR = _LANG_CHAR_FIRST + r"[a-zÀ-ÖØ-öø-ÿĀ-žḀ-ỹ.()]*"

# Leading lettered sub-entry marker glued onto the first language of a
# sub-entry inside the same <i> tag, e.g. "<i>(a) Ta.</i>". The DSAL HTML
# attaches "(a) "/"(b) "/... to the sub-entry-initial language, producing
# abbreviations like "(a) Ta." that would otherwise fail _is_valid_lang.
_SUBENTRY_MARKER_RE = re.compile(r"^\(\s*[a-z]\s*\)\s*")

# A grammatical/sense qualifier glued onto the FRONT of an "id." (idem)
# lexicographic reference, e.g. "(pl.) id." or "(obl. -r) id., arrowhead".
# Burrow attaches the qualifier to the SAME gloss as the "id." reference
# rather than treating it as a distinct meaning, so the simpler "id."/
# "id.;..." cases below never match (their gloss isn't exactly "id." and
# doesn't start with "id."), and the chained-back real meaning never gets
# substituted in -- the attestation is left showing the bare qualifier
# plus a literal "id." placeholder instead of its real sense.
_QUALIFIED_ID_RE = re.compile(r"^\(([^)]*)\)\s*id\.\s*(.*)$", re.IGNORECASE)

# Matches a leading qualifier on an ALREADY-chained-back gloss (e.g. the
# preceding attestation in the SAME chain also resolved a qualified "id."
# and itself starts with "(its own qualifier) real meaning"). Stripped
# before re-prepending the CURRENT attestation's own qualifier, so a chain
# of several qualified "id." attestations (Konḍa -> Pe. -> Manḍ. in DED
# 5440) each show "(own qualifier) real meaning" rather than accumulating
# every earlier link's qualifier too (which would also break Starling's
# substring-meaning-match check downstream).
_LEADING_PAREN_RE = re.compile(r"^\([^)]*\)\s*")

# Inline (non-capturing) version of the sub-entry marker, consumed right after
# the opening <i> tag in the patterns below so that group(1) captures the bare
# language abbreviation. The DSAL HTML glues "(a) "/"(b) "/... onto the
# sub-entry-initial language (e.g. "<b><i>(a) Ta.</i> oru</b>"); without this,
# Pattern A's _LANG_CHAR -- which must start uppercase -- skips the whole span
# and Tamil is silently dropped. The class is deliberately narrow (a single
# parenthesised lowercase letter) so it cannot swallow real abbreviations or
# between-language qualifiers like "(S.2)"/"(Tr.)".
_OPT_SUBENTRY = r"(?:\(\s*[a-z]\s*\)\s*)?"

# A parenthetical qualifier glued INSIDE the <i> language marker, e.g.
# "<b><i>Nk. (Ch.)</i> ōn</b>". For Naiki the inventory knows the marker by
# this exact form ("Nk. (Ch.)"); for source tags ("Te. (SAN)", "Ka. (DCV)")
# the parenthetical is bibliographic and _clean_lang_abbrev strips it back to
# the base abbrev. Without this, _LANG_CHAR (which forbids the internal space)
# stops at "Nk." and every pattern fails on the " (Ch.)", dropping ~314 Naiki
# attestations across the corpus.
_OPT_LANG_QUALIFIER = r"(?:\s*\([^)<]*\))?"

# A grammatical/sense qualifier glued onto the FRONT of a language marker
# inside the same <i> tag, e.g. "<b><i>(tr.). Ka.</i> headword</b>" (Burrow
# attaches voice/transitivity/locative qualifiers -- (tr.), (intr.), (loc.)
# -- to the PRECEDING headword's sense, but the markup glues the closing
# "). " onto the FOLLOWING language marker's own <i> span instead of closing
# before it). _LANG_CHAR must start uppercase, so without this every
# _PATTERNS entry fails to anchor inside the <i> tag and the whole language
# is silently dropped (97 across the corpus). A rarer sibling shape leaks
# the opening "(" just outside the <i> tag instead of inside it (e.g.
# "<b>(<i>intr.). Ka.</i>") -- handled by the optional `\(?` in Pattern A's
# opener below, not here.
_OPT_LEADING_QUALIFIER = r"(?:\(?[^()<>]*\)\.\s*)?"

# Abbreviation capture group reused by every pattern below: a language
# abbreviation (_LANG_CHAR), an optional trailing period, and an optional
# in-marker parenthetical qualifier (_OPT_LANG_QUALIFIER).
_LANG_ABBREV = r"(" + _LANG_CHAR + r"\.?" + _OPT_LANG_QUALIFIER + r")"

# A parenthetical qualifier between the closing language marker and the
# headword, e.g. "<i>Ga.</i> (S.²) <b>dona</b>" or
# "<b><i>Go.</i> (Tr.) aḍrai id.</b>". Patterns B/C/F already skipped this
# before their separate <b>headword</b>; Pattern A lacked it, so when the
# qualifier and headword share Pattern A's single <b> span (no tag boundary
# to stop at) the qualifier got glued onto the captured headword text --
# and the headword-cleanup filter drops any string starting with "(",
# silently discarding the whole attestation (Go. in DED 107, Kuwi in DED 83).
# A bare "?" (Burrow's uncertainty mark) sometimes stands in this position with
# no parentheses, e.g. DED 4143 "<i><b>Tu.</b></i> ? <b>pēñci</b>"; skip it too
# so the following <b>headword</b> is still reached. (A real headword never
# begins with "?", so this cannot swallow form content.)
_OPT_HEADWORD_QUALIFIER = r"\s*(?:\?\s*)?(?:\([^)]*\)\s*)*"

# Headword content for Patterns B/D, C, F, allowed to span across a nested
# non-<b> tag (grammatical <i>pl.</i>/<i>obl.</i> qualifier, <at>...</at>
# artifact) instead of stopping at it. Unlike Pattern A (fixed for the same
# underlying issue via a `(?=<)` lookahead -- safe there because Pattern A's
# headword has no closing tag of its own to lean on), these three patterns'
# headword sits in its OWN bounded <b>...</b> span, so naively stopping at
# the first tag truncates real headword content that comes after the nested
# tag closes (e.g. DED 5440's Kuwi "vegū (<i>pl.</i> veska)" would become
# just "vegū (", losing "veska)"). `<(?!/b>)` consumes any "<" that doesn't
# start the literal closing "</b>", so the capture spans transparently over
# nested tag-pairs and stops only at the headword's real closing tag.
# (Verified corpus-wide: no nested <b>...</b> pair ever occurs inside a
# B/D, C, or F headword span, so this can't be fooled by a genuinely nested
# bold span closing the capture early.) The captured text can now contain
# nested-tag markup (stripped by _HTML_TAG_RE in _find_all_lang_spans).
#
# Also excludes spanning into a bare <i>Uppercase...</i> -- a DIFFERENT
# language's own marker, not a grammatical qualifier -- which a plain
# `<(?!/b>)` would otherwise swallow whole when the previous language's
# headword <b> span doesn't close until after the next language's <i>
# marker (e.g. DED 4900: "<b>muŋgi pōtu. <i>Go.</i></b>" -- Ga.'s headword
# bold span stays open across Go.'s entire marker). Grammatical qualifiers
# (<i>pl.</i>, <i>obl.</i>) are always lowercase, so this only blocks real
# language transitions.
_HEADWORD_SPAN_ACROSS_NESTED = (
    r"(?:[^<]|<(?!/b>)(?!i>" + _LANG_CHAR_FIRST + r"))+"
)

# Strips nested-tag markup a headword capture may now contain (see
# _HEADWORD_SPAN_ACROSS_NESTED above), e.g. "vegū (<i>pl.</i> veska)" ->
# "vegū (pl. veska)". A no-op for patterns whose capture never contains "<"
# in the first place (A, E), so applied unconditionally to every match.
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# Compiled patterns for language markers in order of specificity.
# Each yields (lang_abbrev, headword_text, match_object).
_PATTERNS = [
    # Pattern A: <b><i>Lang.</i> headword</b>
    # Headword run ends at the next tag, not necessarily </b>: a nested tag
    # inside the bold span (grammatical <i>obl.</i> qualifier, italicised
    # scientific name, or <at>…</at> encoding artifact) otherwise breaks the
    # match and silently drops the language (~613 across the corpus).
    # The mandatory \s+ right after </i> (rather than folding into the
    # shared _OPT_HEADWORD_QUALIFIER, which allows zero whitespace) matters
    # here specifically: Patterns B/C/F anchor on a literal <b> right after
    # the optional qualifier, so a zero-whitespace qualifier-skip can't run
    # away into ordinary prose -- if there's no <b> immediately there, the
    # match just fails. Pattern A's capture has no such anchor (it runs to
    # the next tag of any kind), so without this boundary it swallowed
    # italicised non-language tokens glued directly to following punctuation
    # with no space, e.g. "<i>Artocarpus</i>; avekka" or
    # "<i>Cyprinus</i>; kayyan" (scientific names) and
    # "<i>Grammar</i>) xalxnā" (a citation-title qualifier) as bogus
    # languages.
    re.compile(
        r"<b>\(?<i>" + _OPT_LEADING_QUALIFIER + _OPT_SUBENTRY + _LANG_ABBREV
        + r"</i>\s+" + _OPT_HEADWORD_QUALIFIER + r"([^<]+)(?=<)",
        re.DOTALL,
    ),
    # Pattern C: <i><b>Lang.</b></i> <b>headword</b>
    # _OPT_LEADING_QUALIFIER mirrors Pattern A: the PREVIOUS language's trailing
    # grammatical qualifier is sometimes glued in front of this marker inside the
    # same bold span (e.g. DED 946 "<i><b>(intr.). Go.</b></i> (Tr.) <b>wōṛānā</b>"),
    # so the abbrev capture would otherwise start on "(" and fail _is_valid_lang,
    # dropping the whole language. Optional + non-capturing: normal
    # "<i><b>Go.</b></i>" markers are unaffected and group numbering is unchanged.
    # The optional "\.?" between "</b>" and "</i>" tolerates the abbrev's trailing
    # period landing OUTSIDE the bold span, "<i><b>Koḍ</b>.</i>" (DED 3918): the
    # abbrev then captures as "Koḍ" (no period), resolved by the "Koḍ" alias.
    re.compile(
        r"<i><b>" + _OPT_LEADING_QUALIFIER + _OPT_SUBENTRY + _LANG_ABBREV + r"</b>\.?</i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern CQ: <i><b>Lang.</b> (bibliographic qualifier)</i> <b>headword</b>
    # A source/dictionary citation sits AFTER the bold-wrapped abbrev but still
    # INSIDE the italic marker: DED 1563 "<i><b>Tu.</b> (Eng.-Tulu Dict.)</i>
    # <b>girige</b>", DED 5006 "<i><b>Ta.</b> (DCV)</i> <b>muṟaḷai</b>". Pattern C
    # needs "</b></i>" immediately (the citation breaks it), and CIT handles the
    # inverse shape where the "(" opens INSIDE the bold and its ")" lands after
    # the marker -- neither fires here. The bolded abbrev plus the mandatory
    # "(...)" between "</b>" and "</i>" keep this off ordinary "<i><b>Lang.</b></i>"
    # markers; the citation is discarded (only the headword bold is captured).
    re.compile(
        r"<i><b>" + _OPT_SUBENTRY + _LANG_ABBREV + r"</b>\s*\([^<]*\)</i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern C3: <i>(qualifier). <b>Lang.</b></i> <b>headword</b>
    # Leading qualifier / subentry marker as plain text INSIDE the italic but
    # BEFORE the bold-wrapped language marker (DED 3655
    # "<i>(neut.). <b>Go.</b></i> (Tr.) <b>nālung</b>", DED 2876
    # "<i>(a) <b>Kol.</b></i> <b>so·ŋg-</b>"). Distinct from Pattern C (qualifier
    # inside the bold) and C2 (qualifier in its own bold). With both leading
    # groups empty this collapses to "<i><b>Lang.</b></i>" == Pattern C, which
    # runs first and wins the position, so no double-count.
    re.compile(
        r"<i>" + _OPT_LEADING_QUALIFIER + _OPT_SUBENTRY + r"<b>"
        + _OPT_SUBENTRY + _LANG_ABBREV + r"</b></i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern CIT: <i><b>Lang. (CITATION</b></i> ...remainder) <b>headword</b>
    # A bibliographic source citation opens with "(" INSIDE the marker tag but
    # its closing ")" lands only after the marker, so the abbrev capture breaks
    # on the "(" and the whole language is dropped:
    #   DED 2686 "<i><b>Te. (TVB</b></i>, Guntur dial.; comm. by K.) <b>cūru</b>"
    #   DED 886  "<i><b>Te. (VPK</b></i>, intro. p. 123) <b>ēnu</b>"
    #   DED 931  "<i><b>Kol. (SSTW</b></i>, p. 83) <b>panta okeng</b>"
    #   DED 3884 "<i>Go. (LSI</i>, Kōi) <b>paṇi</b>"  (no inner <b>)
    # Capture the abbrev alone, consume the citation (its "(" inside the marker
    # through the first ")" after it), then the usual qualifier(s) before the
    # fresh <b>headword</b>. The MANDATORY "(" right after the abbrev keeps this
    # from firing on ordinary "<i><b>Lang.</b></i>" or "(Oll.)"-qualifier markers.
    re.compile(
        r"<i>(?:<b>)?" + _LANG_ABBREV + r"\s*\([^<>)]*(?:</b>)?</i>[^<>)]*\)\s*"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern C2: <i><b>(qualifier).</b> Lang.</i> <b>headword</b>
    # Same leading-qualifier root cause as Pattern C, but the qualifier sits in
    # its OWN <b>...</b> inside the <i>, with the language marker as plain text
    # after it (DED 3682 "<i><b>(tr.).</b> Go.</i> (Tr.) <b>nindānā</b>"). Neither
    # Pattern C (needs Lang inside the bold) nor B/D (needs Lang right after <i>)
    # anchors here. The required bolded parenthetical before the marker keeps this
    # from firing on ordinary <i>Lang.</i> spans.
    re.compile(
        r"<i><b>\([^)]*\)\.?</b>\s*" + _LANG_ABBREV + r"</i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern B/D: <i>Lang.</i> ... <b>headword</b>
    # Allows optional qualifiers like (S.2), (A.), (Tr.) between lang and headword
    re.compile(
        r"<i>" + _OPT_SUBENTRY + _LANG_ABBREV + r"</i>"
        + _OPT_HEADWORD_QUALIFIER + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern F: <i>Lang.</i></b> ... <b>headword</b>
    # The previous language's leftover gloss text leaks into an outer <b>
    # span that doesn't close until just past this language's <i>Lang</i>
    # marker (e.g. Kui's "(obl. for all mā-)." sits inside a <b> that closes
    # right after "<i>Kuwi</i>"); the headword is in a fresh <b> after.
    re.compile(
        r"<i>" + _OPT_SUBENTRY + _LANG_ABBREV + r"</i>\s*</b>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Pattern E: <i>Lang.</i> plain-text-headword (no <b> wrapper on headword)
    # Handles cases where a grammatical qualifier (fem., pl., etc.) opens an outer
    # <b> block and the language marker + headword appear as plain text inside it,
    # e.g. <b><i>fem.</i> aṭiyātti. <i>Ko.</i> aṛy</b>
    # Negative lookbehind avoids re-matching <b><i>Lang.</i> headword</b> (Pattern A).
    re.compile(
        r"(?<!<b>)<i>" + _OPT_SUBENTRY + _LANG_ABBREV + r"</i>"
        r"\s*(?:\([^)]*\)\s*)*"
        r"([^\s<;(][^<;(]*?)(?=\s*[;<(]|\s*</?[bi])",
        re.DOTALL,
    ),
    # Pattern G: <i><b>Lang.</b></i> <lone-vowel headword, plain text>
    # The deictic/pronominal base entries write their leading headword-language
    # marker in Pattern-C markup (<i><b>Lang.</b></i>) but the headword itself is
    # a bare vowel in PLAIN text, not <b>-wrapped, followed directly by the
    # running-prose gloss (DED 1: "<i><b>Ta.</b></i> a demonstr. base expr..."):
    # Pattern C needs a <b>headword</b>, Pattern E needs a bare <i>Lang.</i>, so
    # neither fires and the entry's own defining language is dropped. A lone
    # vowel is the quality guard -- an English gloss word is never a single vowel
    # -- so this cannot capture prose. Only 1 match corpus-wide today (DED 1 Ta.).
    re.compile(
        r"<i><b>" + _OPT_SUBENTRY + _LANG_ABBREV + r"</b></i>\s+"
        r"([aāiīuūeēoō](?:,\s*[aāiīuūeēoō])*)(?=[\s.;])",
        re.DOTALL,
    ),
]

# Patterns that scan INTO an <i> span (not anchored on "<i>Abbrev" at the span
# start) and so require the stricter _is_known_lang_abbrev gate in
# _find_all_lang_spans, not just _is_valid_lang.
#
# To.-type: a language marker buried at the END of an <i> span, after lowercase
# qualifier/scientific-name text, with NO <b> (or "<b>(") immediately before the
# <i> -- malformed DSAL markup where the closing </i> lands after the abbrev
# instead of before it. Two real shapes:
#   DED 5154  "...am<-> <i>incl.). To.</i> em</b>"  (sense qualifier before abbrev)
#   DED 5     "<i>Tu.</i> agase-mara <i>Agati grandiflora. Te.</i></b> (B) <b>agase-...">
#             (scientific name before abbrev; headword in a fresh <b> after a source tag)
# No other _PATTERNS entry can anchor here -- they all need the abbrev at the
# <i> span start. The leading "(?<!<b>)(?<!<b>\()" excludes Pattern A and the
# leading-qualifier shape (which keep the abbrev at the <i> start, just behind a
# <b> or "<b>("), so this only fires on the genuinely-embedded case.
#
# Two shapes, distinguished by whether a <b> immediately precedes the <i>:
#
#  (1) NO <b> before <i>, headword in its OWN <b>...</b> (_TOTYPE_NOT_BOLD +
#      trailing "<b>(headword)</b>"). The plain-text-headword flavour of THIS
#      shape stays omitted, because where a form is elided the marker is
#      followed directly by the English gloss in running text (e.g. DED 814
#      "erukku <i>Calotropis gigantea. Ma.</i> gigantic swallow-wort...") and a
#      text capture would wrongly store the gloss as the headword.
#
#  (2) <b> IMMEDIATELY before <i>, PLAIN-TEXT headword in the same bold run
#      (_TOTYPE_BOLD_EMBEDDED, added below). The editor opens a fresh <b> only
#      when a real headword follows, so the mandatory "<b>" here is exactly the
#      signal that separates a form-present case (safe to capture) from the
#      elided-gloss case above (which has no <b> before its marker <i>). Sized
#      corpus-wide at 216 unparsed spans (Ma 114, Ka 34, Te 27, Tu 15, Koḏ 10,
#      To 5, Ko 4, Pa 4, +3), 0 English-gloss contamination -- the scientific
#      name is the PREVIOUS language's gloss-tail wrongly folded into this <i>.
_TOTYPE_NOT_BOLD = r"(?<!<b>)(?<!<b>\()"
_TOTYPE_PRE = r"<i>[^<]*?[a-z][^<]*?"
# First char of a Dravidian transliteration form; gates the plain-text headword
# capture in shape (2) so a capitalised follow-token can't be taken as a form.
_FORM_FIRST = r"[a-zāīūēōṛṝḷṇṭḍḷṅñśṣḻ]"
_STRICT_PATTERNS = [
    re.compile(
        _TOTYPE_NOT_BOLD + _TOTYPE_PRE + _LANG_ABBREV + r"</i>(?:\s*</b>)?"
        + _OPT_HEADWORD_QUALIFIER + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Shape (2): <b><i>...sci-name. Abbrev.</i> plain-text-headword <...
    re.compile(
        r"<b>\s*" + _TOTYPE_PRE + _LANG_ABBREV
        + r"</i>\s+(?:\([^)]*\)\s*)*(" + _FORM_FIRST + r"[^<]*)(?=<)",
        re.DOTALL,
    ),
    # Shape (3): <i><b>...sci-name. Abbrev.</b></i> <b>bold-headword</b>
    # The <i><b> (bold-order) analog of shape (2): the previous language's
    # trailing scientific name is folded into this marker's bold span before the
    # abbrev, with the headword in a fresh <b> (DED 2730
    # "<i><b>Zizyphus oenoplia. Ka.</b></i> (Lush.) <b>suri-muḷḷu</b>"). The
    # mandatory lowercase-text-then-space before the abbrev keeps it off ordinary
    # "<i><b>Ka.</b></i>" markers; the _is_known_lang_abbrev gate (applied in the
    # strict-pattern loop) prevents capturing the scientific name itself as a lang.
    re.compile(
        r"<i><b>[^<]*?[a-z][^<]*?\s" + _LANG_ABBREV + r"</b></i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Shape (4): <i>sci-name. <b>Abbrev.</b></i> <b>headword</b>
    # The previous language's trailing scientific binomial and the next language's
    # marker share one <i> span, but here ONLY the abbrev is bolded (the sci-name
    # is plain text inside the italic) -- the inverse of shape (3), where the
    # sci-name sits INSIDE the bold. DED 3824 "...vayaḷai purslane, <i>P.
    # quadrifida. <b>Ma.</b></i> <b>pacaḷa, paśaḷa</b>", DED 3755 "...<i>Phyllanthus
    # emblica. <b>Ma.</b></i> <b>nelli</b>", DED 4250 "...<i>Trichosanthes anguina.
    # <b>Ma.</b></i> <b>puṭṭal, piṭṭal</b>". The mandatory lowercase sci-name text
    # before <b>Abbrev keeps it off ordinary "<i><b>Ma.</b></i>" markers; the
    # _is_known_lang_abbrev gate (strict-pattern loop) blocks the sci-name itself,
    # and the <b>-bounded headword is contamination-proof.
    re.compile(
        r"<i>[^<]*?[a-z][^<]*?<b>" + _LANG_ABBREV + r"</b></i>"
        + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
]

# Running-text variant of shape (1): NO <b> before the <i>, and the headword is
# PLAIN TEXT after the marker (e.g. DED 257 "<i>M. edule. Tu.</i> alimarů,
# alimārů"). This is the case shape (1)'s comment deferred, because when the form
# is elided the marker is followed directly by an English gloss (DED 814
# "<i>Calotropis gigantea. Ma.</i> gigantic swallow-wort...") with no <b> anchor
# to tell them apart -- so this pattern is gated by _rt_headword_ok, a lexical
# guard (not a structural one), and is iterated SEPARATELY from _STRICT_PATTERNS.
_RT_SCINAME_PATTERN = re.compile(
    _TOTYPE_NOT_BOLD + r"<i>[^<]*?[a-z][^<]*?\s" + _LANG_ABBREV
    + r"</i>\s+(?:\([^)]*\)\s*)*(" + _FORM_FIRST + r"[^<]*)(?=<)",
    re.DOTALL,
)

# Common English words that begin or pervade a Burrow gloss; a captured
# running-text headword starting with (or immediately continuing into) one of
# these is prose, not a Dravidian form, so _rt_headword_ok rejects it.
_ENGLISH_GLOSS_WORDS = frozenset(
    """a an the of or and with to as in on at for used kind large small big long
    wild sacred common red white black green sweet bitter tree plant bird fish
    gigantic swallow manure leaf leaves flower fruit root seed nut oil water milk
    grass shrub herb climber creeper species see same be become being sp var""".split()
)


# Plain-text (untagged) language markers in running text: the abbrev is NOT
# wrapped in <i> (so no _PATTERNS/_STRICT entry anchors on it) and sits right
# after a sentence boundary, with its headword either sharing the same <b> run
# (shape 1) or in its own fresh <b> immediately after (shape 2). This is the
# "glued into the previous language's gloss" miss documented for Tulu, Konda,
# Malayalam, Malto, Parji, and Kodagu -- e.g. "...male servant. Tu. <b>ūḷiga</b>"
# (DED 758), "...(Voc. 97). Konḍa <b>al- (aṭ-, aṇ-)</b>" (DED 260),
# "mango stone. <b>Tu. aṇḍi</b>" (DED 126).
#
# These have NO structural <i>-anchor, so over-firing on a mid-gloss
# cross-reference (e.g. DED 382's "see 4411 Ta. <b>peru</b>)") is the risk. Two
# guards keep them safe: (1) the MANDATORY sentence-boundary lookbehind
# `(?<=\.\s)` (a period + whitespace) -- a cross-ref like "4411 Ta." is preceded
# by a digit, not a period, so it never fires; (2) the _is_known_lang_abbrev
# gate applied in the loop (same gate the To.-type/RT patterns use), which
# rejects a capitalised English word ("Water", "Big") that isn't a real
# language. Iterated AFTER _PATTERNS/_STRICT/_RT so any position an anchored
# pattern already claimed wins.
# A cross-reference introducer sitting immediately before a plain-text language
# marker means the marker is a CROSS-REFERENCE target, not a new attestation:
# "...abuse. Cf. Ta. eḷku" (DED 776), "? Cf. Kor. (O.) elkiri" (DED 835),
# "(cf. Ta. ovvoṉṟu each one)" (DED 990). These introducers (Cf./cf./see/s.v./
# esp./viz./under/=) all themselves end in a period, so they satisfy the
# _PLAINTEXT_MARKER_PATTERNS' `(?<=\.\s)` sentence-boundary lookbehind and would
# be wrongly captured. (The number-bearing form "Cf. 856 Ta." is already
# excluded -- the digit between breaks the immediate period boundary -- so only
# the no-number "Cf. Ta." shape needs this guard.) Checked against the
# tag-stripped tail right before the marker.
_CROSSREF_TAIL_RE = re.compile(
    r"(?:\bcf|\bsee|\bs\.\s*v|\bviz|\besp|\bunder|\be\.\s*g|=)\s*\.?\s*$",
    re.IGNORECASE,
)


def _plaintext_marker_is_crossref(entry_html: str, marker_start: int) -> bool:
    """True when a plain-text marker at ``marker_start`` follows a cross-reference
    introducer (Cf./cf./see/s.v./esp./viz./under/=), so it is a cross-reference
    target rather than a new language attestation."""
    tail = _HTML_TAG_RE.sub("", entry_html[max(0, marker_start - 40):marker_start])
    return bool(_CROSSREF_TAIL_RE.search(tail))


def _plaintext_marker_in_parenthetical(entry_html: str, marker_start: int) -> bool:
    """True when a plain-text marker sits inside an unclosed "(" -- i.e. within a
    grammatical sub-form list "(obl. Old Ta. niṉ-, mod. Ta. uṉ-)" (DED 3684), a
    "(whence borrowed forms, e.g. Te. ...)" derivation note (DED 2448), or a
    "(= ...; cf. ... Pe. ...)" comparison (DED 2682). A genuine top-level
    attestation is never inside an open parenthetical, so a positive paren depth
    at the marker means this is an in-gloss sub-form or editorial note, not a new
    attestation. Also blocks a Gondi sub-dialect qualifier like "(pl. W. Mu. Ma.
    ...)" (DED 400) from being mis-parsed as a Malayalam attestation. Paren depth
    is computed over the tag-stripped text before the marker (headword
    parentheticals earlier in the entry are balanced, so they net to zero)."""
    before = _HTML_TAG_RE.sub("", entry_html[:marker_start])
    return before.count("(") > before.count(")")


_PLAINTEXT_MARKER_PATTERNS = [
    # Shape 1: <b>Abbrev. headword</b>  (marker + headword share one bold run)
    re.compile(
        r"(?<=\.\s)<b>" + _LANG_ABBREV + r"\s+("
        + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
    # Shape 2: Abbrev. <b>headword</b>  (untagged marker, headword in fresh <b>)
    re.compile(
        r"(?<=\.\s)" + _LANG_ABBREV + r"\s*" + _OPT_HEADWORD_QUALIFIER
        + r"<b>(" + _HEADWORD_SPAN_ACROSS_NESTED + r")</b>",
        re.DOTALL,
    ),
]


# Comma-for-period marker: the abbrev's terminal period is OCR'd as a comma
# sitting OUTSIDE the italic -- "<b><i>Ta</i>, par̤i (-pp-, -tt-)</b>" (DED 4002,
# the Tamil form par̤i). Pattern A's mandatory "</i>\s+" (its guard against
# capturing "<i>SciName</i>; word" scientific names as bogus languages) cannot
# admit the comma without reopening that hole, so this is a SEPARATE pattern:
# the abbrev capture has no trailing period (the comma replaced it), and the
# _find_all_lang_spans loop reconstructs it and gates on the KNOWN-language check
# -- a scientific name ("Artocarpus.") is not a known language and is rejected,
# closing the false-positive risk the comma would otherwise open.
_COMMA_MARKER_PATTERN = re.compile(
    r"<b>\(?<i>" + _OPT_LEADING_QUALIFIER + _OPT_SUBENTRY + r"(" + _LANG_CHAR + r")"
    + r"</i>,\s+" + _OPT_HEADWORD_QUALIFIER + r"([^<]+)(?=<)",
    re.DOTALL,
)


def _rt_headword_ok(text: str) -> Optional[str]:
    """Lexical guard for _RT_SCINAME_PATTERN (which has no <b> anchor).

    Accept a plain-text run as a headword only when it reads as a Dravidian form,
    not an English gloss (the elided-form case). A form carries a diacritic /
    length-dot, or is a token that is not a common English gloss word; prose
    glosses begin with -- or run straight into -- an English word.
    """
    t = text.strip()
    if not t:
        return None
    first = re.split(r"[\s,;]", t, 1)[0].strip(".,;()")
    if not first:
        return None
    if any(ord(c) > 127 or c == "·" or c == "·" for c in first):
        return t  # diacritic / length-dot -> unambiguously a transliterated form
    if first.lower() in _ENGLISH_GLOSS_WORDS:
        return None
    # Plain-ASCII first token that is not itself a gloss word: still reject if the
    # run immediately continues as English prose (a following gloss word).
    for tk in t.split()[1:3]:
        if tk.strip(".,;()").lower() in _ENGLISH_GLOSS_WORDS:
            return None
    return t


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


def _is_known_qualified_abbrev(abbrev: str) -> bool:
    """True if the inventory knows this exact (possibly qualified) abbreviation."""
    try:
        from dialect_mapping import normalize_burrow

        return normalize_burrow(abbrev) != abbrev
    except ImportError:
        return False


def _clean_lang_abbrev(raw: str) -> str:
    """Normalize whitespace and strip a leading sub-entry marker (e.g. "(a) ").

    A parenthetical captured inside the <i> marker is kept only when the
    inventory recognises the full dialect-qualified form ("Nk. (Ch.)" = Naiki);
    otherwise it is a bibliographic/source tag ("Te. (SAN)", "Ka. (DCV)") and is
    stripped back to the base abbreviation.
    """
    abbrev = _SUBENTRY_MARKER_RE.sub("", raw.strip()).strip()
    if "(" in abbrev and not _is_known_qualified_abbrev(abbrev):
        abbrev = re.sub(r"\s*\([^)]*\)\s*$", "", abbrev).strip()
    return abbrev


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


def _is_known_lang_abbrev(abbrev: str) -> bool:
    """True only if the abbreviation resolves to a KNOWN language/dialect name.

    Stricter than _is_valid_lang (which merely block-lists known non-languages
    and accepts any other capitalised 2-10 char token). Used to gate the
    To.-type pattern (_STRICT_PATTERNS): because that pattern scans INTO an <i>
    span after arbitrary lowercase text, it lacks the "<i>Abbrev" structural
    anchor every other pattern relies on to stay on a real language marker, so
    italic citation titles/botanical authorities (Volume, Sanskrit, Linn.)
    would otherwise be captured as bogus languages.
    """
    try:
        from dialect_mapping import normalize_burrow
    except ImportError:
        return True  # fail open, matching _normalize_language's tolerance
    cl = abbrev.strip()
    if not cl:
        return False
    return normalize_burrow(cl) != cl or normalize_burrow(cl.rstrip(".")) != cl.rstrip(".")



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
        """Clean DED number: strip leading zeros. '045' -> '45', '0047' -> '47'.

        Scrape/corpus-layer semantics: split-entry suffixes are deliberately
        KEPT ("4896(a)" stays "4896(a)") so the (a)/(b) halves of a split DED
        entry remain distinct corpus entries and inspect_ded_entry can index
        them separately. The validation/ledger layer folds those suffixes --
        use ``textnorm.clean_ded_number`` there instead.
        """
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
                # A dialect qualifier can sit BETWEEN the language marker and the
                # headword, outside the <i> marker --
                # "<i><b>Nk.</b></i> (Ch.) <b>aṛka</b>" (DED 75/430) or
                # "<i>Nk.</i> (Ch.) <b>khīr</b>" (DED 1623). There
                # _OPT_HEADWORD_QUALIFIER consumes and discards it, so "Nk. (Ch.)"
                # (= Naiki) would be stored as bare "Nk." (= Naikri, the WRONG
                # dialect -- the form itself parses correctly). Fold the qualifier
                # back into the abbrev, but ONLY when the inventory recognises the
                # composite (normalize_burrow changes it) -- so bibliographic /
                # source tags in the same position ("Go. (Tr.)", "Ga. (S.)",
                # "(LSI 4.572)") are left untouched, none resolving to a composite.
                between = entry_html[m.end(1):m.start(2)]
                qual_m = re.search(r"\(([^)<]*)\)", between)
                if qual_m:
                    qualified = f"{lang_abbrev} ({qual_m.group(1).strip()})"
                    if _is_known_qualified_abbrev(qualified):
                        lang_abbrev = qualified
                headword_text = _HTML_TAG_RE.sub("", m.group(2)).strip()

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

        # Strict patterns (To.-type) scan into an <i> span, so they additionally
        # require a KNOWN language abbreviation -- not merely a valid-looking one.
        # Run after _PATTERNS so a position already claimed by an anchored
        # pattern wins (these only fire on genuinely-embedded markers no other
        # pattern reaches).
        for pattern in _STRICT_PATTERNS:
            for m in pattern.finditer(entry_html):
                lang_abbrev = _clean_lang_abbrev(m.group(1))
                if not _is_valid_lang(lang_abbrev) or not _is_known_lang_abbrev(lang_abbrev):
                    continue
                headword_text = _HTML_TAG_RE.sub("", m.group(2)).strip()
                if m.start() not in seen_starts:
                    seen_starts[m.start()] = _LangSpan(
                        lang_abbrev=lang_abbrev,
                        headword_text=headword_text,
                        start=m.start(),
                        end=m.end(),
                    )

        # Running-text sci-name pattern: no <b> anchor, so the plain-text headword
        # is passed through the _rt_headword_ok lexical guard (rejects English
        # gloss of the elided-form case). Run last, position-deduped like the rest.
        for m in _RT_SCINAME_PATTERN.finditer(entry_html):
            if m.start() in seen_starts:
                continue
            lang_abbrev = _clean_lang_abbrev(m.group(1))
            if not _is_valid_lang(lang_abbrev) or not _is_known_lang_abbrev(lang_abbrev):
                continue
            headword_text = _rt_headword_ok(_HTML_TAG_RE.sub("", m.group(2)))
            if headword_text is None:
                continue
            seen_starts[m.start()] = _LangSpan(
                lang_abbrev=lang_abbrev,
                headword_text=headword_text,
                start=m.start(),
                end=m.end(),
            )

        # Plain-text (untagged) running-text markers -- no <i> anchor, so gated
        # by _is_known_lang_abbrev AND the pattern's own mandatory sentence
        # boundary. Run last so any anchored-pattern position wins first.
        for pattern in _PLAINTEXT_MARKER_PATTERNS:
            for m in pattern.finditer(entry_html):
                if m.start() in seen_starts:
                    continue
                if _plaintext_marker_is_crossref(entry_html, m.start()):
                    continue
                if _plaintext_marker_in_parenthetical(entry_html, m.start()):
                    continue
                lang_abbrev = _clean_lang_abbrev(m.group(1))
                if not _is_valid_lang(lang_abbrev) or not _is_known_lang_abbrev(lang_abbrev):
                    continue
                headword_text = _HTML_TAG_RE.sub("", m.group(2)).strip()
                seen_starts[m.start()] = _LangSpan(
                    lang_abbrev=lang_abbrev,
                    headword_text=headword_text,
                    start=m.start(),
                    end=m.end(),
                )

        # Comma-for-period marker (<i>Ta</i>, headword): reconstruct the abbrev's
        # elided period and admit ONLY a known language, so a scientific name in
        # the same shape can't be captured. Run last, position-deduped.
        for m in _COMMA_MARKER_PATTERN.finditer(entry_html):
            if m.start() in seen_starts:
                continue
            abbrev = _clean_lang_abbrev(m.group(1))
            abbrev_dot = abbrev if abbrev.endswith(".") else abbrev + "."
            if not _is_known_lang_abbrev(abbrev_dot):
                continue
            headword_text = _HTML_TAG_RE.sub("", m.group(2)).strip()
            seen_starts[m.start()] = _LangSpan(
                lang_abbrev=abbrev_dot,
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

            # Headwords are regex-sliced from str(blockquote), which preserves
            # HTML entities (unlike the gloss's get_text path) -- so a source
            # "(&lt; ...)" derivation note reaches here still escaped. Decode it
            # so the stored headword reads "(< ...)" as published, not "(&lt; ...)".
            headwords = [
                html.unescape(hw.strip().rstrip("( ").strip())
                for hw in _split_headword_chain(span.headword_text)
            ]
            headwords = [
                hw
                for hw in headwords
                if hw
                and not hw.startswith("(")
                and (len(hw) > 1 or hw in _VOWEL_HEADWORDS)
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
                    # Store the full gloss as bounded by _extract_gloss (which
                    # already stops at the next language marker / DED(S) ref).
                    # A former [:200] cap silently dropped inline sub-dialect
                    # forms sitting past 200 chars -- e.g. DED 513's
                    # "(ASu.) ḍiyyōr", which the matcher's gloss extractor then
                    # could not reach. Uncapped glosses already ran to ~2700
                    # chars, so this only lets the ~112 capped ones reach their
                    # natural bounded length.
                    gloss=gloss_text,
                    source_text=f"{span.lang_abbrev} {', '.join(headwords)}",
                )
            )

        # Resolve lexicographic "id." glosses (idem = same as previous attestation).
        last_real_gloss = ""
        for att in attestations:
            g = att.gloss.strip()
            m_qualified_id = _QUALIFIED_ID_RE.match(g)
            # An "id." whose antecedent lists several forms is ambiguous -- the
            # reference points at one form's meaning, not the whole section, and
            # no string rule resolves which one -- so leave the literal "id."
            # instead of gluing in the wrong meaning (kept in sync with
            # repair_burrow_corpus_glosses.py; see antecedent_is_multiform).
            resolvable = bool(last_real_gloss) and not antecedent_is_multiform(last_real_gloss)
            if g.lower() == "id.":
                if resolvable:
                    att.gloss = last_real_gloss
            elif m_qualified_id:
                # e.g. "(pl.) id." -> "(pl.) <prev gloss>". Strip any
                # leading qualifier already on last_real_gloss first (it may
                # naturally have one of its own, e.g. Konḍa's own gloss is
                # "(pl. veRku) firewood, fuel.") so chained qualifiers don't
                # accumulate across multiple attestations.
                qualifier = m_qualified_id.group(1).strip()
                suffix = m_qualified_id.group(2).strip().lstrip(";").strip()
                if resolvable:
                    bare_gloss = _LEADING_PAREN_RE.sub("", last_real_gloss).strip()
                    combined = f"({qualifier}) {bare_gloss}" if qualifier else bare_gloss
                    att.gloss = f"{combined}; {suffix}" if suffix else combined
            elif g.lower().startswith("id."):
                # e.g. "id.; extra note" → "<prev gloss>; extra note"
                if resolvable:
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

        starling_hw_norm = normalize_for_match(starling_headword)

        for att in attestations:
            result = match_languages(att.language_abbrev, starling_language)
            if not result.matched:
                continue

            for burrow_hw in att.headwords:
                burrow_hw_norm = normalize_for_match(burrow_hw)
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
