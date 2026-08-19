"""
Comprehensive Starling-to-Burrow dialect mapping module.

Provides language family groupings, dialect resolution, and bidirectional
mapping between StarlingDB and Burrow & Emeneau's DEDR notation.

Language hierarchy:
  - South Dravidian I: Tamil, Malayalam, Kannada, Kodagu, Tulu
  - South Dravidian II: Telugu
  - South-Central: Gondi varieties, Konda, Kui-Kuwi, Kolami-Gadba
  - Central: Kolami, Naiki, Parji, Gadba
  - North Dravidian: Kurukh, Malto, Brahui
  - Nilgiri: Kota, Toda, Irula, Kurumba varieties

Burrow consolidates some groups (e.g., all Gondi → "Go.") while Starling
preserves dialectal distinctions.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum


class LanguageBranch(Enum):
    """Major Dravidian language branches."""

    SOUTH_DRAVIDIAN_I = "South Dravidian I"
    SOUTH_DRAVIDIAN_II = "South Dravidian II"
    SOUTH_CENTRAL = "South-Central Dravidian"
    CENTRAL = "Central Dravidian"
    NORTH_DRAVIDIAN = "North Dravidian"
    NILGIRI = "Nilgiri"
    PROTO = "Proto-Dravidian"


@dataclass
class LanguageInfo:
    """Metadata for a Dravidian language."""

    burrow_abbrev: str
    starling_base: str
    branch: LanguageBranch
    dialects: List[str] = field(default_factory=list)
    notes: str = ""

    @property
    def all_forms(self) -> List[str]:
        """All valid forms: base name + dialects."""
        return [self.starling_base] + self.dialects


LANGUAGE_INVENTORY: Dict[str, LanguageInfo] = {
    # South Dravidian I
    "Ta.": LanguageInfo(
        burrow_abbrev="Ta.",
        starling_base="Tamil",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    "Ma.": LanguageInfo(
        burrow_abbrev="Ma.",
        starling_base="Malayalam",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    "Ka.": LanguageInfo(
        burrow_abbrev="Ka.",
        starling_base="Kannada",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    "Koḍ.": LanguageInfo(
        burrow_abbrev="Koḍ.",
        starling_base="Kodagu",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    "Tu.": LanguageInfo(
        burrow_abbrev="Tu.",
        starling_base="Tulu",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    # South Dravidian II
    "Te.": LanguageInfo(
        burrow_abbrev="Te.",
        starling_base="Telugu",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_II,
        dialects=[
            "Telugu (Krishnamurti)",
            "Inscriptional Telugu",
            "Merolu Telugu",
            "Proto-Telugu",
        ],
    ),
    # Gondi (consolidated in Burrow, dialectal in Starling)
    "Go.": LanguageInfo(
        burrow_abbrev="Go.",
        starling_base="Gondi",
        branch=LanguageBranch.SOUTH_CENTRAL,
        dialects=[
            "Koya Gondi",
            "Muria Gondi",
            "Maria Gondi",
            "Betul Gondi",
            "Adilabad Gondi",
            "Mandla Gondi (Phailbus)",
            "Maria Gondi (Mitchell)",
            "Mandla Gondi (Williamson)",
            "Seoni Gondi",
            "Gommu Gondi",
            "Yeotmal Gondi",
            "Chindwara Gondi",
            "Durg Gondi",
            "Chanda Gondi",
            "Mandla Gondi",
            "Maria Gondi (Lind)",
            "Maria Gondi (Smith)",
        ],
        notes='Burrow uses single abbrev "Go." for all Gondi dialects',
    ),
    # Konda
    "Konḍa": LanguageInfo(
        burrow_abbrev="Konḍa",
        starling_base="Konda",
        branch=LanguageBranch.SOUTH_CENTRAL,
        dialects=["Konda (Burrow/Bhattacharya)"],
    ),
    # Kui-Kuwi
    "Kui": LanguageInfo(
        burrow_abbrev="Kui",
        starling_base="Kui",
        branch=LanguageBranch.SOUTH_CENTRAL,
        dialects=["Khuttia Kui"],
    ),
    "Kuwi": LanguageInfo(
        burrow_abbrev="Kuwi",
        starling_base="Kuwi (Schulze)",
        branch=LanguageBranch.SOUTH_CENTRAL,
        dialects=[
            "Kuwi (Fitzgerald)",
            "Kuwi (Israel)",
            "Sunkarametta Kuwi",
            "Kuwi (Mahanti)",
            "Tekriya Kuwi",
            "Dongriya Kuwi",
            "Parja Kuwi",
        ],
        notes='Starling base is "Kuwi (Schulze)"',
    ),
    # Kolami-Gadba-Naiki cluster
    "Kol.": LanguageInfo(
        burrow_abbrev="Kol.",
        starling_base="Kolami",
        branch=LanguageBranch.CENTRAL,
        dialects=[
            "Kinwat Kolami",
            "Kolami (Setumadhava Rao)",
        ],
    ),
    "Ga.": LanguageInfo(
        burrow_abbrev="Ga.",
        starling_base="Gadba",
        branch=LanguageBranch.CENTRAL,
        dialects=[
            "Salur Gadba",
            "Ollari Gadba",
            "Kondekor Gadba",
            "Poya Gadba",
        ],
    ),
    "Nk.": LanguageInfo(
        burrow_abbrev="Nk.",
        starling_base="Naikri",
        branch=LanguageBranch.CENTRAL,
        dialects=[],
    ),
    "Nk. (Ch.)": LanguageInfo(
        burrow_abbrev="Nk. (Ch.)",
        starling_base="Naiki",
        branch=LanguageBranch.CENTRAL,
        dialects=[],
    ),
    # Parji
    "Pa.": LanguageInfo(
        burrow_abbrev="Pa.",
        starling_base="Parji",
        branch=LanguageBranch.CENTRAL,
        dialects=[],
    ),
    # Pengo
    "Pe.": LanguageInfo(
        burrow_abbrev="Pe.",
        starling_base="Pengo",
        branch=LanguageBranch.SOUTH_CENTRAL,
        dialects=[],
    ),
    # Manda
    "Manḍ.": LanguageInfo(
        burrow_abbrev="Manḍ.",
        starling_base="Manda",
        branch=LanguageBranch.CENTRAL,
        dialects=[],
    ),
    # North Dravidian
    "Kur.": LanguageInfo(
        burrow_abbrev="Kur.",
        starling_base="Kurukh",
        branch=LanguageBranch.NORTH_DRAVIDIAN,
        dialects=[],
    ),
    "Malt.": LanguageInfo(
        burrow_abbrev="Malt.",
        starling_base="Malto",
        branch=LanguageBranch.NORTH_DRAVIDIAN,
        dialects=[],
    ),
    "Br.": LanguageInfo(
        burrow_abbrev="Br.",
        starling_base="Brahui",
        branch=LanguageBranch.NORTH_DRAVIDIAN,
        dialects=[],
    ),
    # Nilgiri
    "Ko.": LanguageInfo(
        burrow_abbrev="Ko.",
        starling_base="Kota",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    "To.": LanguageInfo(
        burrow_abbrev="To.",
        starling_base="Toda",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    "Ir.": LanguageInfo(
        burrow_abbrev="Ir.",
        # Starling spells this "Irula" (plain l) throughout; DEDR's retroflex
        # "Iruḷa" is kept as the corpus display but must not be the match target.
        starling_base="Irula",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    "ĀlKu.": LanguageInfo(
        burrow_abbrev="ĀlKu.",
        starling_base="Ālu Kuṟumba",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    "PālKu.": LanguageInfo(
        burrow_abbrev="PālKu.",
        starling_base="Pālu Kuṟumba",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    "Kurub.": LanguageInfo(
        burrow_abbrev="Kurub.",
        starling_base="Beṭṭa Kuruba",
        branch=LanguageBranch.NILGIRI,
        dialects=[],
    ),
    # Miscellaneous
    "Kor.": LanguageInfo(
        burrow_abbrev="Kor.",
        starling_base="Koraga",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_I,
        dialects=[],
    ),
    "Bel.": LanguageInfo(
        burrow_abbrev="Bel.",
        starling_base="Belari",
        branch=LanguageBranch.SOUTH_DRAVIDIAN_II,
        dialects=[],
    ),
    # Proto-Dravidian
    "PDr.": LanguageInfo(
        burrow_abbrev="PDr.",
        starling_base="Proto-Dravidian",
        branch=LanguageBranch.PROTO,
        dialects=[],
    ),
    "Dr.": LanguageInfo(
        burrow_abbrev="Dr.",
        starling_base="Proto-Dravidian",
        branch=LanguageBranch.PROTO,
        dialects=[],
    ),
}

# A few Burrow abbreviations appear in the source with inconsistent
# orthography -- a plain consonant where the canonical form is retroflex, or a
# dropped trailing period. Map the stray spellings onto their canonical
# inventory key so match_languages resolves them like the canonical form.
_ABBREV_ALIASES: Dict[str, str] = {
    "Mand.": "Manḍ.",  # DED 34: plain-d spelling of Manda (canonical Manḍ.)
    "Koḍ": "Koḍ.",     # DED 215/2826/5297: Kodagu missing its trailing period
    "Kui.": "Kui",     # DED 837: Kui with a stray trailing period (canonical is bare)
    "Kod.": "Koḍ.",    # DED 4547: plain-d "Kod." (no dot-below) spelling of Kodagu
    "Konḏa.": "Konḍa", # DED 3684: macron-below-d + period variant of Konḍa
    "Ko..": "Ko.",     # DED 3700: Kota with a doubled trailing period
}

# Reverse indexes (built from LANGUAGE_INVENTORY)
_STARLING_TO_BURROW: Dict[str, str] = {}
_DIALECT_TO_BASE: Dict[str, str] = {}
_BURROW_TO_INFO: Dict[str, LanguageInfo] = {}


def _build_indexes() -> None:
    """Build reverse lookup indexes from LANGUAGE_INVENTORY."""
    global _STARLING_TO_BURROW, _DIALECT_TO_BASE, _BURROW_TO_INFO

    for abbrev, info in LANGUAGE_INVENTORY.items():
        _BURROW_TO_INFO[abbrev] = info

        if info.starling_base not in _STARLING_TO_BURROW:
            _STARLING_TO_BURROW[info.starling_base] = abbrev

        for dialect in info.dialects:
            if dialect not in _DIALECT_TO_BASE:
                _DIALECT_TO_BASE[dialect] = info.starling_base

    # Register stray-orthography abbreviation variants against the canonical
    # LanguageInfo (does not touch _STARLING_TO_BURROW, so the canonical
    # abbreviation stays the reverse-lookup target).
    for variant, canonical in _ABBREV_ALIASES.items():
        info = _BURROW_TO_INFO.get(canonical)
        if info and variant not in _BURROW_TO_INFO:
            _BURROW_TO_INFO[variant] = info


_build_indexes()


@dataclass
class MatchResult:
    """Result of a language match operation."""

    matched: bool
    burrow_abbrev: Optional[str] = None
    starling_canonical: Optional[str] = None
    match_type: str = ""  # 'exact', 'dialect', 'base', 'fuzzy', 'none'
    confidence: float = 0.0
    notes: str = ""


def normalize_burrow(burrow_lang: str) -> str:
    """
    Convert Burrow abbreviation to canonical Starling base language.

    Examples:
        'Go.' → 'Gondi'
        'Ta.' → 'Tamil'
        'Kuwi' → 'Kuwi (Schulze)'
    """
    info = _BURROW_TO_INFO.get(burrow_lang)
    return info.starling_base if info else burrow_lang


def burrow_to_starling(burrow_abbrev: str) -> Optional[str]:
    """Get Starling base language name from Burrow abbreviation."""
    info = _BURROW_TO_INFO.get(burrow_abbrev)
    return info.starling_base if info else None


def starling_to_burrow(starling_lang: str) -> Optional[str]:
    """
    Get Burrow abbreviation from Starling language name.
    Handles both base languages and dialects.
    """
    if starling_lang in _STARLING_TO_BURROW:
        return _STARLING_TO_BURROW[starling_lang]

    base = _DIALECT_TO_BASE.get(starling_lang)
    if base and base in _STARLING_TO_BURROW:
        return _STARLING_TO_BURROW[base]

    return None


def get_language_info(identifier: str) -> Optional[LanguageInfo]:
    """
    Get LanguageInfo from either Burrow abbreviation or Starling name.
    """
    if identifier in _BURROW_TO_INFO:
        return _BURROW_TO_INFO[identifier]

    abbrev = starling_to_burrow(identifier)
    if abbrev:
        return _BURROW_TO_INFO.get(abbrev)

    return None


def match_languages(
    burrow_lang: str, starling_lang: str, strict: bool = False
) -> MatchResult:
    """
    Match Burrow and Starling language names with detailed diagnostics.

    Args:
        burrow_lang: Burrow abbreviation (e.g., 'Go.', 'Ta.')
        starling_lang: Starling language name (e.g., 'Maria Gondi', 'Tamil')
        strict: If True, require exact match; if False, allow fuzzy matching

    Returns:
        MatchResult with match details and confidence score
    """
    burrow_info = _BURROW_TO_INFO.get(burrow_lang)
    if not burrow_info:
        return MatchResult(
            matched=False,
            match_type="none",
            confidence=0.0,
            notes=f"Unknown Burrow abbreviation: {burrow_lang}",
        )

    # Exact base match
    if starling_lang == burrow_info.starling_base:
        return MatchResult(
            matched=True,
            burrow_abbrev=burrow_lang,
            starling_canonical=burrow_info.starling_base,
            match_type="exact",
            confidence=1.0,
        )

    # Dialect match
    if starling_lang in burrow_info.dialects:
        return MatchResult(
            matched=True,
            burrow_abbrev=burrow_lang,
            starling_canonical=burrow_info.starling_base,
            match_type="dialect",
            confidence=0.95,
            notes=f"{starling_lang} is a dialect of {burrow_info.starling_base}",
        )

    # Check if starling_lang is a base that maps to a dialect
    if starling_lang in _DIALECT_TO_BASE:
        base = _DIALECT_TO_BASE[starling_lang]
        if base == burrow_info.starling_base:
            return MatchResult(
                matched=True,
                burrow_abbrev=burrow_lang,
                starling_canonical=base,
                match_type="base",
                confidence=0.9,
                notes=f"{starling_lang} resolves to base {base}",
            )

    if strict:
        return MatchResult(
            matched=False,
            burrow_abbrev=burrow_lang,
            starling_canonical=burrow_info.starling_base,
            match_type="none",
            confidence=0.0,
            notes=f"No match (strict mode): {burrow_lang} vs {starling_lang}",
        )

    # Fuzzy matching: substring overlap
    burrow_canonical = burrow_info.starling_base.lower()
    starling_lower = starling_lang.lower()

    # Check for meaningful substring match
    if burrow_canonical in starling_lower or starling_lower in burrow_canonical:
        min_len = min(len(burrow_canonical), len(starling_lower))
        if min_len >= 4:
            return MatchResult(
                matched=True,
                burrow_abbrev=burrow_lang,
                starling_canonical=burrow_info.starling_base,
                match_type="fuzzy",
                confidence=0.7,
                notes=f'Fuzzy match: "{burrow_canonical}" ~= "{starling_lower}"',
            )

    return MatchResult(
        matched=False,
        burrow_abbrev=burrow_lang,
        starling_canonical=burrow_info.starling_base,
        match_type="none",
        confidence=0.0,
        notes=f"No match: {burrow_info.starling_base} != {starling_lang}",
    )


def get_dialects(base_language: str) -> List[str]:
    """Get all known dialects for a base language."""
    info = get_language_info(base_language)
    return info.dialects if info else []


# ---------------------------------------------------------------------------
# Gondi inline abbreviations
# ---------------------------------------------------------------------------
# Burrow consolidates all Gondi dialects under a single "Go." attestation and
# encodes dialect/source distinctions as parenthetical inline markers within
# the gloss, e.g.:
#   Go. accānā (Tr.) to be cut; (Mu.) acc- to split; (Tr. W.) askānā ...
#
# Key:   Burrow inline abbreviation (as it appears in the gloss)
# Value: Starling dialect name(s) that correspond to that source/dialect
#
# Sources confirmed from DEDR frontmatter §31 and Voc. sigilla
# (Burrow & Bhattacharya, 'A comparative vocabulary of the Gondi dialects',
# JAS 2.73-251, 1960 — cited as Voc. / CVOTGD in Starling):
#
#   Tr.  = C. G. Chenevix Trench — Betul district data (1919–21)
#   W.   = H. D. Williamson — Mandla dialect (1890)
#   Ph.  = Phailbus — Mandla dialect (1963)  [NOT listed in §31; uncertain]
#   Mu.  = Muria Gondi (dialect)
#   Ma.  = Maria Gondi (Hill-Maria) — plain Maria only, per §55
#   M.   = A. N. Mitchell — Maria Gondi, Bison Horn/Dandami Marias of Bastar (1942)
#   L.   = Abraham A. Lind — Maria dialect (1913)
#   G.   = Stephen A. Tyler — Gommu dialect of Koya (1969)
#   Ko.  = Koya Gondi (DGG, Subrahmanyam 1968); distinct from top-level Ko. = Kota
#   A.   = Adilabad fieldnotes (Burrow & Bhattacharya 1951) / Adilabad dialect
#   Ch.  = Chindwara Gondi (dialect)
#   S.   = Seoni Gondi dialect. Evidence: DED 133 Go. gloss "... (Mu. Ko. S.)
#          adm-, (M.) ādmānā id." <-> Starling "Seoni Gondi" adm-; DED 718
#          Go. gloss "...(S.) urum- to lighten" <-> Starling "Seoni Gondi"
#          urum-. No conflict with Kuwi's own "S." = Schulze -- the inline
#          abbreviation tables are keyed per top-level Starling language, so
#          Gondi's "S." and Kuwi's "S." never collide.
GONDI_INLINE_ABBREVS: Dict[str, List[str]] = {
    "Tr.": ["Betul Gondi"],
    "W.": ["Mandla Gondi (Williamson)"],
    "Ph.": ["Mandla Gondi (Phailbus)"],  # not in §31; retained as uncertain
    "Mu.": ["Muria Gondi"],
    "Ma.": ["Maria Gondi"],
    "M.": ["Maria Gondi (Mitchell)"],
    "L.": ["Maria Gondi (Lind)"],
    "G.": ["Gommu Gondi"],
    "Ko.": ["Koya Gondi"],
    # Burrow also tags Koya forms with a spelled-out "Koya Su."/"Koya T." group
    # (Subrahmanyam / Tyler sub-sources); the gloss extractor tokenises those to
    # {"Koya.", "Su."}/{"Koya.", "T."}, so "Koya." routes them to Koya Gondi too.
    # (The leading-"Koya" marker shape is admitted by _DIALECT_MARKER_GROUP_RE.)
    "Koya.": ["Koya Gondi"],
    "A.": ["Adilabad Gondi"],
    "Ch.": ["Chindwara Gondi"],
    "S.": ["Seoni Gondi"],
}

# Kuwi inline source/dialect citations (parallel structure to Gondi above).
# Burrow consolidates all Kuwi citations under "Kuwi" and tags per-source
# forms inline, e.g.:
#   Kuwi (F.) māmbū , (S. Su. P. Isr.) māmbu we (excl.); (F.) mārrō ,
#   (S. Isr.) māro we (incl.) ... (S.) māpo on our side.
#
# Sigil identities are inferred from the DED 5154 paragraph itself plus the
# Starling dialect names and the abbreviation code table in
# lang_abbreviations.md (KWF=Fitzgerald, KWS=Schulze, KWI=Israel,
# SKW=Sunkarametta) — NOT independently confirmed against a DEDR frontmatter
# sigilla section the way Gondi's §31 citations were.
#   F.   = Fitzgerald
#   S.   = Schulze (Starling base "Kuwi (Schulze)")
#   Su.  = Subrahmanyam — Sunkarametta Kuwi
#   P.   = Parja Kuwi
#   Isr. = Israel
KUWI_INLINE_ABBREVS: Dict[str, List[str]] = {
    "F.": ["Kuwi (Fitzgerald)"],
    "S.": ["Kuwi (Schulze)"],
    "Su.": ["Sunkarametta Kuwi"],
    "P.": ["Parja Kuwi"],
    "Isr.": ["Kuwi (Israel)"],
}

# Kui inline dialect citations.
#   K. = Khuttia Kui
KUI_INLINE_ABBREVS: Dict[str, List[str]] = {
    "K.": ["Khuttia Kui"],
}

_INLINE_ABBREV_SOURCES = [GONDI_INLINE_ABBREVS, KUWI_INLINE_ABBREVS, KUI_INLINE_ABBREVS]

# Reverse index: Starling dialect name → inline abbreviation(s)
_DIALECT_TO_INLINE_ABBREVS: Dict[str, List[str]] = {}


def _build_inline_abbrev_index() -> None:
    global _DIALECT_TO_INLINE_ABBREVS
    for source in _INLINE_ABBREV_SOURCES:
        for abbrev, dialects in source.items():
            for dialect in dialects:
                _DIALECT_TO_INLINE_ABBREVS.setdefault(dialect, []).append(abbrev)


_build_inline_abbrev_index()


def get_inline_abbrevs_for_starling_dialect(starling_dialect: str) -> List[str]:
    """
    Return the Burrow inline abbreviations for a Starling dialect whose base
    language consolidates dialect citations inline (Gondi, Kuwi, Kui).

    e.g. 'Betul Gondi'              → ['Tr.']
         'Mandla Gondi (Williamson)' → ['W.']
         'Maria Gondi'               → ['Ma.']
         'Kuwi (Fitzgerald)'         → ['F.']
         'Khuttia Kui'               → ['K.']
    Returns an empty list for unmapped dialects.
    """
    return _DIALECT_TO_INLINE_ABBREVS.get(starling_dialect, [])


def get_branch(language: str) -> Optional[LanguageBranch]:
    """Get the language branch for a given language."""
    info = get_language_info(language)
    return info.branch if info else None


def list_languages_by_branch(branch: LanguageBranch) -> List[str]:
    """List all languages in a given branch."""
    return [
        info.starling_base
        for info in LANGUAGE_INVENTORY.values()
        if info.branch == branch
    ]


def diagnostic_report() -> str:
    """Generate a diagnostic report of the mapping coverage."""
    lines = ["Dravidian Language Mapping Diagnostic Report", "=" * 70, ""]

    for branch in LanguageBranch:
        langs = list_languages_by_branch(branch)
        if not langs:
            continue

        lines.append(f"\n{branch.value} ({len(langs)} languages):")
        lines.append("-" * 50)

        for lang in sorted(langs):
            info = get_language_info(lang)
            if not info:
                continue

            dialects_str = f" [{len(info.dialects)} dialects]" if info.dialects else ""
            lines.append(f"  {info.burrow_abbrev:<10} {lang:<30}{dialects_str}")

            if info.dialects and len(info.dialects) <= 5:
                for d in info.dialects:
                    lines.append(f"    - {d}")
            elif info.dialects:
                for d in info.dialects[:3]:
                    lines.append(f"    - {d}")
                lines.append(f"    ... and {len(info.dialects) - 3} more")

            if info.notes:
                lines.append(f"    Note: {info.notes}")

    lines.append(f"\n{'=' * 70}")
    lines.append(f"Total languages: {len(_STARLING_TO_BURROW)}")
    lines.append(f"Total dialects tracked: {len(_DIALECT_TO_BASE)}")

    return "\n".join(lines)


if __name__ == "__main__":
    print(diagnostic_report())

    print("\n\nTest Cases:")
    print("=" * 70)

    test_cases = [
        ("Go.", "Maria Gondi"),
        ("Go.", "Gondi"),
        ("Kuwi", "Kuwi (Schulze)"),
        ("Ta.", "Tamil"),
        ("Te.", "Telugu (Krishnamurti)"),
        ("Ka.", "Telugu"),
        ("Ko.", "Kota"),
        ("Dr.", "Proto-Dravidian"),
    ]

    for burrow, starling in test_cases:
        result = match_languages(burrow, starling)
        status = "✓" if result.matched else "✗"
        print(
            f"{status} {burrow:<10} vs {starling:<30} → {result.match_type} (conf: {result.confidence:.2f})"
        )
        if result.notes:
            print(f"  {result.notes}")
