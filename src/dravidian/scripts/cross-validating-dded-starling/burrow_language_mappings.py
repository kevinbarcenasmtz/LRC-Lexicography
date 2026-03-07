"""
Backward-compatible shim for burrow_language_mappings.

All language mapping logic now lives in dialect_mapping.py. This module
provides the old function signatures so existing code (burrow_corpus_scraper,
ded_paragraph_resolver, etc.) continues to work without modification.

For new code, import directly from dialect_mapping instead.
"""

from __future__ import annotations

from typing import Optional

from dialect_mapping import (
    LANGUAGE_INVENTORY,
    match_languages,
    normalize_burrow,
    starling_to_burrow,
    get_language_info,
)


def normalize_language(burrow_abbrev: str) -> str:
    """Convert Burrow abbreviation to full language name."""
    return normalize_burrow(burrow_abbrev)


def match_language_variant(burrow_lang: str, starling_lang: str) -> bool:
    """Check if a Burrow abbreviation and Starling language name refer to the same language."""
    result = match_languages(burrow_lang, starling_lang, strict=False)
    return result.matched


# Flat lookup tables (for any code that accessed these directly)
BURROW_TO_STARLING = {
    info.burrow_abbrev: info.starling_base for info in LANGUAGE_INVENTORY.values()
}

STARLING_VARIANTS = {}
for info in LANGUAGE_INVENTORY.values():
    for dialect in info.dialects:
        STARLING_VARIANTS[dialect] = info.starling_base
