"""
Shared DED edition classification (DEDR main dictionary vs Appendix).

Previously duplicated in burrow_corpus_scraper.py and
patch_corpus_editions.py; both must agree on the page boundary and the
text heuristics, so the single source of truth lives here.
"""

from __future__ import annotations

import re
from typing import Optional

# Page boundary between DEDR main dictionary and Appendix.
# Derived from the CONTENTS in the frontmatter: "Appendix (1-61) 509"
APPENDIX_START_PAGE = 509


def classify_edition(page: int) -> str:
    """Return 'DEDR' or 'Appendix' based on the page number."""
    return "Appendix" if page >= APPENDIX_START_PAGE else "DEDR"


def detect_edition_from_text(full_text: str) -> Optional[str]:
    """
    Heuristic fallback: detect edition from cross-reference markers in the
    entry text itself.

    Appendix entries point forward to DEDR, e.g. "[DEDR 4054]" or "DEDR 4054".
    DEDR entries point backward to older editions, e.g. "DED(S) 2913" or "DEDS 8".
    """
    # Appendix entries contain forward refs like [DEDR NNNN]
    if re.search(r"\[DEDR\s+\d+\]", full_text):
        return "Appendix"
    # DEDR entries end with backward refs like "DED(S, N) 56" or "DEDS 8"
    if re.search(r"DED(\(S(?:,\s*N)?\)|S)\s+\d+", full_text):
        return "DEDR"
    return None
