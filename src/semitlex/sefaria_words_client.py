"""Sefaria Words API client used to enrich BDB text entries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from sefaria_config import API_CONFIG, HEADERS, SCRAPER_CONFIG


class SefariaWordsClient:
    """Client for `GET /api/words/{word}` with BDB filtering."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_word(self, word: str) -> List[Dict[str, Any]]:
        """Return all lexicon matches for a given word."""
        encoded_word = quote(word, safe="")
        endpoint = API_CONFIG["words_endpoint_template"].format(word=encoded_word)
        url = f"{API_CONFIG['base_url']}{endpoint}"
        response = self.session.get(url, timeout=SCRAPER_CONFIG["timeout_seconds"])
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        return []

    def fetch_bdb_entry(self, headword: str) -> Optional[Dict[str, Any]]:
        """
        Return the most relevant BDB Dictionary entry for a headword.

        Priority:
        1) Exact headword + parent_lexicon == "BDB Dictionary"
        2) Any parent_lexicon == "BDB Dictionary"
        """
        entries = self.fetch_word(headword)
        bdb_entries = [
            entry
            for entry in entries
            if isinstance(entry, dict)
            and entry.get("parent_lexicon") == "BDB Dictionary"
        ]
        if not bdb_entries:
            return None

        exact_match = [
            entry for entry in bdb_entries if entry.get("headword") == headword
        ]
        if exact_match:
            return exact_match[0]
        return bdb_entries[0]
