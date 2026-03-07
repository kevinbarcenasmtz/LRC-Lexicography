"""Ping Sefaria BDB API endpoint to verify connectivity."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests


CURRENT_DIR = Path(__file__).resolve().parent
SEMITLEX_DIR = CURRENT_DIR.parent
if str(SEMITLEX_DIR) not in sys.path:
    sys.path.insert(0, str(SEMITLEX_DIR))

from sefaria_config import API_CONFIG, HEADERS, SCRAPER_CONFIG  # noqa: E402


def main() -> int:
    url = f"{API_CONFIG['base_url']}{API_CONFIG['index_endpoint']}"
    started = time.perf_counter()
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=SCRAPER_CONFIG["timeout_seconds"],
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        response.raise_for_status()
        payload = response.json()
        output = {
            "ok": True,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "url": url,
            "keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except requests.RequestException as err:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        output = {
            "ok": False,
            "latency_ms": latency_ms,
            "url": url,
            "error": str(err),
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
