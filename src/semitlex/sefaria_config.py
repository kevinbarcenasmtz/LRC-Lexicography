"""Configuration for Sefaria BDB scraping."""

from __future__ import annotations

API_CONFIG = {
    "base_url": "https://www.sefaria.org",
    "index_endpoint": "/api/index/BDB",
    "texts_endpoint_template": "/api/texts/{ref}",
    "words_endpoint_template": "/api/words/{word}",
}

SCRAPER_CONFIG = {
    # Traversal is performed via `next` from each /api/texts response.
    "bdb_start_ref": "BDB, א",
    "bdb_end_ref": "BDB, תִּשְׁעִים",
    # Test with 1-3 entries first. Set to None for full run.
    "subset_size": 3,
    # Ample delay for respectful long-running lab-machine scraping.
    "request_delay_seconds": 4.5,
    "timeout_seconds": 30,
    "max_retries": 3,
    "backoff_multiplier": 2.0,
    "retry_base_wait_seconds": 2.0,
    "checkpoint_every_n_entries": 1,
    "default_output_dir": "data/semitlex",
    "checkpoint_filename": "sefaria_bdb_checkpoint.json",
    "output_json_filename": "sefaria_bdb_complete.json",
    "output_xlsx_filename": "sefaria_bdb_lexicon.xlsx",
    "failed_refs_filename": "failed_refs.csv",
    "run_metadata_filename": "run_metadata.json",
}

HEADERS = {
    "User-Agent": "LRC-Lexicography-SefariaBDBScraper/1.0 (lexicography research)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
