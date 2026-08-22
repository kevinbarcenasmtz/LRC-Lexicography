"""One-time crawl to recover real Starling permalinks for DravidiLex root/etymon
records.

Starling exposes no permalink for a root record anywhere in the HTML we scrape
(neither the paginated listing nor, obviously, anywhere else) -- but a direct
single-record view DOES work if you already know its internal text_number:

    https://starlingdb.org/cgi-bin/response.cgi?single=1&basename=%2fdata%2fdrav%2fdravet&text_number=N&root=config

text_number has gaps (deleted/merged entries in Starling's own DB) and is not
derivable from page/position math, so the only way to recover it is to walk
text_number = 1..CAP and match each returned record back to one of our 2,211
roots by (headword, gloss) -- the same text Starling itself renders, sourced
from the same original scrape.

Writes data/dravidian/starling/dravet_root_permalinks.json:
    { "<Starling ID>": "<https://... url>", ... }
"""

import json
import re
import time
import unicodedata
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "dravidian"
BATCH_IMPORT_JSON = DATA_DIR / "lrc_import" / "dravidilex_batch_import.json"
OUT_PATH = DATA_DIR / "starling" / "dravet_root_permalinks.json"
CHECKPOINT_PATH = DATA_DIR / "starling" / "dravet_root_permalinks.checkpoint.json"

BASE_URL = "https://starlingdb.org/cgi-bin/response.cgi"
BASENAME = "/data/drav/dravet"
CAP = 2500
DELAY = 0.8  # matches dravidian_scraper.py's polite per-record delay
MAX_CONSECUTIVE_MISSES = 300  # stop early if we clearly ran off the end of the table
CHECKPOINT_EVERY = 25  # requests between checkpoint saves

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def normalize(text):
    if text is None:
        return ""
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    # The live page and our stored scrape don't always agree on precomposed
    # vs. decomposed diacritics (e.g. "aŋā́ḍi" byte-differs from itself across
    # sources) -- normalize so matching isn't sensitive to that.
    text = unicodedata.normalize("NFC", text)
    return text


def build_lookup():
    """(headword, gloss) -> Starling ID, for every root we need a permalink for."""
    with open(BATCH_IMPORT_JSON, encoding="utf-8") as f:
        records = json.load(f)
    lookup = {}
    dupes = 0
    for r in records:
        if r.get("IsEtymon") != "1":
            continue
        key = (normalize(r.get("Headwords")), normalize(r.get("Gloss")))
        if key in lookup:
            dupes += 1
            continue
        lookup[key] = r.get("Starling ID")
    print(f"built lookup for {len(lookup)} roots ({dupes} headword+gloss collisions skipped)", flush=True)
    return lookup


def fetch_record(session, text_number):
    url = f"{BASE_URL}?single=1&basename={quote(BASENAME, safe='')}&text_number={text_number}&root=config"
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f"  [{text_number}] request error: {e}", flush=True)
        return None, url
    if resp.status_code != 200:
        return None, url
    soup = BeautifulSoup(resp.text, "html.parser")
    record_div = soup.find("div", class_="results_record")
    if not record_div:
        return None, url
    headword = None
    gloss = None
    for div in record_div.find_all("div", recursive=False):
        field_span = div.find("span", class_="fld")
        if not field_span:
            continue
        field_name = field_span.get_text(strip=True).rstrip(":").strip()
        value_span = div.find("span", class_="unicode")
        value = value_span.get_text(" ", strip=True) if value_span else ""
        if field_name == "Proto-Dravidian":
            headword = value
        elif field_name == "Meaning":
            gloss = value
    if headword is None or gloss is None:
        return None, url
    return (normalize(headword), normalize(gloss)), url


def save_checkpoint(text_number, found):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_text_number": text_number, "found": found}, f, ensure_ascii=False)


def load_checkpoint():
    if not CHECKPOINT_PATH.exists():
        return 0, {}
    with open(CHECKPOINT_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return data["last_text_number"], data["found"]


def main():
    lookup = build_lookup()
    start_text_number, found = load_checkpoint()
    remaining = {k: v for k, v in lookup.items() if v not in found.values()}
    if start_text_number:
        print(f"resuming from checkpoint: text_number={start_text_number}, "
              f"{len(found)} already found, {len(remaining)} remaining", flush=True)

    session = requests.Session()
    consecutive_misses = 0
    text_number = start_text_number
    while text_number < CAP and remaining:
        text_number += 1
        key, url = fetch_record(session, text_number)
        if key is None:
            consecutive_misses += 1
        else:
            consecutive_misses = 0
            starling_id = remaining.pop(key, None)
            if starling_id:
                found[starling_id] = url
        if text_number % 25 == 0:
            print(f"  ...{text_number}/{CAP}, matched {len(found)}/{len(lookup)}, remaining {len(remaining)}", flush=True)
        if text_number % CHECKPOINT_EVERY == 0:
            save_checkpoint(text_number, found)
        if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
            print(f"  {MAX_CONSECUTIVE_MISSES} consecutive misses at text_number={text_number}, stopping early", flush=True)
            break
        time.sleep(DELAY)

    save_checkpoint(text_number, found)
    print(f"done: matched {len(found)}/{len(lookup)} roots, scanned up to text_number={text_number}", flush=True)
    if remaining:
        print(f"  {len(remaining)} roots never matched (kept using the 'nearest reflex' fallback link)", flush=True)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(found, f, ensure_ascii=False, indent=2)
    print(f"wrote {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
