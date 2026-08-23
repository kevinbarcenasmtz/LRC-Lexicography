"""Companion to crawl_root_permalinks.py: scrapes every (headword, gloss, url)
triple from basename=dravet, text_number=1..CAP, without trying to match
anything. Feeds fuzzy_match_stragglers.py so matching logic can be iterated
on without re-hitting the network.

Writes data/dravidian/starling/dravet_root_corpus.json:
    [ {"text_number": N, "headword": "...", "gloss": "...", "url": "..."}, ... ]
"""

import json
import time
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "dravidian"
OUT_PATH = DATA_DIR / "starling" / "dravet_root_corpus.json"
CHECKPOINT_PATH = DATA_DIR / "starling" / "dravet_root_corpus.checkpoint.json"

BASE_URL = "https://starlingdb.org/cgi-bin/response.cgi"
BASENAME = "/data/drav/dravet"
CAP = 2500
DELAY = 0.8
CHECKPOINT_EVERY = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def fetch_record(session, text_number):
    url = f"{BASE_URL}?single=1&basename={quote(BASENAME, safe='')}&text_number={text_number}&root=config"
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f"  [{text_number}] request error: {e}", flush=True)
        return None
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    record_div = soup.find("div", class_="results_record")
    if not record_div:
        return None
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
        return None
    return {"text_number": text_number, "headword": headword, "gloss": gloss, "url": url}


def save_checkpoint(text_number, corpus):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_text_number": text_number, "corpus": corpus}, f, ensure_ascii=False)


def load_checkpoint():
    if not CHECKPOINT_PATH.exists():
        return 0, []
    with open(CHECKPOINT_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return data["last_text_number"], data["corpus"]


def main():
    start_text_number, corpus = load_checkpoint()
    if start_text_number:
        print(f"resuming from checkpoint: text_number={start_text_number}, {len(corpus)} records so far", flush=True)

    session = requests.Session()
    text_number = start_text_number
    while text_number < CAP:
        text_number += 1
        rec = fetch_record(session, text_number)
        if rec:
            corpus.append(rec)
        if text_number % 25 == 0:
            print(f"  ...{text_number}/{CAP}, corpus size {len(corpus)}", flush=True)
        if text_number % CHECKPOINT_EVERY == 0:
            save_checkpoint(text_number, corpus)
        time.sleep(DELAY)

    save_checkpoint(text_number, corpus)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)
    print(f"done: {len(corpus)} records scanned up to text_number={text_number}", flush=True)
    print(f"wrote {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
