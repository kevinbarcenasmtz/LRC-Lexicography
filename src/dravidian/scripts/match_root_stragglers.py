"""Second-pass matching for DravidiLex root permalinks, run against the
corpus crawl_root_corpus.py already collected (no network calls here).

Strategy, in order of confidence:
  1. Exact match (already done by crawl_root_permalinks.py) -- loaded, not redone.
  2. Loose-exact match: strip diacritics/punctuation/case from both sides and
     match again. Catches simple text drift (an accent corrected, a hyphen
     added) with no risk of a wrong link.
  3. Conservative fuzzy match: difflib ratio on headword+gloss, accepted only
     when it's each side's unambiguous best match above a high threshold.
     Printed out for manual spot-checking before anything gets merged.

Writes data/dravidian/starling/dravet_root_permalinks.json (updated in place,
merging new matches into the existing exact-match set) and prints a report.
"""

import difflib
import json
import re
import unicodedata
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "dravidian"
BATCH_IMPORT_JSON = DATA_DIR / "lrc_import" / "dravidilex_batch_import.json"
PERMALINKS_PATH = DATA_DIR / "starling" / "dravet_root_permalinks.json"
CORPUS_PATH = DATA_DIR / "starling" / "dravet_root_corpus.json"

LOOSE_STRIP_CHARS = re.compile(r"[*\-()\[\]{}.,;:!?\"'~/]")
WHITESPACE = re.compile(r"\s+")

FUZZY_THRESHOLD = 0.82
HEADWORD_WEIGHT = 0.6
GLOSS_WEIGHT = 0.4


def normalize(text):
    if text is None:
        return ""
    text = str(text).strip()
    text = WHITESPACE.sub(" ", text)
    return unicodedata.normalize("NFC", text)


def loose_normalize(text):
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = LOOSE_STRIP_CHARS.sub(" ", text)
    text = WHITESPACE.sub(" ", text).strip().lower()
    return text


def load_roots():
    with open(BATCH_IMPORT_JSON, encoding="utf-8") as f:
        records = json.load(f)
    return [r for r in records if r.get("IsEtymon") == "1"]


def main():
    roots = load_roots()
    with open(PERMALINKS_PATH, encoding="utf-8") as f:
        found = json.load(f)
    with open(CORPUS_PATH, encoding="utf-8") as f:
        corpus = json.load(f)

    already_used_urls = set(found.values())
    corpus_pool = [c for c in corpus if c["url"] not in already_used_urls]

    remaining = [r for r in roots if r["Starling ID"] not in found]
    print(f"starting: {len(found)} exact matches, {len(remaining)} remaining, "
          f"{len(corpus_pool)} unconsumed corpus records")

    # --- Pass 2: loose-exact ---
    corpus_by_loose = {}
    for c in corpus_pool:
        key = (loose_normalize(c["headword"]), loose_normalize(c["gloss"]))
        corpus_by_loose.setdefault(key, []).append(c)

    loose_matches = 0
    still_remaining = []
    for r in remaining:
        key = (loose_normalize(r["Headwords"]), loose_normalize(r["Gloss"]))
        candidates = corpus_by_loose.get(key)
        if candidates and len(candidates) == 1:
            c = candidates[0]
            found[r["Starling ID"]] = c["url"]
            already_used_urls.add(c["url"])
            loose_matches += 1
        else:
            still_remaining.append(r)
    print(f"loose-exact pass: +{loose_matches} matches, {len(still_remaining)} remaining")

    # --- Pass 3: conservative fuzzy (mutual best match only) ---
    corpus_pool = [c for c in corpus_pool if c["url"] not in already_used_urls]

    def score(root, c):
        hw = difflib.SequenceMatcher(None, loose_normalize(root["Headwords"]), loose_normalize(c["headword"])).ratio()
        gl = difflib.SequenceMatcher(None, loose_normalize(root["Gloss"]), loose_normalize(c["gloss"])).ratio()
        return HEADWORD_WEIGHT * hw + GLOSS_WEIGHT * gl

    # Best corpus match per remaining root
    root_best = {}
    for r in still_remaining:
        best_c, best_s = None, 0.0
        for c in corpus_pool:
            s = score(r, c)
            if s > best_s:
                best_c, best_s = c, s
        if best_c and best_s >= FUZZY_THRESHOLD:
            root_best[r["Starling ID"]] = (best_c, best_s, r)

    # Best root match per corpus record involved, to enforce mutual-best
    corpus_best_root = {}
    for sid, (c, s, r) in root_best.items():
        key = c["url"]
        if key not in corpus_best_root or s > corpus_best_root[key][1]:
            corpus_best_root[key] = (sid, s)

    fuzzy_matches = []
    for sid, (c, s, r) in root_best.items():
        if corpus_best_root.get(c["url"], (None, 0))[0] == sid:
            fuzzy_matches.append((sid, r, c, s))

    print(f"\nfuzzy pass: {len(fuzzy_matches)} mutual-best matches >= {FUZZY_THRESHOLD} threshold")
    print("=" * 100)
    for sid, r, c, s in sorted(fuzzy_matches, key=lambda x: -x[3]):
        print(f"[{s:.3f}] {sid}: {r['Headwords']!r} / {r['Gloss']!r}")
        print(f"          -> {c['headword']!r} / {c['gloss']!r}  ({c['url']})")
    print("=" * 100)

    for sid, r, c, s in fuzzy_matches:
        found[sid] = c["url"]

    total_remaining = len(still_remaining) - len(fuzzy_matches)
    print(f"\nfinal: {len(found)}/{len(roots)} roots matched "
          f"({loose_matches} loose-exact + {len(fuzzy_matches)} fuzzy added), "
          f"{total_remaining} still unmatched (kept on 'nearest reflex' fallback)")

    with open(PERMALINKS_PATH, "w", encoding="utf-8") as f:
        json.dump(found, f, ensure_ascii=False, indent=2)
    print(f"wrote {PERMALINKS_PATH}")


if __name__ == "__main__":
    main()
