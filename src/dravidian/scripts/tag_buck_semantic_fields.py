"""Suggest Buck semantic-field tags for the DravidiLex etyma.

Zero-shot classification, v2: each etymon is represented two ways —
(a) its Starling proto gloss, and (b) an enriched document that adds the
distinct meanings of every reflex in its subtree plus the Burrow DED
attestation glosses joined via the root's DED number(s) (96% of roots join).
Both are embedded with a sentence-transformer and compared against Buck's
1,098 semantic fields (field text + its category, e.g. "Physical World —
Earth, Land"); the final score averages the two cosine similarities, so the
terse proto gloss stays authoritative while the enrichment disambiguates
short/ambiguous glosses ("mark, token", "leech", "that (over there)").

No training data exists, so this is a *first pass for human review*.

Input
-----
- data/dravidian/lrc_import/dravidilex_batch_import.json (roots + subtree)
- data/dravidian/burrow_ded/burrow_corpus.cleaned.json (DED glosses)
- data/dravidian/lrc_import/buck_semantic_category.csv / buck_semantic_field.csv

Output
------
- data/dravidian/lrc_import/buck_tag_suggestions_v2.pending-review.csv —
  one row per etymon: top-3 suggested fields with scores, `chosen_abbr`
  prefilled with the top suggestion, plus the v1 choice and a diff flag so
  review effort can focus on rows where v2 disagrees with v1. After review,
  rename to buck_tag_suggestions.csv and re-run build_dravidilex_import.py.

Run with the repo venv: lrc_venv/bin/python src/dravidian/scripts/tag_buck_semantic_fields.py
"""

import csv
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "data" / "dravidian" / "lrc_import"
BURROW_JSON = REPO_ROOT / "data" / "dravidian" / "burrow_ded" / "burrow_corpus.cleaned.json"
V1_CSV = OUT_DIR / "buck_tag_suggestions.csv"
OUT_CSV = OUT_DIR / "buck_tag_suggestions_v2.pending-review.csv"

# all-mpnet-base-v2 is markedly stronger than MiniLM for short-text semantics;
# still runs locally on CPU in a few minutes for this volume.
MODEL_NAME = "all-mpnet-base-v2"
TOP_K = 3
GLOSS_WEIGHT = 0.5  # rest goes to the enriched document
MAX_MEANINGS = 15
MAX_DOC_CHARS = 1200


def clean_meaning(text):
    """First clause of a gloss, minus grammar markers and 'id.' echoes."""
    text = re.sub(r"\s*\((?:n|v|adj|adv|pl|sg|hon|intr|tr)\.?\)\s*", " ", text)
    text = text.split(";")[0]
    text = re.sub(r"\s+", " ", text).strip(" .,")
    if text.lower() in ("id", "ditto", ""):
        return None
    return text


def load_buck_fields():
    categories = {}
    with open(OUT_DIR / "buck_semantic_category.csv", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            categories[row["abbr"]] = row["text"]

    fields = []
    with open(OUT_DIR / "buck_semantic_field.csv", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            abbr = row["abbr"]
            if not abbr or abbr.startswith("None"):
                continue
            category = categories.get(abbr.split("_")[0], "")
            fields.append({
                "abbr": abbr,
                "number": row["number"],
                "text": row["text"],
                "label": f"{category} — {row['text']}" if category else row["text"],
            })
    return fields


def load_etyma_with_context():
    """Roots in file order, each with subtree reflex meanings (file order =
    tree order, so every non-root row belongs to the most recent root)."""
    with open(OUT_DIR / "dravidilex_batch_import.json", encoding="utf-8") as f:
        records = json.load(f)

    burrow = json.load(open(BURROW_JSON, encoding="utf-8-sig"))
    burrow_by_ded = {}
    for entry in burrow["entries"]:
        burrow_by_ded.setdefault(str(entry["ded_number"]), entry)

    etyma = []
    current = None
    for r in records:
        gloss = r.get("Gloss", "").strip()
        if r.get("IsEtymon"):
            clean = clean_meaning(gloss) or gloss
            current = {
                "id": r["Starling ID"],
                "headwords": r["Headwords"],
                "gloss": gloss,
                "clean_gloss": clean,
                "ded_numbers": [n.strip() for n in r.get("Number in DED", "").split(",") if n.strip()],
                "meanings": [],
            }
            etyma.append(current)
        elif current is not None:
            m = clean_meaning(gloss) if gloss else None
            if m and m.lower() != current["clean_gloss"].lower() and m not in current["meanings"]:
                current["meanings"].append(m)

    for etymon in etyma:
        for ded in etymon["ded_numbers"]:
            entry = burrow_by_ded.get(ded)
            if not entry:
                continue
            for att in entry.get("attestations", []):
                m = clean_meaning(att.get("gloss", "") or "")
                if m and m not in etymon["meanings"] and m.lower() != etymon["clean_gloss"].lower():
                    etymon["meanings"].append(m)

        doc = etymon["clean_gloss"]
        if etymon["meanings"]:
            doc += ". Related meanings: " + "; ".join(etymon["meanings"][:MAX_MEANINGS])
        etymon["doc"] = doc[:MAX_DOC_CHARS]
    return etyma


def load_v1_choices():
    if not V1_CSV.exists():
        return {}
    with open(V1_CSV, encoding="utf-8") as f:
        return {r["Starling ID"]: r["chosen_abbr"] for r in csv.DictReader(f)}


def main():
    fields = load_buck_fields()
    etyma = load_etyma_with_context()
    v1 = load_v1_choices()
    enriched = sum(1 for e in etyma if e["meanings"])
    print(f"{len(etyma)} etyma ({enriched} with enrichment), {len(fields)} Buck fields")

    from sentence_transformers import SentenceTransformer, util

    model = SentenceTransformer(MODEL_NAME)
    field_vecs = model.encode([f["label"] for f in fields], normalize_embeddings=True,
                              show_progress_bar=True)
    gloss_vecs = model.encode([e["clean_gloss"] for e in etyma], normalize_embeddings=True,
                              show_progress_bar=True)
    doc_vecs = model.encode([e["doc"] for e in etyma], normalize_embeddings=True,
                            show_progress_bar=True)
    scores = GLOSS_WEIGHT * util.cos_sim(gloss_vecs, field_vecs) \
        + (1 - GLOSS_WEIGHT) * util.cos_sim(doc_vecs, field_vecs)

    n_changed = 0
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["Starling ID", "Headwords", "Gloss", "chosen_abbr",
                  "v1_choice", "differs_from_v1", "enrichment_used"]
        for k in range(1, TOP_K + 1):
            header += [f"suggestion_{k}", f"field_{k}", f"score_{k}"]
        writer.writerow(header)
        for i, etymon in enumerate(etyma):
            top = scores[i].argsort(descending=True)[:TOP_K].tolist()
            new_choice = fields[top[0]]["abbr"]
            old_choice = v1.get(etymon["id"], "")
            differs = "YES" if old_choice and new_choice != old_choice else ""
            if differs:
                n_changed += 1
            row = [etymon["id"], etymon["headwords"], etymon["gloss"], new_choice,
                   old_choice, differs, "yes" if etymon["meanings"] else ""]
            for j in top:
                row += [fields[j]["abbr"], fields[j]["label"], f"{scores[i][j].item():.3f}"]
            writer.writerow(row)

    print(f"wrote {OUT_CSV.relative_to(REPO_ROOT)}")
    print(f"{n_changed} of {len(etyma)} differ from v1 — review those rows first.")
    print("After review: rename to buck_tag_suggestions.csv and re-run build_dravidilex_import.py.")


if __name__ == "__main__":
    main()
