"""Suggest Buck semantic-field tags for the DravidiLex etyma.

Zero-shot classification: embed each root reconstruction's English gloss and
each of Buck's semantic fields (field text + its category, e.g.
"Physical World — Earth, Land") with a sentence-transformer, and rank fields
by cosine similarity. No training data exists, so this is a *first pass for
human review*, not a final assignment.

Input
-----
- data/dravidian/lrc_import/dravidilex_batch_import.json (root rows only)
- data/dravidian/lrc_import/buck_semantic_category.csv / buck_semantic_field.csv
  (copied from the MayaLex import data in linguistics_research_center)

Output
------
- data/dravidian/lrc_import/buck_tag_suggestions.csv — one row per etymon:
  Starling ID, Headwords, Gloss, top-3 suggested fields with scores, and a
  `chosen_abbr` column prefilled with the top suggestion. Reviewers edit or
  blank out `chosen_abbr`; build_dravidilex_import.py then injects the chosen
  tags as a 'Semantic Tag (Buck)' extra-data column on root rows.

Run with the repo venv: lrc_venv/bin/python src/dravidian/scripts/tag_buck_semantic_fields.py
"""

import csv
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "data" / "dravidian" / "lrc_import"
MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 3


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
            category_abbr = abbr.split("_")[0]
            category = categories.get(category_abbr, "")
            fields.append({
                "abbr": abbr,
                "number": row["number"],
                "text": row["text"],
                "label": f"{category} — {row['text']}" if category else row["text"],
            })
    return fields


def load_etyma():
    with open(OUT_DIR / "dravidilex_batch_import.json", encoding="utf-8") as f:
        records = json.load(f)
    etyma = []
    for r in records:
        if not r.get("IsEtymon"):
            continue
        gloss = r.get("Gloss", "").strip()
        # strip part-of-speech parentheticals that add noise, e.g. "mould (n.)"
        clean = re.sub(r"\s*\((?:n|v|adj|adv)\.?\)\s*", " ", gloss).strip()
        etyma.append({
            "id": r["Starling ID"],
            "headwords": r["Headwords"],
            "gloss": gloss,
            "clean_gloss": clean or gloss,
        })
    return etyma


def main():
    fields = load_buck_fields()
    etyma = load_etyma()
    print(f"{len(etyma)} etyma glosses, {len(fields)} Buck fields")

    from sentence_transformers import SentenceTransformer, util

    model = SentenceTransformer(MODEL_NAME)
    field_vecs = model.encode([f["label"] for f in fields], normalize_embeddings=True,
                              show_progress_bar=True)
    gloss_vecs = model.encode([e["clean_gloss"] for e in etyma], normalize_embeddings=True,
                              show_progress_bar=True)
    scores = util.cos_sim(gloss_vecs, field_vecs)  # (n_etyma, n_fields)

    out_path = OUT_DIR / "buck_tag_suggestions.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["Starling ID", "Headwords", "Gloss", "chosen_abbr"]
        for k in range(1, TOP_K + 1):
            header += [f"suggestion_{k}", f"field_{k}", f"score_{k}"]
        writer.writerow(header)
        for i, etymon in enumerate(etyma):
            top = scores[i].argsort(descending=True)[:TOP_K].tolist()
            row = [etymon["id"], etymon["headwords"], etymon["gloss"], fields[top[0]]["abbr"]]
            for j in top:
                row += [fields[j]["abbr"], fields[j]["label"], f"{scores[i][j].item():.3f}"]
            writer.writerow(row)
    print(f"wrote {out_path.relative_to(REPO_ROOT)}")
    print("Review the file (edit/blank 'chosen_abbr'), then re-run "
          "build_dravidilex_import.py to inject tags into the import files.")


if __name__ == "__main__":
    main()
