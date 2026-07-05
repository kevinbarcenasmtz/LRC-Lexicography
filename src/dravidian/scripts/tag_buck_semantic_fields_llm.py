"""Classify DravidiLex etyma into Buck semantic fields with Claude (v3).

Unlike the embedding taggers (v1 MiniLM, v2 MPNet+enrichment), this asks a
generative model to pick the field — it knows that "brinjal" is an eggplant
and that "that (over there)" is a demonstrative, the exact cases where
embeddings failed. Design (per the Claude API guidance):

- model: claude-haiku-4-5 — cheap classification tier, approved for this task
- one system prompt carries the task + the full 1,098-field Buck taxonomy,
  marked with cache_control so requests 2..N read it at ~0.1x price
- 25 etyma per request (89 requests total), each with its gloss plus a few
  reflex/Burrow meanings for context
- structured outputs (output_config.format json_schema) guarantee parseable
  responses; the SDK retries 429/5xx automatically
- suggestions validated against the known abbr list; invalids blanked

Output: data/dravidian/lrc_import/buck_tag_suggestions_v3_llm.pending-review.csv
with the v1/v2 choices alongside for three-way comparison.

Run:  ANTHROPIC_API_KEY=... lrc_venv/bin/python src/dravidian/scripts/tag_buck_semantic_fields_llm.py
"""

import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tag_buck_semantic_fields import load_buck_fields, load_etyma_with_context  # noqa: E402

import anthropic  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "data" / "dravidian" / "lrc_import"
OUT_CSV = OUT_DIR / "buck_tag_suggestions_v3_llm.pending-review.csv"
V1_CSV = OUT_DIR / "buck_tag_suggestions.csv"
V2_CSV = OUT_DIR / "buck_tag_suggestions_v2.pending-review.csv"

MODEL = "claude-haiku-4-5"
BATCH_SIZE = 25
MAX_CONTEXT_MEANINGS = 6

OUTPUT_SCHEMA = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "properties": {
            "assignments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "abbr": {"type": "string"},
                        "second_abbr": {"type": "string"},
                    },
                    "required": ["id", "abbr", "second_abbr"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["assignments"],
        "additionalProperties": False,
    },
}


def build_system_prompt(fields):
    taxonomy = "\n".join(f"{f['abbr']}\t{f['label']}" for f in fields)
    return (
        "You are a historical lexicographer assigning semantic-field tags to "
        "Proto-Dravidian etyma for a comparative lexicon. Each etymon has an "
        "English gloss (and sometimes related meanings from its reflexes and "
        "from Burrow & Emeneau's Dravidian Etymological Dictionary).\n\n"
        "Assign each etymon the single best-fitting field from Buck's semantic "
        "classification below, plus a second-best alternative. Rules:\n"
        "- 'abbr' and 'second_abbr' MUST be abbreviations copied exactly from "
        "the taxonomy; never invent one.\n"
        "- Classify by the core meaning of the gloss, not incidental words. "
        "Culture-specific items map to their category (e.g. a vegetable name "
        "-> the food/plant field that fits best).\n"
        "- Grammatical/function words (demonstratives, pronouns, particles) "
        "rarely have a perfect field; choose the closest available (e.g. "
        "spatial deictics -> a Spatial Relations field).\n"
        "- Return one assignment per input id, in the same order.\n\n"
        "BUCK SEMANTIC FIELDS (abbr<TAB>category — field):\n" + taxonomy
    )


def main():
    fields = load_buck_fields()
    valid_abbrs = {f["abbr"] for f in fields}
    etyma = load_etyma_with_context()
    print(f"{len(etyma)} etyma, {len(fields)} Buck fields, model {MODEL}", flush=True)

    prior = {}
    for path, key in ((V1_CSV, "v1"), (V2_CSV, "v2")):
        if path.exists():
            with open(path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    prior.setdefault(row["Starling ID"], {})[key] = row["chosen_abbr"]

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
    system = [{
        "type": "text",
        "text": build_system_prompt(fields),
        "cache_control": {"type": "ephemeral"},
    }]

    results = {}
    cache_reads = 0
    for start in range(0, len(etyma), BATCH_SIZE):
        batch = etyma[start:start + BATCH_SIZE]
        lines = []
        for e in batch:
            ctx = "; ".join(e["meanings"][:MAX_CONTEXT_MEANINGS])
            line = f"{e['id']} | gloss: {e['clean_gloss']}"
            if ctx:
                line += f" | related: {ctx}"
            lines.append(line)
        prompt = "Classify these etyma:\n" + "\n".join(lines)

        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system,
            output_config={"format": OUTPUT_SCHEMA},
            messages=[{"role": "user", "content": prompt}],
        )
        cache_reads += response.usage.cache_read_input_tokens or 0
        text = next(b.text for b in response.content if b.type == "text")
        for a in json.loads(text)["assignments"]:
            abbr = a["abbr"] if a["abbr"] in valid_abbrs else ""
            second = a["second_abbr"] if a["second_abbr"] in valid_abbrs else ""
            results[a["id"]] = (abbr, second)
        done = min(start + BATCH_SIZE, len(etyma))
        print(f"  {done}/{len(etyma)} classified", flush=True)

    labels = {f["abbr"]: f["label"] for f in fields}
    n_v1_agree = n_v2_agree = n_all_agree = n_missing = 0
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Starling ID", "Headwords", "Gloss", "review_tier",
                         "chosen_abbr", "chosen_field",
                         "llm_second", "second_field",
                         "v1_choice", "v1_field", "v2_choice", "v2_field",
                         "agrees_v1", "agrees_v2"])
        for e in etyma:
            abbr, second = results.get(e["id"], ("", ""))
            if not abbr:
                n_missing += 1
            p = prior.get(e["id"], {})
            v1, v2 = p.get("v1", ""), p.get("v2", "")
            a1 = "YES" if abbr and abbr == v1 else ""
            a2 = "YES" if abbr and abbr == v2 else ""
            n_v1_agree += bool(a1)
            n_v2_agree += bool(a2)
            n_all_agree += bool(a1 and a2)
            if not abbr:
                tier = "4-unclassified"
            elif a1 and a2:
                tier = "1-all-three-agree"
            elif a1 or a2:
                tier = "2-llm-plus-one"
            else:
                tier = "3-llm-alone"
            writer.writerow([e["id"], e["headwords"], e["gloss"], tier,
                             abbr, labels.get(abbr, ""),
                             second, labels.get(second, ""),
                             v1, labels.get(v1, ""), v2, labels.get(v2, ""),
                             a1, a2])

    print(f"wrote {OUT_CSV.relative_to(REPO_ROOT)}")
    print(f"agreement: llm=v1 {n_v1_agree}, llm=v2 {n_v2_agree}, "
          f"all three {n_all_agree}, unclassified {n_missing}")
    print(f"cache read tokens total: {cache_reads}")


if __name__ == "__main__":
    main()
