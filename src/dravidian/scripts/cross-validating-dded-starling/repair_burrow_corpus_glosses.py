"""
Repair cached Burrow attestation glosses using paragraph-level full_text.

This fixes scrape/parser artifacts such as:
- "ask-to cut" instead of "ask- to cut"
- "id.;ac-acroprickly." instead of "id.; ac-acro prickly."

Usage:
  python repair_burrow_corpus_glosses.py \
      validation_output/burrow_cache/burrow_corpus.json \
      --output validation_output/burrow_cache/burrow_corpus.cleaned.json
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from textnorm import recover_attestation_gloss_from_full_text


def _normalize_spacing(gloss: str) -> str:
    g = re.sub(r"\s+", " ", gloss or "").strip()
    # Normalize common collapsed id. pattern.
    g = re.sub(r"^id\.;\s*", "id.; ", g, flags=re.IGNORECASE)
    return g


# A grammatical/sense qualifier glued onto the FRONT of an "id." (idem)
# lexicographic reference, e.g. "(pl.) id." or "(obl. -r) id., arrowhead".
# Burrow attaches the qualifier to the SAME gloss as the "id." reference
# rather than treating it as a distinct meaning, so the simpler "id."/
# "id.;..." cases in the second pass below never match (kept in sync with
# the identical pattern in burrow_entry_parser.py's parse_language_sections,
# which runs the same resolution earlier but can miss this shape when the
# leading "(" was consumed into the headword capture during parsing and
# only restored here by recover_attestation_gloss_from_full_text above).
_QUALIFIED_ID_RE = re.compile(r"^\(([^)]*)\)\s*id\.\s*(.*)$", re.IGNORECASE)

# Strips a leading qualifier off an already-resolved last_real_gloss before
# re-prepending the CURRENT attestation's own qualifier, so a chain of
# several qualified "id." attestations (Konḍa -> Pe. -> Manḍ. in DED 5440)
# each show "(own qualifier) real meaning" instead of accumulating every
# earlier link's qualifier too (kept in sync with burrow_entry_parser.py).
_LEADING_PAREN_RE = re.compile(r"^\([^)]*\)\s*")


def repair_corpus(data: Dict[str, Any]) -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = data.get("entries", [])
    total_attestations = 0
    changed = 0

    for entry in entries:
        full_text = str(entry.get("full_text", "") or "")
        attestations = entry.get("attestations", [])
        if not isinstance(attestations, list):
            continue

        for att in attestations:
            if not isinstance(att, dict):
                continue
            total_attestations += 1

            abbrev = str(att.get("language_abbrev", "") or "")
            headwords = att.get("headwords", [])
            if not isinstance(headwords, list):
                headwords = []
            headwords = [str(h or "") for h in headwords]
            old_gloss = str(att.get("gloss", "") or "")

            repaired = recover_attestation_gloss_from_full_text(
                full_text,
                abbrev,
                headwords,
                old_gloss,
            )
            repaired = _normalize_spacing(repaired)

            if repaired != old_gloss:
                att["gloss"] = repaired
                changed += 1

    # Second pass: resolve "id." (idem) glosses within each entry.
    for entry in entries:
        attestations = entry.get("attestations", [])
        if not isinstance(attestations, list):
            continue
        last_real_gloss = ""
        for att in attestations:
            if not isinstance(att, dict):
                continue
            g = (att.get("gloss", "") or "").strip()
            m_qualified_id = _QUALIFIED_ID_RE.match(g)
            if g.lower() == "id.":
                if last_real_gloss:
                    att["gloss"] = last_real_gloss
                    changed += 1
            elif m_qualified_id:
                # e.g. "(pl.) id." -> "(pl.) <prev gloss>". Strip any
                # leading qualifier already on last_real_gloss first (it may
                # naturally have one of its own, e.g. Konḍa's own gloss is
                # "(pl. veRku) firewood, fuel.") so chained qualifiers don't
                # accumulate across multiple attestations.
                qualifier = m_qualified_id.group(1).strip()
                suffix = m_qualified_id.group(2).strip().lstrip(";").strip()
                if last_real_gloss:
                    bare_gloss = _LEADING_PAREN_RE.sub("", last_real_gloss).strip()
                    combined = f"({qualifier}) {bare_gloss}" if qualifier else bare_gloss
                    new_gloss = f"{combined}; {suffix}" if suffix else combined
                    if new_gloss != g:
                        att["gloss"] = new_gloss
                        changed += 1
            elif g.lower().startswith("id."):
                if last_real_gloss:
                    suffix = g[3:].lstrip(";").strip()
                    new_gloss = f"{last_real_gloss}; {suffix}" if suffix else last_real_gloss
                    if new_gloss != g:
                        att["gloss"] = new_gloss
                        changed += 1
                    g = att["gloss"]
                last_real_gloss = g
            else:
                last_real_gloss = g

    data["_repair_meta"] = {
        "repaired_attestations": changed,
        "total_attestations": total_attestations,
        "script": "repair_burrow_corpus_glosses.py",
    }
    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair Burrow corpus attestation glosses from full_text"
    )
    parser.add_argument("corpus", help="Path to burrow_corpus.json")
    parser.add_argument(
        "--output",
        help="Output path (default: <input>.cleaned.json)",
    )
    args = parser.parse_args()

    in_path = Path(args.corpus)
    out_path = Path(args.output) if args.output else in_path.with_suffix(".cleaned.json")

    with in_path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)

    repaired = repair_corpus(data)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8-sig") as f:
        json.dump(repaired, f, ensure_ascii=False, indent=2)

    meta = repaired.get("_repair_meta", {})
    print(
        f"Repaired {meta.get('repaired_attestations', 0)} / "
        f"{meta.get('total_attestations', 0)} attestations"
    )
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()

