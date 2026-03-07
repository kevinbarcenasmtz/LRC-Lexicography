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


def _recover_attestation_gloss_from_full_text(
    full_text: str,
    source_abbrev: str,
    source_headword: str,
    fallback_gloss: str,
) -> str:
    if not full_text or not source_abbrev or not source_headword:
        return fallback_gloss

    normalized = re.sub(r"\s+", " ", full_text).strip()
    marker = f"{source_abbrev.strip()} {source_headword.strip()}"
    marker_pos = normalized.lower().find(marker.lower())
    if marker_pos < 0:
        return fallback_gloss

    tail = normalized[marker_pos + len(marker) :].strip()

    # Stop at next top-level language token to prevent bleed into next attestation.
    ignore_tokens = {
        "Tr.",
        "W.",
        "Ph.",
        "Mu.",
        "Ma.",
        "A.",
        "Ch.",
        "Voc.",
        "Cf.",
        "e.g.",
    }
    for m in re.finditer(r"\s([A-Z][A-Za-zÀ-ÖØ-öø-ÿĀ-žḀ-ỿ]+\.)\s+\S", tail):
        tok = m.group(1)
        if tok in ignore_tokens:
            continue
        tail = tail[: m.start()].strip()
        break

    tail = re.sub(r"\s+DEDS?\b.*$", "", tail, flags=re.IGNORECASE).strip()
    if not tail:
        return fallback_gloss
    return tail if len(tail) > len(fallback_gloss) else fallback_gloss


def _normalize_spacing(gloss: str) -> str:
    g = re.sub(r"\s+", " ", gloss or "").strip()
    # Normalize common collapsed id. pattern.
    g = re.sub(r"^id\.;\s*", "id.; ", g, flags=re.IGNORECASE)
    return g


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
            headword = ""
            if isinstance(headwords, list) and headwords:
                headword = str(headwords[0] or "")
            old_gloss = str(att.get("gloss", "") or "")

            repaired = _recover_attestation_gloss_from_full_text(
                full_text,
                abbrev,
                headword,
                old_gloss,
            )
            repaired = _normalize_spacing(repaired)

            if repaired != old_gloss:
                att["gloss"] = repaired
                changed += 1

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

