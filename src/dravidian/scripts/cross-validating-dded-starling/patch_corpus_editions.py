"""
Retroactively tag existing burrow_corpus.json entries with edition info
(DEDR vs Appendix) without re-scraping.

Usage:
    python patch_corpus_editions.py burrow_corpus.json
    python patch_corpus_editions.py burrow_corpus.json --output patched_corpus.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from editions import classify_edition, detect_edition_from_text


def patch_corpus(input_path: str, output_path: str | None = None) -> None:
    path = Path(input_path)
    with open(path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    entries = data.get("entries", [])
    patched = 0
    collisions_fixed = 0

    for entry in entries:
        page = entry.get("page", 0)
        ded_number = entry.get("ded_number")

        # Skip entries already tagged
        if "edition" in entry:
            continue

        edition = classify_edition(page)
        full_text = entry.get("full_text", "")
        text_edition = detect_edition_from_text(full_text)
        if text_edition and text_edition != edition:
            edition = text_edition

        entry["edition"] = edition
        entry["ded_number_raw"] = ded_number

        if edition == "Appendix" and ded_number is not None:
            if not str(ded_number).startswith("App."):
                entry["ded_number"] = f"App.{ded_number}"
                collisions_fixed += 1

        patched += 1

    # Summary
    dedr_count = sum(1 for e in entries if e.get("edition") == "DEDR")
    app_count = sum(1 for e in entries if e.get("edition") == "Appendix")

    print(f"Patched {patched} entries")
    print(f"  DEDR: {dedr_count}")
    print(f"  Appendix: {app_count}")
    print(f"  Collisions fixed (number prefixed with App.): {collisions_fixed}")

    out = Path(output_path) if output_path else path
    with open(out, "w", encoding="utf-8-sig") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved to: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Patch existing burrow_corpus.json with edition tags"
    )
    parser.add_argument("corpus", help="Path to burrow_corpus.json")
    parser.add_argument("--output", help="Output path (default: overwrite input)")
    args = parser.parse_args()
    patch_corpus(args.corpus, args.output)


if __name__ == "__main__":
    main()
