"""Build the DravidiLex pilot import files for the LRC platform.

Inputs
------
- data/dravidian/starling/output.xlsx
    Tree-structured Starling export (one row per word, linked by Parent Word ID),
    produced by src/dravidian/notebooks/destructuring_of_scraped_json.ipynb.
- data/dravidian/three-tier-language tree.xlsx
    Family / Subfamily / Language tiers (Krishnamurti 2003).

Outputs (data/dravidian/lrc_import/)
------------------------------------
- dravidian_starling_data.xlsx
    Same tree as output.xlsx, but every protoform row carries the DED
    number(s) of all reflexes in its subtree (all distinct values kept).
- dravidilex_languages.csv
    Family,Subfamily,Language rows for the Filament Utilities language
    uploader. Includes intermediate proto-languages that appear in the
    Starling data so protoform rows can resolve a Language at import time.
- dravidilex_batch_import.json
    Reflex rows in the Utilities uploader format (Headwords / Gloss /
    Language required; every other key lands in LexReflexExtraData).
    The tree is preserved through Starling ID / Parent Word ID extra data.
- dravidilex_batch_import.xlsx
    Human-reviewable spreadsheet of the same rows.
"""

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import openpyxl
from openpyxl.utils import get_column_letter

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data" / "dravidian"
STARLING_XLSX = DATA_DIR / "starling" / "output.xlsx"
TREE_XLSX = DATA_DIR / "three-tier-language tree.xlsx"
OUT_DIR = DATA_DIR / "lrc_import"

# Starling uses both spellings; keep one.
LANGUAGE_NORMALIZATION = {
    "Proto-North-Dravidian": "Proto-North Dravidian",
}

# Starling dialect/source-split names -> canonical language in the
# three-tier tree. The original Starling name is preserved per row in the
# "Language (Starling)" extra-data field.
DIALECT_TO_LANGUAGE = {
    # Gondi dialects / sources
    "Koya Gondi": "Gondi",
    "Muria Gondi": "Gondi",
    "Maria Gondi": "Gondi",
    "Maria Gondi (Mitchell)": "Gondi",
    "Maria Gondi (Lind)": "Gondi",
    "Maria Gondi (Smith)": "Gondi",
    "Betul Gondi": "Gondi",
    "Adilabad Gondi": "Gondi",
    "Mandla Gondi": "Gondi",
    "Mandla Gondi (Phailbus)": "Gondi",
    "Mandla Gondi (Williamson)": "Gondi",
    "Seoni Gondi": "Gondi",
    "Gommu Gondi": "Gondi",
    "Yeotmal Gondi": "Gondi",
    "Chindwara Gondi": "Gondi",
    "Durg Gondi": "Gondi",
    "Chanda Gondi": "Gondi",
    # Kuwi sources (tree spells the language "Kuvi")
    "Kuwi (Schulze)": "Kuvi",
    "Kuwi (Fitzgerald)": "Kuvi",
    "Kuwi (Israel)": "Kuvi",
    "Kuwi (Mahanti)": "Kuvi",
    "Sunkarametta Kuwi": "Kuvi",
    "Parja Kuwi": "Kuvi",
    "Tekriya Kuwi": "Kuvi",
    "Dongriya Kuwi": "Kuvi",
    # Kui dialects
    "Khuttia Kui": "Kui",
    # Gadba varieties (tree distinguishes Ollari from Gadaba)
    "Ollari Gadba": "Ollari",
    "Salur Gadba": "Gadaba",
    "Kondekor Gadba": "Gadaba",
    "Poya Gadba": "Gadaba",
    # Kolami sources
    "Kinwat Kolami": "Kolami",
    "Kolami (Setumadhava Rao)": "Kolami",
    # Telugu varieties
    "Telugu (Krishnamurti)": "Telugu",
    "Inscriptional Telugu": "Telugu",
    "Merolu Telugu": "Telugu",
    # Konda sources
    "Konda (Burrow/Bhattacharya)": "Konda",
    # Kasaba is a Nilgiri variety closest to Irula (DEDR frontmatter)
    "Kasaba": "Irula",
}

# Intermediate proto-languages reconstructed in Starling but absent from the
# three-tier tree; placed under the tier they belong to (Krishnamurti 2003).
PROTO_LANGUAGE_TIERS = {
    "Proto-Dravidian": ("Proto-Dravidian", "Proto-Dravidian"),
    "Proto-South Dravidian": ("Proto-South Dravidian", "Proto-South Dravidian"),
    "Proto-Central Dravidian": ("Proto-Central Dravidian", "Proto-Central Dravidian"),
    "Proto-North Dravidian": ("Proto-North Dravidian", "Proto-North Dravidian"),
    "Proto-Nilgiri": ("Proto-South Dravidian", "Proto-South Dravidian I (South Dravidian)"),
    "Proto-Telugu": ("Proto-South Dravidian", "Proto-South Dravidian II (South-Central Dravidian)"),
    "Proto-Gondi-Kui": ("Proto-South Dravidian", "Proto-South Dravidian II (South-Central Dravidian)"),
    "Proto-Gondi": ("Proto-South Dravidian", "Proto-South Dravidian II (South-Central Dravidian)"),
    "Proto-Kui-Kuwi": ("Proto-South Dravidian", "Proto-South Dravidian II (South-Central Dravidian)"),
    "Proto-Pengo-Manda": ("Proto-South Dravidian", "Proto-South Dravidian II (South-Central Dravidian)"),
    "Proto-Kolami-Gadba": ("Proto-Central Dravidian", "Proto-Central Dravidian"),
}

# The tree spreadsheet writes Tamil's subfamily without the "Proto-" prefix
# the other ten SD I rows use; normalize to the majority spelling.
SUBFAMILY_NORMALIZATION = {
    "South Dravidian I (South Dravidian)": "Proto-South Dravidian I (South Dravidian)",
}


def load_starling_rows():
    wb = openpyxl.load_workbook(STARLING_XLSX, read_only=True)
    ws = wb.active
    row_iter = ws.iter_rows(values_only=True)
    header = list(next(row_iter))
    rows = []
    for values in row_iter:
        row = dict(zip(header, values))
        for key in ("Language", "Parent Language"):
            if row.get(key) in LANGUAGE_NORMALIZATION:
                row[key] = LANGUAGE_NORMALIZATION[row[key]]
        rows.append(row)
    wb.close()
    return header, rows


def propagate_ded_numbers(rows):
    """Give every protoform row the DED number(s) of its whole subtree."""
    children = defaultdict(list)
    by_id = {}
    for row in rows:
        by_id[row["ID"]] = row
        if row["Parent Word ID"]:
            children[row["Parent Word ID"]].append(row["ID"])

    def own_ded(row):
        value = row.get("Number in DED")
        if value is None or str(value).strip() == "":
            return set()
        return {int(part) for part in re.findall(r"\d+", str(value))}

    subtree_cache = {}

    def subtree_ded(node_id):
        if node_id in subtree_cache:
            return subtree_cache[node_id]
        numbers = own_ded(by_id[node_id])
        for child_id in children[node_id]:
            numbers |= subtree_ded(child_id)
        subtree_cache[node_id] = numbers
        return numbers

    for row in rows:
        if row["Language"].startswith("Proto-"):
            numbers = subtree_ded(row["ID"])
            row["Number in DED"] = ", ".join(str(n) for n in sorted(numbers))
        elif row.get("Number in DED") not in (None, ""):
            row["Number in DED"] = str(int(row["Number in DED"]))
    return rows


def write_xlsx(path, header, rows):
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet("Sheet1")
    ws.append(header)
    for row in rows:
        ws.append([row.get(col) for col in header])
    wb.save(path)


def build_languages_csv():
    """Family,Subfamily,Language rows for the Utilities language uploader."""
    wb = openpyxl.load_workbook(TREE_XLSX)
    ws = wb.active
    entries = []
    seen = set()

    def add(family, subfamily, language):
        subfamily = subfamily or family
        subfamily = SUBFAMILY_NORMALIZATION.get(subfamily, subfamily)
        key = (family, subfamily, language)
        if key not in seen:
            seen.add(key)
            entries.append(key)

    row_iter = ws.iter_rows(values_only=True)
    next(row_iter)  # header
    for family, subfamily, language in row_iter:
        if family is None:
            continue
        # Family- or subfamily-only tier rows carry no importable language;
        # proto-languages get real rows from PROTO_LANGUAGE_TIERS below.
        if language:
            add(family.strip(), subfamily.strip() if subfamily else None, language.strip())

    for language, (family, subfamily) in PROTO_LANGUAGE_TIERS.items():
        add(family, subfamily, language)

    return entries


def build_import_rows(header, rows):
    """Map tree rows onto the Utilities reflex-upload format."""
    extra_columns = [col for col in header if col not in ("Headword", "Meaning", "Language")]
    import_rows = []
    for row in rows:
        starling_language = row["Language"]
        language = DIALECT_TO_LANGUAGE.get(starling_language, starling_language)
        record = {
            "Headwords": str(row["Headword"]).strip(),
            "Gloss": str(row["Meaning"]).strip() if row.get("Meaning") else "",
            "Language": language,
        }
        if language != starling_language:
            record["Language (Starling)"] = starling_language
        for col in extra_columns:
            value = row.get(col)
            if value is None or str(value).strip() == "":
                continue
            key = "Starling ID" if col == "ID" else col
            record[key] = str(value).strip()
        import_rows.append(record)
    return import_rows


def main():
    OUT_DIR.mkdir(exist_ok=True)

    header, rows = load_starling_rows()
    rows = propagate_ded_numbers(rows)

    tree_path = OUT_DIR / "dravidian_starling_data.xlsx"
    write_xlsx(tree_path, header, rows)
    print(f"wrote {tree_path.relative_to(REPO_ROOT)} ({len(rows)} rows)")

    languages = build_languages_csv()
    lang_path = OUT_DIR / "dravidilex_languages.csv"
    with open(lang_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Family", "Subfamily", "Language"])
        writer.writerows(languages)
    print(f"wrote {lang_path.relative_to(REPO_ROOT)} ({len(languages)} languages)")

    import_rows = build_import_rows(header, rows)

    known_languages = {language for _, _, language in languages}
    unmapped = sorted({r["Language"] for r in import_rows} - known_languages)
    if unmapped:
        raise SystemExit(f"languages missing from languages CSV: {unmapped}")

    json_path = OUT_DIR / "dravidilex_batch_import.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(import_rows, f, ensure_ascii=False, indent=1)
    print(f"wrote {json_path.relative_to(REPO_ROOT)} ({len(import_rows)} records)")

    import_columns = ["Headwords", "Gloss", "Language", "Language (Starling)"]
    for record in import_rows:
        for key in record:
            if key not in import_columns:
                import_columns.append(key)
    xlsx_path = OUT_DIR / "dravidilex_batch_import.xlsx"
    write_xlsx(xlsx_path, import_columns, import_rows)
    print(f"wrote {xlsx_path.relative_to(REPO_ROOT)} ({len(import_rows)} rows)")


if __name__ == "__main__":
    main()
