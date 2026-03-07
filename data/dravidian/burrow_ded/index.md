# Context Of This Directory

This directory contains the cached Burrow DED corpus files used by the
Starling tree validator in:

`src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py`

The validator cross-checks Starling proto-tree language entries against Burrow
DED paragraph attestations.

## Core Task (Validation Goal)

For each Starling record with a DED number:

1. Look up the corresponding Burrow DED paragraph.
2. Verify each language entry under that Starling branch appears in Burrow.
3. Validate with tree context (proto hierarchy), not flat row matching.
4. Roll up branch status:
   - `fully_attested`
   - `partially_attested`
   - `not_attested`

Proto-Dravidian (root) is context only; validation applies to DED-bearing
branches below it.

## Files In This Folder

### `burrow_corpus.json`

Original cached Burrow corpus scraped/parsing output.

Contains:
- entry metadata (`ded_number`, `edition`, `raw_html`, `full_text`)
- parsed `attestations` with `language_abbrev`, `headwords`, `gloss`

Note:
- This file can contain parser-collapse artifacts in gloss text
  (for example missing spaces around tokens).

### `burrow_corpus.cleaned.json`

Repaired corpus generated from `burrow_corpus.json` using `full_text` fallback.

Contains the same schema as `burrow_corpus.json`, but many attestation glosses
are normalized for better downstream matching/reporting.

Recommended corpus for validator runs:
- Use this file by default.

### `burrow_corpus_checkpoint.json`

Checkpoint metadata from corpus scraping workflow.

Used for resume/progress handling during scraping; not typically used directly
in validation reporting.

## How To Run Validation

From repository root:

```powershell
$env:PYTHONIOENCODING='utf-8'; python src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py data/dravidian/starling_complete_data.json --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json --output-dir tree_validation_output
```

Test mode (first N records):

```powershell
$env:PYTHONIOENCODING='utf-8'; python src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py data/dravidian/starling_complete_data.json --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json --output-dir tree_validation_output --test 2
```

## Validation Outputs

By default, outputs go to `tree_validation_output/`:

- `tree_validation_results.xlsx`
  - Primary row-level validation output.
- `tree_validation_results.csv`
  - CSV mirror of row-level validation output.
- `coverage_by_ded_paragraph.xlsx`
  - Per-DED coverage rollup (row and language overlap metrics).
- `validation_audit_report.xlsx`
  - Reviewer-oriented issue report with sheets:
    - `row_issues`
    - `meaning_mismatches`
    - `missing_starling_meaning`
    - `branch_rollup`
    - `ded_rollup`
- `tree_validation_summary.json`
  - High-level numeric summary.

