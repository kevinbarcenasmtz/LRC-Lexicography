# Context Of This Directory

This directory contains outputs produced by:

`src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py`

These files are generated from Starling data cross-validated against Burrow DED
paragraph attestations.

## Files

### `tree_validation_results.xlsx`

Primary row-level validation report

Each row is one Starling language entry under a DED-bearing branch, with:
- branch/proto context
- Starling form + meaning
- matched Burrow segment scope/form/meaning
- match confidence + notes
- branch rollup status (`fully_attested`, `partially_attested`, etc.)


### `coverage_by_ded_paragraph.xlsx` (Not manually checked right now)

Per-DED coverage rollup.

Includes:
- row-level counts (`matched`, `language-only`, `unmatched`)
- row match rate
- language overlap summary (Starling vs Burrow)

### `validation_audit_report.xlsx` (Better representation of 'errors')

Reviewer-focused issue workbook derived from `tree_validation_results`.

Sheets:
- `row_issues`
- `meaning_mismatches`
- `missing_starling_meaning`
- `branch_rollup`
- `ded_rollup`

Use this first when auditing regressions and mismatches.

### `tree_validation_summary.json`

Compact machine-readable summary:
- total rows
- match rate
- branch status breakdown
- confidence distribution

## Recommended Review Flow

1. Start with `validation_audit_report.xlsx`:
   - check `row_issues` and `meaning_mismatches`.
2. Use `tree_validation_results.xlsx` for row-level context and Burrow segment details.
3. Use `coverage_by_ded_paragraph.xlsx` for DED-level gaps/asymmetry.
4. Use `tree_validation_summary.json` for quick headline metrics.
