# Context Of This Directory

Reference for the outputs produced by:

`src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py`

They are generated from Starling data cross-validated against Burrow DED paragraph attestations.

> ⚠ **The files themselves are not here.** This directory holds only this index. The validator
> writes to **`data/dravidian/cross-validating-dded-starling/tree_validation_output/` at the repository root** (its `--output-dir` default), and
> that is where every artifact described below actually lives.

## Current state (pilot frozen 2026-08-21)

```
total_language_entries   19,354   (in scope; 653 subgroup-DB orphans excluded)
entries_matched          19,083   98.6%
  Language only             136   logged genuine_divergence — AI-triaged, unreviewed
  No                        135
unique_branches           7,948   fully_attested 7,740 · not_attested 44 · ded_not_in_corpus 10
```

**Do not quote 98.6% without its denominator caveat** — against the full 20,007-row scrape it is
95.4%. See `docs/dravidian_validator_progress.md` §11.

> ⚠ **Four of the reports below are stale.** `tree_validation_results.xlsx`/`.csv`,
> `validation_audit_report.xlsx` and `coverage_by_ded_paragraph.xlsx` are **2026-07-07** builds —
> they predate the entire 94.0% → 98.6% tail (§10) and disagree with the summary. Only
> `tree_validation_summary.json` and the three review artifacts are current (2026-08-21).
> Re-run the validator before using any row-level report.

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

### Review artifacts (git-tracked, current as of 2026-08-21)

These four are force-tracked in git — small, hand-curated, and not reproducible from a re-run.

| File | What it is |
|---|---|
| `tree_validation_summary.json` | Headline metrics. **The only current numeric source of truth.** |
| `genuine_divergence_worksheet.csv` | The adjudicated residual, 6 categories. ⚠ **One revision stale**: it lists 144 rows, but `c5f78ff` then recovered 8 of its 11 `display_fallback_or_postposed` rows. Live residual is **136**. Regenerate before using as a queue. |
| `loop_findings.md` | Per-language diagnostics for unmatched rows — dominant verdict + proposed fix per language group. 19 of 52 groups worked. |
| `loop_worklist.json` | Tracks those groups: **19 `done` (415 rows), 33 `pending` (170 rows)**. The resumable part of the work. |

## Recommended Review Flow

1. Start with `tree_validation_summary.json` for the headline metrics — it is the only current one.
2. For the unmatched tail, read `loop_findings.md` (diagnosis + proposed fix per language) and pick
   up `loop_worklist.json`'s 33 `pending` groups.
3. For the adjudicated residual, use `genuine_divergence_worksheet.csv` — after regenerating it.
4. **Re-run the validator** before relying on `validation_audit_report.xlsx`,
   `tree_validation_results.xlsx` or `coverage_by_ded_paragraph.xlsx`; the copies on disk are
   from 2026-07-07.
