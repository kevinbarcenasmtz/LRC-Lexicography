# Context Of This Directory

Reference for the outputs produced by:

`src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py`

They are generated from Starling data cross-validated against Burrow DED paragraph attestations.

> ⚠ **The files themselves are not here.** This directory holds only this index. The validator
> writes to **`data/dravidian/cross-validating-dded-starling/tree_validation_output/` at the repository root** (its `--output-dir` default), and
> that is where every artifact described below actually lives.

## Current state (pilot frozen 2026-08-21)

```
total_language_entries   19,371   (in scope; 654 subgroup-DB orphans excluded)
entries_matched          19,100   98.6%
  Language only             136   logged genuine_divergence — AI-triaged, unreviewed
  No                        135
unique_branches           7,948   fully_attested 7,740 · not_attested 44 · ded_not_in_corpus 10
```

**Do not quote 98.6% without its denominator caveat** — against the full 20,025-row scrape it is
95.4%. See `docs/dravidian_validator_progress.md` §11.

> ✅ **All reports regenerated 2026-09-05** by a full validator re-run. The four that had been
> stuck at their 2026-07-07 build — `tree_validation_results.{csv,xlsx}`,
> `validation_audit_report.xlsx`, `coverage_by_ded_paragraph.xlsx` — are current and agree with the
> summary. `genuine_divergence_worksheet.csv` was regenerated to the live 136 rows.
> `triage_queue.csv` is still 2026-06-23: it comes from `triage_mismatches.py`, not the validator,
> and only needs rebuilding when triage resumes.

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
| `genuine_divergence_worksheet.csv` | The adjudicated residual — **136 rows, regenerated 2026-09-05**: `partial_shared_root` 52, `distinct_reflex` 39, `close_spelling_vowel_consonant` 24, `morphological_citation` 11, `display_fallback_or_postposed` 7, `inscr_telugu_r_d_corresp` 3. ⚠ AI-classified; see the open TODO below. |
| `loop_findings.md` | Per-language diagnostics for unmatched rows — dominant verdict + proposed fix per language group. 19 of 52 groups worked. |
| `loop_worklist.json` | Tracks those groups: **19 `done` (415 rows), 33 `pending` (170 rows)**. The resumable part of the work. |

## Recommended Review Flow

1. Start with `tree_validation_summary.json` for the headline metrics — it is the only current one.
2. For the unmatched tail, read `loop_findings.md` (diagnosis + proposed fix per language) and pick
   up `loop_worklist.json`'s 33 `pending` groups.
3. For the adjudicated residual, use `genuine_divergence_worksheet.csv` — after regenerating it.
4. `validation_audit_report.xlsx`, `tree_validation_results.xlsx` and
   `coverage_by_ded_paragraph.xlsx` are current as of 2026-09-05 — use them directly.

## Open TODOs

- **Human spot-check of the `genuine_divergence` set.** 135 of the ledger's 147 language-level
  records are `reviewed_by: claude-triage`. DED 1818 Muria `gumiya` was labelled `distinct_reflex`
  and later matched by `c5f78ff`, so at least one classification in 144 was wrong. Until a
  Dravidianist samples across the 6 categories, describe the residual as *classified*, never
  *verified*.
- **Scrape CVOTGD.** All 662 CVOTGD source rows carry a `page_number` and an empty
  `original_entry` — the one remaining data gap in the import payload.
- **Finish `loop_worklist.json`** — 33 of 52 language groups still `pending` (170 of 585 rows).
