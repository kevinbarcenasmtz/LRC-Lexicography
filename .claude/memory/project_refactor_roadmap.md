---
name: project-refactor-roadmap
description: "Cross-validation scripts refactor — COMPLETE (4 commits, each validated byte-identical against the 2026-08-16 baseline)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 337aac4a-653e-47a2-a8ac-8b8314dee111
  modified: 2026-08-16T06:18:00.806Z
---

Refactor of src/dravidian/scripts/cross-validating-dded-starling, completed 2026-08-16.
Every commit validated by full-pipeline re-run + byte-compare of
tree_validation_results.csv and summary against the 2026-08-16 00:30 baseline
(87.1% / 17,080 matched) — all identical.

Commits: 484cf6c (shared textnorm.py/editions.py; deleted dead
burrow_enhanced_validation.py, burrow_language_mappings.py shim,
_validate_branch_direct), 136f1f9 (MatchOutcome dataclass; load_burrow_corpus
reports malformed attestations to stderr instead of silent drop), f526ad6
(split into validation_models.py / gloss_extraction.py / reporting.py;
validator now ~1,035 lines; triage imports build_validation_audit_frames from
reporting), fe18280 (DED-cleaner unification).

**Two-layer DED-number cleaning is now deliberate architecture** (Kevin's ruling
2026-08-16): `textnorm.clean_ded_number` is the validation/ledger-layer cleaner
(folds "4896(a)"→"4896", "0"→None sentinel) used by validator + review_ledger +
triage_mismatches; `BurrowEntryParser.clean_ded_number` is the scrape/corpus
layer and deliberately keeps "4896(a)"/"4896(b)" distinct so split entries stay
separate corpus entries (inspect_ded_entry depends on this). review_ledger's
wrapper falls back to the stripped literal on None so the archival "0" ledger
entry stays reachable. All 22 ledger keys unchanged under the new cleaner.

Validation recipe: run validator on **data/dravidian/starling/starling_complete_data.json**
+ data/dravidian/burrow_ded/burrow_corpus.cleaned.json, `cmp` CSV against
data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.csv. See [[project-triage-ded-ledger-status]].

**Input repointed 2026-09-05:** this note previously said `starling_complete_data_scrape.json`
— a superseded scrape, no longer on disk. The current inputs are `starling_complete_data.json`
and the markup superset `starling_complete_data_markup.json`. Every reference has been repointed
(validator docstring, `/triage-ded` skill, docs, vault).

**Baseline note:** the 87.1% / 17,080 figure above was the 2026-08-16 refactor baseline and is
long superseded — the pilot froze at **98.6% / 19,083** on 2026-08-21.
