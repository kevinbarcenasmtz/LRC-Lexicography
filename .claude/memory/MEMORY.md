# Memory Index

- [Triage-ded ledger status](project_triage_ded_ledger_status.md) — **FINAL pilot state 2026-08-21: 98.6%, 19,100/19,371 matched, Language only 136, No 135.** Residual is adjudicated `genuine_divergence`, AI-triaged and unreviewed — never fold it into a published rate.
- [Burrow parser — current status](project_burrow_parser_status.md) — parser fixes through v16 (`766011a`); corpus in sync. One open shape: the To.-type running-text TEXT variant. Kui bare sense-suffix forms and the DED 410/4896 duplicate entries remain.
- [Refactor roadmap — cross-validation scripts](project_refactor_roadmap.md) — COMPLETE: 4 commits (`484cf6c`…`fe18280`), each byte-identical vs baseline; two-layer DED cleaning is deliberate (textnorm = validator/ledger, parser = scrape/corpus).

## Where the authoritative numbers live

`data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_summary.json` (git-tracked) is the only current metric
source. `docs/dravidian_validator_progress.md` §10–§11 carries the full 94.0% → 98.6% tail, the
denominator caveat (654 subgroup-DB orphans excluded; 95.4% against the full 20,025-row scrape),
and the citation guidance for the AsiaLex paper. Four reports in `data/dravidian/cross-validating-dded-starling/tree_validation_output/` are
stale 2026-07-07 builds — re-run the validator before using any row-level output.
