# Cross-Machine Sync (Windows <-> macOS)

How this repo's working context moves between machines. Three tiers: what
travels in **git**, what travels over **Box**, and what is **rebuilt locally**.

The guiding rule comes from `data/dravidian/burrow_ded/index.md`:
**`burrow_corpus.json` is the single source of truth.** Everything downstream
(`burrow_corpus.cleaned.json`, every file in `data/dravidian/cross-validating-dded-starling/tree_validation_output/`) is
reproducible from it plus tracked code. So only genuinely irreplaceable things
need to travel.

---

## Tier 1 — In git

Small, hand-curated, no script regenerates them. Force-tracked via negation
rules at the bottom of `.gitignore` (the blanket `*.json` / `*.csv` / `*.xlsx`
rules would otherwise swallow them).

| File | What it is |
|---|---|
| `data/dravidian/burrow_ded/review_ledger.json` | **174 DED entries; 147 language-level records, of which 145 are `genuine_divergence`.** Accumulated per-DED decisions and notes. Nothing regenerates this. ⚠ 135 of the 147 language-level records are `reviewed_by: claude-triage` — AI-classified, not human-reviewed. |
| `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_summary*.json` | Metric history — the audit trail behind every fix (~156 KB total). |
| `data/dravidian/cross-validating-dded-starling/tree_validation_output/genuine_divergence_worksheet.csv` | Active divergence-logging worksheet. |
| `data/dravidian/cross-validating-dded-starling/tree_validation_output/loop_worklist.json`, `loop_findings.md` | Review worklists. |
| `src/dravidian/scripts/.../unmatched_diagnostics.md` | Diagnostics driving the current matcher work. |
| `CLAUDE.md`, `.claude/skills/triage-ded/SKILL.md` | Project instructions + the triage skill. |
| `.claude/memory/*.md` | Claude's project memory (see "Memory" below). |

## Tier 2 — Over Box

Source-of-truth scrapes and current outputs. Too large for git, and they change
only on a re-scrape or a full revalidation.

| SHA-256 (first 16) | Size | Path | Captured |
|---|---:|---|---|
| `f09c59b666fad80b` | 16.1 MB | `data/dravidian/burrow_ded/burrow_corpus.json` | 2026-09-05 (unchanged since 08-20) |
| `6b6dc5dc6d17a5bb` | 17.9 MB | `data/dravidian/burrow_ded/burrow_corpus.cleaned.json` | 2026-09-05 (unchanged since 08-20) |
| `472c959d1fe3adf8` | 6.9 MB | `data/dravidian/starling/starling_complete_data.json` | 2026-09-05 |
| `f3df8ae54effb769` | 8.5 MB | `data/dravidian/starling/starling_complete_data_markup.json` | 2026-09-05 |
| `6aabc8c835cd2a9b` | 2.4 MB | `data/dravidian/starling/output.xlsx` | 2026-09-05 |
| `a686023c3ab10b1b` | 71.9 MB | `data/dravidian/lrc_import/dravidilex_batch_import.json` | 2026-09-05 |
| `9dfeaefabdafb83d` | 4.8 MB | `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.xlsx` | ⚠ **stale — 2026-07-07 build** |
| `b1a7fe84a3f61241` | 1.1 MB | `data/dravidian/cross-validating-dded-starling/tree_validation_output/validation_audit_report.xlsx` | ⚠ **stale — 2026-07-07 build** |
| `9b145a7d94a1c058` | 0.1 MB | `data/dravidian/cross-validating-dded-starling/tree_validation_output/coverage_by_ded_paragraph.xlsx` | ⚠ **stale — 2026-07-07 build** |
| `ef2f120e9a720b08` | 40.5 MB | `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.csv` | ⚠ **stale — 2026-07-07 build** |

> **Superseded input (2026-09-05).** Earlier revisions of this file, the vault's progress report
> §6, and the validator's own docstring referred to `starling_complete_data_scrape.json`. That was
> a real scrape, now **superseded and no longer on disk**. The current inputs are
> `data/dravidian/starling/starling_complete_data.json` and the markup-preserving superset
> `starling_complete_data_markup.json` from the 2026-08-08 re-scrape. Both were last rewritten
> 2026-08-22 by the scraper colon-artifact fix (`08e14ae`). All references have been repointed,
> including `starling_tree_validator.py`'s docstring and the `/triage-ded` skill.
>
> **The four `data/dravidian/cross-validating-dded-starling/tree_validation_output/` reports are stale.** They are 2026-07-07 builds and predate
> the entire 94.0% → 98.6% tail. Only `tree_validation_summary.json`, `loop_findings.md`,
> `loop_worklist.json` and `genuine_divergence_worksheet.csv` (all git-tracked) are current
> (2026-08-21). Re-run the validator before shipping any row-level report.

Sizes/hashes captured **2026-09-05**. **Re-run the checksum command below at
upload time and compare after download** — a background job once rewrote
`validation_audit_report.xlsx` mid-capture, so don't trust a stale hash.

```bash
# On either machine, from repo root:
# macOS: shasum -a 256    Linux/Git-Bash: sha256sum
shasum -a 256 data/dravidian/burrow_ded/burrow_corpus.json \
              data/dravidian/burrow_ded/burrow_corpus.cleaned.json \
              data/dravidian/starling/starling_complete_data.json \
              data/dravidian/starling/starling_complete_data_markup.json \
              data/dravidian/starling/output.xlsx
```

Strictly, only `burrow_corpus.json` + `starling_complete_data.json` +
`output.xlsx` are *required* — `burrow_corpus.cleaned.json` and the
`data/dravidian/cross-validating-dded-starling/tree_validation_output/` reports are shipped so the Mac gets byte-parity
immediately instead of spending a reparse + full validation run to get there.

## Tier 3 — Rebuilt locally, never synced

`burrow_corpus.cleaned.json` (if not taken from Box), everything else in
`data/dravidian/cross-validating-dded-starling/tree_validation_output/`, `__pycache__/`, `.mypy_cache/`, `lrc_venv/`.

`.gitignore` now also hard-blocks the snapshot patterns that used to pile up
(`results.*.csv`, `tree_validation_results.*.csv`,
`burrow_corpus.cleaned.*.json`) — 856 MB of those were cleared on 2026-08-20.
Keep using the `.before-x` naming for local A/B runs; it stays out of git.

---

## Mac bootstrap

```bash
git clone https://github.com/kevinbarcenasmtz/LRC-Lexicography.git
cd LRC-Lexicography
git checkout dravidilex-pilot

python3 -m venv lrc_venv
./lrc_venv/bin/pip install -r src/requirements.txt

# Drop the Tier 2 files from Box into these exact paths:
#   data/dravidian/burrow_ded/burrow_corpus.json
#   data/dravidian/burrow_ded/burrow_corpus.cleaned.json
#   data/dravidian/starling/starling_complete_data.json
#   data/dravidian/starling/starling_complete_data_markup.json
#   data/dravidian/starling/output.xlsx
#   data/dravidian/cross-validating-dded-starling/tree_validation_output/*.xlsx
```

**The venv on this Mac is `lrc_venv/`, not `lrc_env/`** — rebuilt 2026-08-08 on Python 3.12 from
`src/requirements.txt` (the pre-move one pointed at `~/Projects/LRC-Lexicography` and lacked the
scraping deps). Layout is `lrc_venv/bin/python`, not `lrc_venv/Scripts/python.exe`; `CLAUDE.md`
documents the Windows path, so use the `bin` form on macOS.

### Rebuild the derived corpus (only if you skipped it on Box)

```bash
./lrc_venv/bin/python src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py \
    data/dravidian/burrow_ded/burrow_corpus.json \
    --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json
```

### Revalidate (confirms parity with this machine)

```bash
PYTHONIOENCODING=utf-8 ./lrc_venv/bin/python \
  src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py \
  data/dravidian/starling/starling_complete_data.json \
  --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json \
  --output-dir data/dravidian/cross-validating-dded-starling/tree_validation_output
```

Expected headline (**2026-08-21, pilot-frozen**): **entries_matched 19,083, match rate 98.6%**
of 19,354 in-scope entries — **Language only 136, No 135**. Compare against
`data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_summary.json`, which is in git.

The 19,354 denominator excludes **653** non-DED-keyed subgroup-database orphans (`ad5da93`);
against the full 20,007-row scrape the same matches are 95.4%. The 136 `Language only` rows are
adjudicated `genuine_divergence`, **not** matches — see `docs/dravidian_validator_progress.md`
§10–§11 before quoting any of these numbers.

### Memory

Claude's project memory lives outside the repo, at a machine-specific path
(`~/.claude/projects/<slug>/memory/`), and the slug differs per machine. The
canonical copies are versioned here at `.claude/memory/`. On the Mac:

```bash
mkdir -p ~/.claude/projects/-Users-<you>-LRC-Lexicography/memory
cp .claude/memory/*.md ~/.claude/projects/-Users-<you>-LRC-Lexicography/memory/
```

When memory changes meaningfully on either machine, copy it back into
`.claude/memory/` and commit, so the two stay reconciled.

---

## Line endings

`core.autocrlf=true` here vs LF on macOS would make every ledger/summary write
look like a whole-file diff. Two guards are in place:

- `.gitattributes` pins the tracked data files to `eol=lf`.
- `review_ledger.py::save_ledger` now writes with `newline="\n"` so both
  machines emit byte-identical JSON.

The existing ledger was renormalized to LF (content verified identical at the
time). The committed ledger holds **174 DED entries / 147 language-level records /
145 `genuine_divergence`** — re-verified 2026-09-05.

---

## Language tree source — RESOLVED 2026-08-22

~~`build_dravidilex_import.py` reads `data/dravidian/three-tier-language tree.xlsx`
(`TREE_XLSX`, line 44) to build `dravidilex_languages.csv`; that Krishnamurti-2003 workbook is
superseded and the constant needs repointing.~~

**Closed.** `data/dravidian/lrc_import/dravidilex_languages.csv` is now the **tracked source of
truth** for the language tier, and the same 38 rows that were imported to lrc-test
(`7b20411`, `bc4b427`, `d9fbef0`). The old three-tier workbook is only a fallback if that CSV is
absent, and it was never tracked in git — nothing needs deleting or migrating.

The CSV is both a tracked *input* and a rewritten normalized *output* of the build script, so it
travels in Tier 1 and needs no Box hop. `data/dravidian/lrc_import/index.md` documents the
38-language layout and the mapping decisions behind it.
