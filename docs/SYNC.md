# Cross-Machine Sync (Windows <-> macOS)

How this repo's working context moves between machines. Three tiers: what
travels in **git**, what travels over **Box**, and what is **rebuilt locally**.

The guiding rule comes from `data/dravidian/burrow_ded/index.md`:
**`burrow_corpus.json` is the single source of truth.** Everything downstream
(`burrow_corpus.cleaned.json`, every file in `tree_validation_output/`) is
reproducible from it plus tracked code. So only genuinely irreplaceable things
need to travel.

---

## Tier 1 — In git

Small, hand-curated, no script regenerates them. Force-tracked via negation
rules at the bottom of `.gitignore` (the blanket `*.json` / `*.csv` / `*.xlsx`
rules would otherwise swallow them).

| File | What it is |
|---|---|
| `data/dravidian/burrow_ded/review_ledger.json` | **168 triage entries / 214 reviewed keys.** Your accumulated per-DED decisions and notes. Nothing regenerates this. |
| `tree_validation_output/tree_validation_summary*.json` | Metric history — the audit trail behind every fix (~156 KB total). |
| `tree_validation_output/genuine_divergence_worksheet.csv` | Active divergence-logging worksheet. |
| `tree_validation_output/loop_worklist.json`, `loop_findings.md` | Review worklists. |
| `src/dravidian/scripts/.../unmatched_diagnostics.md` | Diagnostics driving the current matcher work. |
| `CLAUDE.md`, `.claude/skills/triage-ded/SKILL.md` | Project instructions + the triage skill. |
| `.claude/memory/*.md` | Claude's project memory (see "Memory" below). |

## Tier 2 — Over Box

Source-of-truth scrapes and current outputs. Too large for git, and they change
only on a re-scrape or a full revalidation.

| SHA-256 (first 16) | Size | Path |
|---|---:|---|
| `f09c59b666fad80b` | 16.1 MB | `data/dravidian/burrow_ded/burrow_corpus.json` |
| `6b6dc5dc6d17a5bb` | 17.9 MB | `data/dravidian/burrow_ded/burrow_corpus.cleaned.json` |
| `ebb8df002b5d8c2a` | 8.6 MB | `data/dravidian/starling/starling_complete_data_scrape.json` |
| `8ec996221f813086` | 3.5 MB | `data/dravidian/starling/output.xlsx` |
| `319fdf69fd7aa3ab` | 5.0 MB | `tree_validation_output/tree_validation_results.xlsx` |
| `f1baa61d6274e353` | 1.0 MB | `tree_validation_output/validation_audit_report.xlsx` |
| `4edd5960a8c2f0f5` | 0.1 MB | `tree_validation_output/coverage_by_ded_paragraph.xlsx` |
| `6981368bf786da9e` | 39.8 MB | `tree_validation_output/tree_validation_results.csv` |

Sizes/hashes are a point-in-time snapshot (2026-08-20). **Re-run the checksum
command below at upload time and compare after download** — a background job
rewrote `validation_audit_report.xlsx` mid-capture, so don't trust a stale hash.

```bash
# On either machine, from repo root:
sha256sum data/dravidian/burrow_ded/burrow_corpus.json \
          data/dravidian/burrow_ded/burrow_corpus.cleaned.json \
          data/dravidian/starling/starling_complete_data_scrape.json \
          data/dravidian/starling/output.xlsx
```

Strictly, only `burrow_corpus.json` + `starling_complete_data_scrape.json` +
`output.xlsx` are *required* — `burrow_corpus.cleaned.json` and the
`tree_validation_output/` reports are shipped so the Mac gets byte-parity
immediately instead of spending a reparse + full validation run to get there.

## Tier 3 — Rebuilt locally, never synced

`burrow_corpus.cleaned.json` (if not taken from Box), everything else in
`tree_validation_output/`, `__pycache__/`, `.mypy_cache/`, `lrc_env/`.

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

python3 -m venv lrc_env
./lrc_env/bin/pip install -r src/requirements.txt

# Drop the Tier 2 files from Box into these exact paths:
#   data/dravidian/burrow_ded/burrow_corpus.json
#   data/dravidian/burrow_ded/burrow_corpus.cleaned.json
#   data/dravidian/starling/starling_complete_data_scrape.json
#   data/dravidian/starling/output.xlsx
#   tree_validation_output/*.xlsx
```

Note the Mac venv layout is `lrc_env/bin/python`, not `lrc_env/Scripts/python.exe`.
`CLAUDE.md` documents the Windows path; use the `bin` form on macOS.

### Rebuild the derived corpus (only if you skipped it on Box)

```bash
./lrc_env/bin/python src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py \
    data/dravidian/burrow_ded/burrow_corpus.json \
    --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json
```

### Revalidate (confirms parity with this machine)

```bash
PYTHONIOENCODING=utf-8 ./lrc_env/bin/python \
  src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py \
  data/dravidian/starling/starling_complete_data_scrape.json \
  --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json \
  --output-dir tree_validation_output
```

Expected headline (2026-08-20): **entries_matched 19,075, match rate 98.5%**,
Language-only 144, No 135. Compare against
`tree_validation_output/tree_validation_summary.json`, which is in git.

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
time; the committed ledger holds 174 entries / 145 `genuine_divergence`).

---

## Language tree source

`build_dravidilex_import.py` reads a language-tree workbook to build
`dravidilex_languages.csv` (`build_languages_csv()`, called at line 394):

```python
TREE_XLSX = DATA_DIR / "three-tier-language tree.xlsx"   # line 44
```

That Krishnamurti-2003 workbook is **superseded**. A newer tree lives on the Mac
and is already on Box; the old three-tier file is being retired. It was never
tracked in git (`git log --all --diff-filter=ADR -- "*three-tier*"` is empty) and
is not on the Windows machine, so nothing here needs deleting — only the
`TREE_XLSX` constant needs repointing at the replacement.

Two things to check when repointing:

- **Path/filename** — if the replacement is dropped in at the same path under a
  different name, update line 44 (and the header comment at line 8).
- **Sheet layout** — `build_languages_csv()` does `wb.active` and reads
  Family/Subfamily/Language positionally. If the newer workbook has a different
  active sheet or column order, the path change alone is not enough.

`data/dravidian/lrc_import/index.md` also describes the old tree (lines 5, 21)
and should be updated to name the replacement.
