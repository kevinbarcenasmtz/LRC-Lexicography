---
name: triage-ded
description: Guided session for triaging Burrow DED vs StarlingDB mismatch rows in src/dravidian/scripts/cross-validating-dded-starling. Use when the user wants to review validation mismatches, hunt for parser bugs, or work through the review queue for this project.
---

# Triage DED <-> Starling mismatches

This is a **guided session, not an autonomous batch loop**. The user stays in
control of every fix and every ledger entry. Run exactly **one pass at a
time** through the steps below, and stop to check in with the user at every
numbered decision point. Never silently chain multiple passes together.

All commands below assume the repository root as the working directory and
use the project virtual environment's interpreter (see the repo's
`CLAUDE.md`): `lrc_env\Scripts\python.exe` (PowerShell/cmd) or
`./lrc_env/Scripts/python.exe` (Git Bash) -- not a bare `python`.

Paths used throughout:
- Results: `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.csv`
- Corpus: `data/dravidian/burrow_ded/burrow_corpus.cleaned.json`
- Ledger: `data/dravidian/burrow_ded/review_ledger.json`
- Scripts: `src/dravidian/scripts/cross-validating-dded-starling/{triage_mismatches,inspect_ded_entry,review_ledger}.py`

## Ad-hoc inspection gotchas

If you write one-off `python -c` snippets to inspect the corpus or print
Burrow data directly (rather than using the four scripts above), three
Windows/encoding gotchas apply that the scripts already handle internally
but your snippet will not:

- **BOM**: `burrow_corpus.json` and `burrow_corpus.cleaned.json` are written
  with `encoding="utf-8-sig"`. Open them the same way
  (`open(path, encoding="utf-8-sig")`), or a plain `utf-8` open raises
  `UnicodeDecodeError: Unexpected UTF-8 BOM`.
- **Top-level shape**: the corpus JSON is not a list of entries. It is
  `{"metadata": ..., "entries": [...], "_repair_meta": ..., "_reparse_meta": ...}`.
  Iterate `data["entries"]`, not `data`.
- **Console encoding**: Burrow language abbreviations contain diacritics
  (ḍ, ḷ, ṅ, ṭ, etc.). Windows' default console codepage (cp1252) cannot
  print them, raising `UnicodeEncodeError`. Before printing corpus text in
  an ad-hoc snippet, either wrap stdout
  (`io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")`) or run the
  snippet with `PYTHONIOENCODING=utf-8` set. The five scripts in this
  directory already do this internally for their own output.

## The 10-step loop

1. **Orient.** Run:
   ```
   lrc_env\Scripts\python.exe src/dravidian/scripts/cross-validating-dded-starling/triage_mismatches.py data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.csv --primary ded_language --exclude-reviewed --ledger data/dravidian/burrow_ded/review_ledger.json --output data/dravidian/cross-validating-dded-starling/tree_validation_output/triage_queue.csv
   ```
   Read back only a handful of top rows to the user, not the whole file.
   Call out the shape: one DED# with several different languages failing
   suggests a local/HTML-specific bug confined to that paragraph; one
   language repeated across many DED#s suggests a systemic bug (e.g. the
   historical Gondi inline-sigil bug, which fired 2-5 rows each across 80+
   DED numbers -- unremarkable row-by-row, huge in aggregate).

2. **Ask which item to drill into.** Default suggestion: the top-ranked row.
   Do not assume -- the user may want to chase a different lead.

3. **Drill in.** Run:
   ```
   lrc_env\Scripts\python.exe src/dravidian/scripts/cross-validating-dded-starling/inspect_ded_entry.py <ded#> --reparse
   ```
   Summarize the comparison between cached ground truth, currently-stored
   attestations, and the reparse preview in plain language. State an actual
   diagnosis -- "parser bug" or "genuine divergence" -- with the specific
   evidence (e.g. "Ko. aṛy is in full_text but absent from stored
   attestations and from the reparse preview, so the parser is still
   missing it"), not just "looks like a bug."

4. **If it's a parser bug:** propose the specific code change -- file,
   function, exact diff -- and wait for explicit user approval before
   editing `burrow_entry_parser.py` (`_PATTERNS`, `_is_valid_lang`,
   `_INVALID_LANG_ABBREVS`) or `dialect_mapping.py` (`GONDI_INLINE_ABBREVS`,
   `match_languages`). Do not batch multiple unrelated fixes into one
   approval -- one diagnosis, one proposed fix, one approval.

5. **If it's a genuine divergence:** state the spelling/diacritic/gloss
   difference plainly and ask whether to log it as `genuine_divergence` in
   the ledger. This is a real finding worth keeping for the thesis, not
   something to discard.

6. **After a fix is approved and applied:** first figure out which side the
   fix touches, because that decides whether the corpus even needs
   regenerating:
   - **Parser-side fix** (extraction: `_PATTERNS`, `_is_valid_lang`,
     `_INVALID_LANG_ABBREVS`, sub-entry handling, etc.) changes what gets
     parsed out of the HTML. Re-check locally first with
     `inspect_ded_entry.py <ded#> --reparse` -- it exercises the parser, so
     the reparse preview should now show the recovered attestation. The
     corpus regen below **is** needed.
   - **Matcher-side fix** (`normalize_for_match` in `textnorm.py`,
     `match_languages`, the
     match logic in `_match_entry`) changes only how Starling rows are
     compared against already-parsed attestations. `inspect_ded_entry.py
     --reparse` will **not** reflect it (it only runs the parser), and corpus
     regeneration is a **zero-diff -- skip it entirely**. Verify instead by
     calling the patched function directly (or a small standalone sim) and go
     straight to the full validator run. Note `normalize_for_match` (and the
     other shared normalizers) now live in a **single source of truth**,
     `textnorm.py`, imported by `starling_tree_validator.py`,
     `burrow_entry_parser.py`, and `repair_burrow_corpus_glosses.py` -- so one
     edit there governs both sides; there are no longer two copies to keep in
     sync. (A matcher-side fold applied only in `normalize_for_match` is
     genuinely zero-diff for the corpus even though the parser imports it,
     since it is applied at match time, not baked into stored attestations --
     confirmed e.g. by the 2026-08-18 ẓ/r̤ fold.)

   Then ask before each of the next actions, separately (skip the corpus
   regen action for a matcher-side fix):
   - Regenerating the full corpus (no network call, but rewrites the
     ~5,700-entry cleaned corpus file). **Copy the current
     `burrow_corpus.cleaned.json` aside first** (it's overwritten in
     place, same hazard as the validation summary below):
     ```
     lrc_env\Scripts\python.exe src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py data/dravidian/burrow_ded/burrow_corpus.json --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json
     ```
     Check the printed diff (entries changed, gained/lost per DED#). Any
     **losses** are a regression signal -- stop and investigate with the
     user before continuing, don't push ahead into re-validation.

     **The printed diff is cumulative since the raw scrape, not
     session-over-session.** It always compares against
     `entry["attestations"]` in the *input* `burrow_corpus.json` (the raw,
     unparsed baseline) -- never against the previously-shipped
     `burrow_corpus.cleaned.json`. After multiple fixes have already
     shipped, the printed count conflates every historical fix's delta,
     not just the newest one. To learn one fix's true incremental effect:
     keep the pre-regen backup you just made, regenerate, then diff the
     two cleaned-corpus files' per-entry attestation language-abbreviation
     sets yourself. Don't trust the printed number alone once more than
     one fix is already in the corpus's history.
   - Running the full validator. Flag the cost up front (a few minutes over
     2,211 Starling records) before kicking it off, and copy the prior
     `tree_validation_summary.json` first since the run overwrites it in
     place:
     ```
     lrc_env\Scripts\python.exe src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py data/dravidian/starling/starling_complete_data.json --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json --output-dir data/dravidian/cross-validating-dded-starling/tree_validation_output
     ```

7. **Report the before/after `entry_match_rate`** from
   `tree_validation_summary.json` once re-validation finishes.

8. **Record the outcome.** Draft the note text and show it to the user --
   never log silently. Once confirmed:
   ```
   lrc_env\Scripts\python.exe src/dravidian/scripts/cross-validating-dded-starling/review_ledger.py record --ded <ded#> --status parser_bug_fixed --note "<confirmed note>" --ledger data/dravidian/burrow_ded/review_ledger.json
   ```
   (or `--status genuine_divergence` / `needs_more_info` / `not_a_bug_wontfix`,
   with `--language "<Starling language>"` added for a language-scoped
   decision rather than a whole-DED one).

   For a **systemic, cross-cutting fix** that resolves many DED#s at once
   (e.g. a normalization change), the per-DED key doesn't fit -- record one
   entry under a descriptive synthetic key instead (e.g.
   `--ded "underscore-normalization"`). `_clean_ded` falls back to the raw
   string for non-numeric input, so this is safe; matched rows leave the
   queue by matching, not by ledger suppression, so the entry is pure
   documentation of the finding for the thesis.

   Notes containing diacritics are fine to pass as-is via `--note`. If you
   suspect the shell mangled non-ASCII characters in transit, read the
   ledger entry back immediately after recording (`review_ledger.py` already
   prints/reads UTF-8 safely) to confirm fidelity, rather than pre-emptively
   stripping diacritics from the note text.

9. **Commit and push the code fix** -- only after the fix is verified and the
   ledger entry is written. Ask the user first unless they've already said to
   commit. Stage only the changed **source files**; the ledger
   (`review_ledger.json`) and everything under `data/dravidian/cross-validating-dded-starling/tree_validation_output/` are
   gitignored data artifacts and are not committed. Write a descriptive
   message that states the before/after `entry_match_rate` and row delta.
   Follow the user's standing preference on co-author trailers (they have
   asked to omit the `Co-Authored-By` line on this project).

10. **Ask whether to continue** to the next queue item or stop here for this
    session. Treat "stop" as the default if the user doesn't clearly want to
    continue.

## Explicitly do NOT

- Fix multiple DED numbers in a row without checking in between each one.
- Run the full validator more than once without telling the user it's about
  to happen and roughly how long it takes.
- Write anything to the ledger without the user seeing the proposed note
  text first.
- Treat this as a batch job -- one pass, one check-in cycle, then stop and
  ask before looping again.
