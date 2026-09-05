---
name: triage-ded-ledger-status
description: "DED<->Starling triage ledger — FINAL pilot state 2026-08-21: entry_match_rate 98.6%, entries_matched 19,083/19,354, Language only 136, No 135; residual adjudicated genuine_divergence (AI-triaged, unreviewed)"
metadata: 
  node_type: memory
  type: project
  originSessionId: d56938e8-f7fe-4bc9-8b02-ed5ad986d55a
  modified: 2026-09-05T00:00:00.000Z
---

## FINAL STATE — pilot frozen 2026-08-21

```
total_language_entries   19,354   (in scope; 653 subgroup-DB orphans excluded, ad5da93)
entries_matched          19,083   98.6%
  Language only             136   logged genuine_divergence
  No                        135
```

Source of truth: `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_summary.json` (git-tracked). The last fix
of the pilot was **`c5f78ff`** (postposed dialect markers, matcher-side/zero-regen):
19,075 → **19,083** (+8, all Language-only → Yes), **Language only 144 → 136**, No frozen 135.

**Three corrections to the older notes below, which were written mid-tail:**

1. **The rate is 98.6%, not 98.5%**, and matched is **19,083, not 19,075** — the session log below
   stops one commit short of `c5f78ff`.
2. **Language-only is 136, not 144.** `c5f78ff` recovered 8 of the 11 rows that had been
   *"HELD FOR REVIEW"* as `display_fallback_or_postposed` — they were postposed-marker cases, not
   display bugs. `genuine_divergence_worksheet.csv` still lists 144 and is one revision stale.
3. **The residual was adjudicated, never "accepted as matches."** The ledger note stamped on the
   set reads *"real transcription/dialect variation, not a matcher normalization gap."* Ledger now
   holds **174 DED entries / 147 language-level records / 145 `genuine_divergence`** — and
   **135 of the 147 are `reviewed_by: claude-triage`**, i.e. AI-classified with no human
   spot-check. They are not in the 19,083 and must not be folded into a published rate.
   Of the 144 worksheet rows, 40 are labelled `distinct_reflex` — genuinely different words.

Full narrative, denominator caveats, and citation guidance: `docs/dravidian_validator_progress.md`
§10–§11.

**Open, in value order:** human spot-check of the `genuine_divergence` sample · regenerate the
worksheet and the four stale 2026-07-07 reports in `data/dravidian/cross-validating-dded-starling/tree_validation_output/` · finish
`loop_worklist.json` (33 of 52 language groups still `pending`, 170 rows) · Box re-sync of the
v12–v16 corpus regens.

---

## Infrastructure (new as of 2026-06-18)

`src/dravidian/scripts/cross-validating-dded-starling/` gained three scripts backing the
`/triage-ded` skill: `triage_mismatches.py` (ranks mismatch rows from
`tree_validation_results.csv`), `inspect_ded_entry.py` (cached-vs-reparsed ground truth
for one Burrow DED entry), `review_ledger.py` (cross-session decision record at
`data/dravidian/burrow_ded/review_ledger.json`, keyed by DED# with optional per-language
sub-keys). See [[project_burrow_parser_status]] for the separate Burrow-parser-side
open issue (Pattern E false positives) — that track is independent of this one.

## Ledger entries recorded so far

- **DED 0** — `parser_bug_fixed`. Not a Burrow parser bug — a `starling_tree_validator.py`
  bug. Starling's own source JSON uses a literal `"Number in DED": "0"` as its sentinel
  for "no Burrow correspondence" (9 occurrences in `starling_complete_data.json`).
  `_clean_ded_number()` treated `"0"` as a truthy real DED number, so the validator tried
  `burrow_by_ded.get("0")` (Burrow's DED numbering starts at 1, never 0) and emitted false
  "DED 0 not found in Burrow corpus" mismatches. Fixed by normalizing `"0"` -> `None` in
  `_clean_ded_number` (single point of normalization, used by `_parse_node`). Re-validated:
  `entry_match_rate` 69.9% -> 70.0%, branch `ded_not_in_corpus` 26->17, `no_ded_number` 27->36.

**Why this matters:** the ledger was empty before this session, so a fresh
`triage_mismatches.py --exclude-reviewed` run still surfaces everything. Future sessions
should expect DED 0 to now be excluded from the queue.

- **DED 990, 137, 2674, 410** — `parser_bug_fixed` (2026-06-18). Systemic "Tamil missing"
  bug: sub-entry marker `(a)/(b)/…` glued to the first language inside one `<i>` tag was
  skipped by every `_PATTERNS` entry (`_LANG_CHAR` must start uppercase). Fixed with
  `_OPT_SUBENTRY` non-capturing prefix; see [[project_burrow_parser_status]]. v6 reparse:
  505 entries changed, 0 losses, Ta. gained on 69 DEDs. Validator `entry_match_rate`
  70.0% -> 70.3% (+60 entries). 990/137/2674 fully match now; 410 partial (ittini matches,
  `i` blocked by Phase 2 single-char filter, `iṉṟu…` blocked by a separate Starling
  underscore vs Burrow diacritic normalization gap).

- **`underscore-normalization`** (systemic key, not a DED#) — `parser_bug_fixed` (2026-06-18).
  Starling encodes diacritics in ASCII with a trailing underscore (`in_r_u` = Burrow `iṉṟu`,
  `ir_r_ai` = `iṟṟai`, `cir_ai` = `ciṟai`). `_normalize_for_match` stripped diacritics via
  NFKD but left underscores, so the two sides never reconciled → "language matched but
  headword mismatch" rows. Fixed by adding `.replace("_", "")` to `_normalize_for_match` in
  **both** `starling_tree_validator.py:89` (the copy the validator uses) and
  `burrow_entry_parser.py:198` (kept in sync). 0/34,875 Burrow headwords contain underscores,
  so strictly additive on the Starling side — no regressions possible. Impact:
  `entry_match_rate` 70.3% -> 73.3%, `entries_matched` 13,793 -> 14,383 (+590 across ~299
  DEDs). A faithful pre-fix sim predicted +580; the extra ~10 came from the inline-dialect /
  id-reference match paths the sim didn't cover. DED 410's note was appended with this update.

- **`length-dot-normalization`** (systemic key, not a DED#) — `parser_bug_fixed` (2026-06-18,
  commit `128ca03`). Burrow marks vowel length with a raised dot *after* the vowel (`te·l` =
  `tēl`, `twa·` = `twā`, `aŋga·l` = `aŋgāl`); Starling writes the same length as a macron, which
  `_normalize_for_match` already removed via NFKD + strip-combining. The dots are standalone
  (non-combining) chars, so they survived normalization → "language matched but headword
  mismatch" rows. **Two visually-identical confusables** occur: U+0387 GREEK ANO TELEIA (1,071
  in matched forms, dominant) and U+00B7 MIDDLE DOT (3; what DED 2891 uses) — any fix must cover
  both. Fixed by a `_LENGTH_DOTS` translate table (`{ord(c): None for c in "··ːˑ"}`,
  the two confusables plus IPA length marks U+02D0/U+02D1) applied after `.lower()` in
  `_normalize_for_match`, in **both** files (kept in sync). Matcher-side only — stored corpus
  headwords keep their dots, **no corpus regen needed**; symmetric (Starling already
  length-agnostic via macron-stripping) so no regressions. Impact: `entries_matched`
  14,383 -> 15,004 (+621; 555 exact, 61 loose-substring), `entry_match_rate` 73.3% -> 76.4%.
  Diagnosed from DED 2891 (Kota `tēl`/`te·l`, Toda `twās_`/`twa·s̱`). **Biggest single lever yet.**

- **`eng-velar-nasal-normalization`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18, commit `e1250f2`). Starling writes the velar nasal /ŋ/ as eng (U+014B `ŋ`);
  Burrow uses `ṅ` (U+1E45, n + combining dot above), which NFKD reduces to plain `n`. Eng has
  **no decomposition**, so it survived normalization → "language matched but headword mismatch"
  rows (e.g. 2591/Tulu `ediŋke`≠`ediṅke`, 3014/Kannada `taŋgu`≠`taṅgu`, 63/Tamil
  `aṭaŋku`≠`aṭaṅku`, 3015). Both are notational variants of the same phoneme — NOT a data
  divergence (even Burrow itself writes `ŋ` in some phonetic Kota forms, e.g. `kerŋgl` in
  2591's Cf. note). Folding `ŋ→n` just extends the collapse NFKD already applies to `ṅ`. Fixed
  via `_ENG_FOLD = {ord("ŋ"):"n", ord("Ŋ"):"n"}` translate table applied after `_LENGTH_DOTS`
  in `_normalize_for_match`, in **both** files (kept in sync). Matcher-side only — **no corpus
  regen**. Impact: `entry_match_rate` 76.4% -> 77.6%, `entries_matched` 15,004 -> 15,226 (+222;
  the per-row sim predicted +195, extra from inline/dialect match paths). Residual `ŋ` rows
  that did NOT clear have *additional* genuine divergences (special vowels) and are left alone.

- **`ded-letter-suffix-normalization`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18). DED-not-found class. Burrow's DDSA page numbers DEDR split-entry 4896 as
  `4896(a)` (and mislabels its (b) sub-entry the same, yielding **two** corpus copies). Corpus
  stored `ded_number="4896(a)"`, which the validator's `_clean_ded_number` left intact
  (int-parse fails) → Starling's lookup for plain `4896` missed → all 12 rows fell to "DED 4896
  not found in Burrow corpus". Fixed **matcher-side**: `_clean_ded_number` strips a trailing
  parenthetical letter (`^(\d+)\s*\([a-z]\)$` → base) so `4896(a)`/`4896(b)` index under `4896`;
  both copies fold into one paragraph via `setdefault`. Only letter-suffixed entry in the 5,685
  corpus, so isolated. **Validator copy only** (parser's `clean_ded_number` left as-is to avoid
  corpus-level dup collision); **no corpus regen**. `entries_matched` 15,226 → 15,235 (+9),
  `ded_not_in_corpus` branches 17→10. 3 residual rows now correctly classify as diacritic/hyphen
  divergences (Br. `mukk-ing`/`mukking`, Te. `mūlugu`, Kol. `mū̆lg-`).

- **`naiki-dialect-qualified-abbrev`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18, commit `f29ec78`). **Biggest lever this session.** The parser dropped EVERY Naiki
  attestation: Burrow marks Naiki as `Nk. (Ch.)` (vs. Naikri `Nk.`), but `_LANG_CHAR` forbids the
  internal space, so all four `_PATTERNS` stopped the abbrev at `Nk.` and failed on the ` (Ch.)`
  before the headword — ~314 markers in all three wrapper forms (`<b><i>` 259, bare `<i>` 54,
  `<i><b>` 1). Surfaced as the "Naiki not in DED N; Burrow has …Naikri" language-presence rows
  (5154, 4205). Fixed **parser-side**: optional in-marker qualifier (`_OPT_LANG_QUALIFIER` /
  shared `_LANG_ABBREV`) added to all four patterns; `_clean_lang_abbrev` keeps the qualifier only
  when the inventory recognises the full form (`Nk. (Ch.)` = Naiki) else strips it to the base —
  which also recovered ~25 bibliographic-tag markers (`Te. (SAN)`, `Ka. (DCV)`, `Ta. (lex.)`) as
  their base languages. v7 reparse: **+327 attestations (Nk. (Ch.) +298, Te. +14, Ka. +6,
  Tu./Ma./Ta. +3), 0 losses.** `entry_match_rate` 77.6% → **78.7%**, `entries_matched` 15,235 →
  **15,449 (+214)**, `not_attested` branches 690→668. See [[burrow-parser-status-current-open-issues]].

- **`nested-tag-headword-bold`** (systemic key, not a DED#) — `parser_bug_fixed` (2026-06-18,
  commit `af90534`). **Biggest single lever to date.** Pattern A required the headword run to end
  at `</b>`, so any nested tag inside the headword's bold span broke `([^<]+)</b>` and silently
  dropped the language (no fallback recovers it). Three markup flavors, one root cause: nested
  `<i>obl.</i>` qualifiers (DED 1), italicised scientific names (DED 62), `<at>…</at>` artifacts
  (DED 11). **613 dropped languages / 703 attestations.** Fixed **parser-side**: terminator
  `</b>` → `(?=<)` (stop the run at the next tag — transparent for clean markup) + trim trailing
  `(` artifact in headword cleaning (431/703 needed it). v8 reparse: **+703, 0 losses, 0
  headword-value changes** for already-parsed langs (faithful in-memory sim). `entry_match_rate`
  78.7% → **80.7%**, `entries_matched` 15,449 → **15,841 (+392)**, fully_attested 5,404→5,617,
  not_attested 668→518. See [[burrow-parser-status-current-open-issues]].

- **DED 5154** (per-DED key) — `parser_bug_fixed` (2026-06-18, commit `f277084`). Matcher-side,
  NOT a parser fix despite the status bucket name (ledger has no "matcher" status). Generalized
  the Gondi inline-dialect mechanism (`GONDI_INLINE_ABBREVS` in `dialect_mapping.py`) to
  `KUWI_INLINE_ABBREVS` (F.=Fitzgerald, S.=Schulze, Su.=Sunkarametta, P.=Parja, Isr.=Israel) and
  `KUI_INLINE_ABBREVS` (K.=Khuttia). Burrow's single consolidated `Kuwi`/`Kui` attestation cites
  per-dialect-source forms inline (`(F.) mārrō`, `(S. Isr.) māro`); the matcher previously only
  ever compared against the one stored (excl.-sense) headword, so Starling's per-dialect entries
  (Kuwi Fitzgerald/Schulze/Israel) mismatched their incl.-sense forms. `get_inline_abbrevs_for_
  starling_dialect()` is keyed purely by Starling dialect name string (not Gondi-specific), so
  this was a pure data addition — no new logic, no parser/corpus changes. Systemic like the
  original Gondi mechanism (same sigils recur across the whole Kui-Kuwi cognate set, not just
  DED 5154): `entry_match_rate` 81.4% → **84.3%**, `entries_matched` 15,983 → **16,540 (+557)**.
  Confirmed no regressions (Gondi lookups unchanged, spot-checked). Two more bugs surfaced in the
  same DED 5154 paragraph but deliberately deferred (one diagnosis, one fix): Kui's bare `āju
  (incl.)` sense-suffix form (no dialect prefix — needs a new extraction mechanism, not yet
  built) and Toda embedded in Kota via malformed `<i>...). To.</i>` tag closure (parser-side; see
  [[burrow-parser-status-current-open-issues]] OPEN flag 3, confirmed with concrete HTML evidence
  this session).

- **`leading-qualifier-glued-abbrev`** (systemic key) — `parser_bug_fixed` (2026-06-18, commit
  `32aeb26`). Found while sizing the two bugs from the DED 5154 session above — a corpus-wide
  structural scan turned up a third, bigger pattern: Burrow glues a grammatical/sense qualifier
  (`(tr.)`, `(intr.)`, `(loc.)`, occasionally a botanical name) onto the FRONT of the next
  language's abbreviation inside the same `<i>` tag (e.g. `<b><i>(loc.). Ka.</i> akkuḷisu</b>`,
  DED 25). `_LANG_CHAR` must start uppercase, so no `_PATTERNS` entry could anchor — same failure
  family as Naiki/Ma-type. **97 confirmed misses**, precisely distinguished via real-corpus
  simulation from two surface-similar but structurally different patterns: the original
  To.-type (21 misses, marker stuck inside an already-open `<b>` from the PREVIOUS language —
  harder, no fix shape yet, still deferred) and Kui's bare sense-suffix form (confined to DED
  5154 only, deferred). Fixed via `_OPT_LEADING_QUALIFIER` in `burrow_entry_parser.py` Pattern A
  only. v9 reparse: **+124 attestations, 0 losses, 107 entries changed** (diffed cleaned-corpus
  files directly — see below). `entry_match_rate` 84.3% → **84.6%**, `entries_matched` 16,540 →
  **16,615 (+75)**. See [[burrow-parser-status-current-open-issues]] for the full writeup,
  including a **newly-found but UNDIAGNOSED ~126-case lead** (clean Pattern-A-shaped spans, e.g.
  DED 107 `Go.`, that still don't survive the real parser pipeline for an unknown reason —
  potentially bigger than this fix; start there next).

- **`headword-qualifier-after-lang-marker`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18, commit `e6082c2`). Resolved the ~126-case lead flagged at the end of the previous
  entry. Pattern A's headword capture (`</i>\s+([^<]+)(?=<)`) matched the language abbrev fine
  (e.g. `Go.` in DED 107, `Kuwi` in DED 83) but its capture swallowed a `(Tr.)`/`(S.)`
  dialect-citation qualifier glued onto the headword (no separating tag, since lang marker and
  headword share one `<b>` span); the downstream headword-cleanup filter then dropped the WHOLE
  attestation because it rejects any captured string starting with `(`. Fixed by adding the
  qualifier-skip already used by Patterns B/C/F (shared `_OPT_HEADWORD_QUALIFIER` constant) to
  Pattern A, keeping a **mandatory** `\s+` boundary before it rather than `\s*` — Pattern A's
  capture has no literal anchor tag downstream (unlike B/C/F's required `<b>`), so an initial
  `\s*` version matched with zero whitespace too and introduced false-positive "languages" from
  italicised non-language tokens glued directly to following punctuation with no space:
  `Artocarpus` (DED 15), `Cyprinus` (DED 1252), `Grammar` (DED 1303, a citation-work title),
  `Nāyadh.` (DED 4531). Caught via the corpus-regen diff before shipping, corrected to `\s+`,
  re-verified. v10 reparse: **+63 attestations / 59 entries, 0 losses** (true incremental, diffed
  pre/post cleaned-corpus files directly). `entry_match_rate` 84.6% → **84.9%**, `entries_matched`
  16,615 → **16,657 (+42)**. See [[burrow-parser-status-current-open-issues]].

- **`nested-tag-headword-bold-bcf`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18, commit `791f89b`). Found via the orient pass: DED 5440 had 8 different
  Starling languages mismatching for one DED# (the skill's own signature for a local-HTML
  bug). Diagnosed as the same root-cause family as the Ma-type fix (`af90534`) but for
  Patterns C, B/D, F instead of A: their headword capture required a literal `</b>` with
  zero tags inside, so a nested grammatical qualifier (`<i>pl.</i>`, `<i>obl.</i>`)
  mid-headword broke the match and silently dropped the language (DED 5440 `Kuwi`, DED 1
  `Go.` — both still missing today, confirming it was systemic, not a one-off). Fixed via a
  new `_HEADWORD_SPAN_ACROSS_NESTED` capture that spans across the nested tag instead of
  stopping at it (Pattern A's simpler `(?=<)` fix doesn't transfer — these three patterns'
  headword has its own closing tag to lean on, so a naive lookahead swap would truncate
  real content after the nested tag). **Caught a regression before shipping**: a first
  version without a language-marker guard swallowed a DIFFERENT language's own
  `<i>NextLang.</i>` marker into the preceding language's still-open headword span (DED
  4900, DED 5161 — `Go.` vanished both times), caught via full-corpus old-vs-new parser
  diff (2 losses) before regenerating. Fixed by adding a second negative lookahead blocking
  spanning into `<i>Uppercase...` (grammatical qualifiers are always lowercase). v11
  reparse: **+163 attestations / 121 entries, 0 losses**. `entry_match_rate` 84.9% →
  **86.6%**, `entries_matched` 16,657 → **17,003 (+346)** — a much bigger validator jump
  than the corpus delta alone suggests, since recovered Go./Kuwi/Konḍa attestations each
  satisfy many Starling per-dialect rows at once. See
  [[burrow-parser-status-current-open-issues]].

- **`primary-dialect-marker-gloss-truncation`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-18, commit `7476c9e`). Found while drilling into DED 5440's remaining queue rows
  after `nested-tag-headword-bold-bcf` recovered its `Kuwi` attestation: several rows still
  showed `Match: Yes` but were flagged via the `meaning_mismatch` audit heuristic. Diagnosed as
  a pre-existing matcher gap (not a regression): Burrow's consolidated `Go.`/`Kuwi`/`Kui` entries
  cite their FIRST dialect's form right after the language marker with its own parenthetical
  (e.g. `Kuwi (F.) vegū (pl. veska), (Su. Isr.) vegu..., (P.) vergu..., (S.) weggu...`); that
  `(F.)` marker sits exactly where the headword-qualifier-skip consumes it during parsing, so it
  never survives into the gloss for `_extract_gloss_forms_for_abbrevs` to find when matching
  "Kuwi (Fitzgerald)" specifically — falls back to a direct headword match using the FULL
  untrimmed gloss as "meaning", bleeding every later dialect's text into an otherwise-correct
  match. Sized corpus-wide: ~1,108 Go. + 1,035 Kuwi + 41 Kui entries structurally exposed (upper
  bound; real impact gated by whether Starling has a per-dialect row for that DED#). Fixed
  **matcher-side only** (no parser change, no corpus regen): new
  `_truncate_gloss_before_first_marker` trims the displayed meaning at the first dialect marker,
  deliberately returning `""` when there's no text before it (honest, and avoids the false-flag
  since `_is_meaning_mismatch` treats an empty side as "nothing to compare"). **Caught a second
  bug while verifying**: both `_validate_branch` and `_validate_branch_direct` had
  `if matched_burrow_gloss: vr.burrow_gloss = matched_burrow_gloss` at the call site, which
  silently discarded the intentional `""` and fell back to the SAME untrimmed `att.gloss` —
  caught because the first validator run showed zero change despite the helper working correctly
  in isolation; fixed by using `matched_burrow_gloss` unconditionally whenever
  `get_inline_abbrevs_for_starling_dialect` applies. No `entry_match_rate` change (86.6%, expected
  for a display-only fix) — `row_issues`/triage queue: 5567 → **5161 (-406 rows)**. Two NEW,
  separate findings surfaced and deliberately left open (see below): Manda/Kui gloss
  over-extension, and id.-elided dialect meanings.

- **`inline-dialect-marker-meaning-noise`** (systemic key) — `parser_bug_fixed`
  (2026-06-19, commit `e2f46a8`). Matcher-side, not a parser fix. Drilled into DED 5440's
  remaining queue row (Kui/Kuwi (Israel)/Maria Gondi/Parja Kuwi/Sunkarametta Kuwi all
  failing). Root cause for 4 of the 5 (Kui is separate — see "Manda/Kui gloss
  over-extension" below): `_extract_gloss_forms_for_abbrevs` split a marker's segment with
  a single `segment.split(None, 1)` — first whitespace token becomes the form,
  **everything else** becomes "meaning". Breaks when Burrow cites more than one
  comma-separated form per marker (`(Ma.) vaẖk, veẖki` → meaning becomes the bogus literal
  `"veẖki"`) or attaches a bare `(pl. ...)` to a non-primary form (`(Su. Isr.) vegu ( pl.
  veska)` → meaning becomes `"( pl. veska)"`) — the bogus meaning then fails
  `_is_meaning_mismatch`'s substring check, producing the false "headword mismatch" rows.

  **First attempt (REVERTED, too risky):** joined the comma-chain into the matched FORM
  itself and attached immediate `(...)` parens to the form. Fixed the 4 target rows but,
  on a full validator re-run, **regressed 7 other entries from matched to unmatched**
  (`entries_matched` 17098→17095): DED 5151 Kuwi(Schulze) (giant interrogative-pronoun
  entry — chaining grabbed 3 forms when Starling only recorded a differently-curated 1st
  form), DED 1159/4885 Kuwi(Schulze) (Burrow's `"pl."` word vs Starling's bare `"(kanka)"`
  — attaching it broke the substring match), DED 4968 Maria Gondi (paren held *multiple*
  dialects' plurals `"(pl. D. molohk, Ma. molosku)"`), DED 4900 Adilabad Gondi (paren was a
  `"(Voc. 2870)"` citation ref, not a plural), DED 4325 Maria Gondi (Burrow `ˀ` U+02C0 vs
  Starling `ʔ` U+0294 — unrelated confusable, newly exposed once the paren joined the
  strict-matched form). **Lesson: lengthening/altering the matched FORM string makes the
  substring-match heuristic MORE fragile, because Starling's side wasn't extended in
  lockstep — never touch the form when fixing a meaning-display bug in this matcher.**

  **Second attempt (also caught a near-miss before shipping):** kept form byte-identical
  to old logic, only suppressed "meaning" to `""` via a single-shot noise regex matching a
  bare second form / bare `(...)` citation / trailing `id.`. A full row-level diff (not
  just the topline match rate) showed 457 meaning-only changes — spot-checking found
  genuine English glosses like `"shelter"`, `"mother"`, `"partner"` were being wrongly
  emptied (**364 of 457** were real ASCII glosses, only 95 were the intended leaked-form
  noise). **Lesson: a single bare word after a dialect-marker headword is syntactically
  identical whether it's a leaked second Dravidian headword spelling (noise) or a genuine
  one-word English gloss (real data) — pure regex shape can't tell them apart.**

  **Shipped fix:** added an ASCII gate — a bare single-word remainder is only treated as
  noise if it contains a non-ASCII character (Burrow's English glosses are always plain
  ASCII; only the transliterated Dravidian forms carry diacritics). The `(...)` /
  trailing-`id.` citation branch needed no such gate (grammatical citations are
  legitimately ASCII, e.g. `"(pl. veska)"`, `"id."` — verified by inspecting all 148 ASCII
  suppressions post-gate: all were `(...)`/`id.` citations, zero bare-word ASCII
  false positives). Final verification: full row-level diff across all 20,293 rows = **0**
  Match-status or matched-form changes; 243 meaning-only changes, manually confirmed all
  legitimate (95 non-ASCII leaked forms + 148 ASCII grammatical citations, zero genuine
  glosses lost). `entry_match_rate` unchanged 87.1% / `entries_matched` 17,098 (display-only
  fix, as expected). Triage queue 4894 → **4658 (−236 rows)**.

  **Process lesson (apply to every future matcher-side fix):** verify with a full
  before/after row-level CSV diff (saved old code via `git show HEAD:<path>` to a
  throwaway sibling script, ran both, joined on (record#, DED#, branch, language)),
  checking BOTH the `Match`/form columns (catches the first regression) AND eyeballing a
  sample of `meaning`-only diffs (catches the second, quieter one) — not just the topline
  `entries_matched` count, which can hide N regressions and N improvements cancelling out,
  and won't show silent meaning-only data loss at all.

- **`multi-headword-gloss-leak`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-06-23, commit `d42d0f3`). Found by the user manually sampling 10 rows per
  `Match` bucket from `tree_validation_results.csv` (not from a triage-queue drill-down)
  — a different discovery path than every prior entry in this ledger, confirming manual
  spot-sampling the raw results is a viable way to surface systemic issues the queue
  rollup doesn't directly highlight. Root cause: `_recover_attestation_gloss_from_full_text`
  (in **both** `starling_tree_validator.py` and `repair_burrow_corpus_glosses.py`, kept in
  sync) anchored its full_text marker regex on `headwords[0]` alone, so every additional
  comma-separated alternate spelling (e.g. Kannada `"matti, maddi, mar̤ti"`) leaked into the
  recovered gloss ahead of the real prose (`", maddi, mar̤ti several Terminalia species..."`).
  **Both parser-side and matcher-side**: the `repair_burrow_corpus_glosses.py` copy writes
  `att["gloss"]` directly into the corpus (called from `reparse_burrow_corpus.py`), so the
  leak was baked into `burrow_corpus.cleaned.json` itself, not just a validator display
  artifact — required a full corpus regen, not just a validator re-run.

  Fixed by anchoring on the FULL headword chain (`\s*,\s*`-joined, flexible-separator
  regex) instead of just the first token; updated all 6 call sites in
  `starling_tree_validator.py` + the 1 call site in `repair_burrow_corpus_glosses.py` to
  pass the full `headwords` list instead of `headwords[0]`. **Pre-shipping validation
  caught a subtlety the fix alone would have hidden**: simulating against the already-once-
  repaired `burrow_corpus.cleaned.json` (using its already-leaky gloss as the
  `fallback_gloss` baseline) showed almost no improvement, because the function's own
  `tail if len(tail) > len(fallback) else fallback` tie-breaker prefers whichever is
  *longer* — and the leaky old fallback was artificially long, so it kept winning over the
  newly-correct (shorter) tail. Re-simulating against the **raw, pre-repair**
  `burrow_corpus.json` (the function's true input in the real pipeline) showed the real
  effect: 4,153 of 4,297 noisy multi-headword cases (96.6%) cleanly fixed, 0 regressions.
  **Lesson: when sanity-checking a fix to a function that compares against its own
  previous output as a fallback, simulate against the TRUE upstream input, not a
  once-already-corrupted downstream artifact — otherwise a self-reinforcing "prefer the
  corrupted-but-longer value" bug can mask the fix's real effect entirely.**

  Final verification (full row-level diff, all 20,293 rows, post corpus-regen +
  validator re-run): **0** `Match`-status changes, **0** `Matched Burrow form` changes,
  **2,293** `Matched Burrow meaning` improvements. `entries_matched`/`entry_match_rate`
  unchanged (17,098 / 87.1%, expected for a display-only fix). Noise heuristic (leading
  comma-chain / orphaned closing paren) fell from 3,662 → 1,514 validator rows (corpus-level
  attestation glosses: 6,231 → 2,025). 12/12 manually spot-checked changed rows confirmed
  correct. Triage queue **4,658 → 4,544 groups** (issue rows 4,766 → 4,650).

  **144 still-noisy cases (out of the original 4,297) have a DIFFERENT, deeper cause,
  deliberately left open**: some 3+-alternate-spelling entries have a parenthetical
  qualifier (e.g. `"(B. also)"`) wedged between two alternates in Burrow's actual text,
  and the base parser never captured that extra form into `headwords` at all — this fix
  can only anchor on what's already in the headwords list, so a missing-3rd-form parser
  bug isn't addressed here. Also explicitly NOT touched (separate, deferred root causes
  behind the remaining ~1,514 validator-row noise count): duplicate-headword-spelling
  collisions across distinct sub-entries in one paragraph causing `re.search` to anchor at
  the wrong (leftmost) occurrence (e.g. DED 2468's `Nk.`/`Nk. (Ch.)` both spelled `"sār"`);
  `"Ma."` being deliberately in the bounding-token `ignore_tokens` set (needed elsewhere for
  Gondi-dialect-marker disambiguation) which lets a genuine new Malayalam attestation get
  swallowed into the previous language's gloss; and a qualified-`id.`-chain re-prepending
  bug spotted incidentally in DED 4718 Gadba/Parji (Gadba's `id.` resolved to Parji's
  qualifier text + a truncated fragment of Parji's real gloss, not Parji's full meaning).

- **Six-fix batch (2026-08-16, delegated to a Sonnet subagent, committed `ff6370b` —
  originally `17978c3`, rehashed by a pull-rebase over the user's `current_page` fix, pushed)** —
  user found 7 bug rows by manually sampling `validation_audit_report.xlsx`
  `row_issues`; parent session diagnosed, subagent implemented+verified all six, one corpus
  regen + one validator run. `entry_match_rate` 87.1% → **94.0%**, `entries_matched` 17,080 →
  **18,184 (+1,104)**, total_language_entries 20,275 → 20,007 (−268 pseudo-language rows).
  Ledger keys: `scraper-styled-run-spacing` (dravidian_scraper.py `extract_field_data` now
  `get_text(' ', strip=True)`; future scrapes only), `pseudo-language-fields` (`_METADATA_KEYS`
  += "Additional Forms"/"Miscellaneous"/"Stems"/"Notes on correspondences"),
  `gloss-secondary-form-scan` (**the big lever, +~1,100**: new `gloss_secondary` match_type in
  `_match_entry` — scans best attestation's recovered gloss, semicolon-split segments,
  normalized whole-token containment, ≥3-chars/≥2-tokens guard, dialect-marker truncation for
  Gondi/Kuwi/Kui; fires only after all existing paths fail), `headword-paren-comma-split`
  (parser `_split_headword_chain`, depth-0 commas only; 950 headword-list merges, 0 attestation
  losses), `barred-i-fold` (textnorm `BARRED_I_FOLD` ɨ U+0268/Ɨ U+0197 → i),
  `seoni-gondi-inline-sigil` (`"S.": ["Seoni Gondi"]`, evidence DED 133/718). 36 rows flipped
  Yes→Language-only — all false positives eliminated by Fix D (truncated fragments like `-nt-)`
  had coincidentally substring-matched); 29/36 are the **pending ẓ/r̤ ruling** pairs, 7 are
  Br. -ing hyphenation + 2 content divergences (DED 4572 Ta., 5270 Salur Gadba). Backups:
  `*.before-bugfix-batch.*` in data/dravidian/cross-validating-dded-starling/tree_validation_output/ and data/dravidian/burrow_ded/.
  Also diagnosed but deliberately NOT fixed: raw scrape holds duplicate paragraph records for
  some DEDs (two "137" from page 14, one truncated; loader concatenates → doubled attestations)
  and `repair_burrow_corpus_glosses.py` id.-expansion glues the ENTIRE previous language's gloss
  onto the next attestation (DED 7 Ka. starts with all of To.'s gloss; raw scrape's `id.;` is
  correct) — both queued as follow-ups.

- **Corpus-duplication reconciliation (2026-08-16, this session — roadmap item 2 follow-ups
  #1+#2)** — two loader-side fixes in `load_burrow_corpus` (`starling_tree_validator.py`), no
  corpus regen, no re-scrape; `entry_match_rate` **holds 94.0%** (18,184). Ledger keys
  `duplicate-scraped-paragraph-dedup` + `appendix-page-edition-mislabel`. **Fix A:** the raw
  scrape has 84 same-page double-scrape DED groups (one copy truncated at an (a)/(b) boundary,
  e.g. DED 137 p.14); the loader's setdefault+append concatenated them → overlapping languages
  doubled. Dedup each paragraph's attestations on the fully-repaired
  `(language_abbrev, headwords, gloss)` tuple after concat+repair → **988 folded**,
  match-rate-neutral (removing dup Burrow forms can't change a Starling match; win = de-doubled
  coverage/audit). **Fix B:** 10 Appendix supplement entries (pp.509–512, DED
  1/3/4/7/27/44/45/47/49/50) mislabelled `edition="DEDR"` because `editions.detect_edition_from_text`
  matched their `DED(S) N` backward-ref and overrode the page≥509 classification; their IA-loan
  reflexes were merging into the real DEDR paragraph. Loader now skips
  `edition=="Appendix" OR page>=APPENDIX_START_PAGE` (page authoritative; 62/5623 clean split).
  1 eliminated false positive (DED 1 Starling Tamil `a` had matched appendix-only `akkaṭa`).
  **Verified:** full row-level diff, 20,007 rows both sides, 0 added/removed, exactly 1
  Match-status change (that false positive). Real `data/dravidian/cross-validating-dded-starling/tree_validation_output/` regenerated.
  **~~Deferred~~ RESOLVED (ledger `id-expansion-overglue`, 2026-08-17):** follow-up #2
  id.-expansion over-glue — shipped **option A′ (leave ambiguous `id.` unresolved)**. New
  `textnorm.antecedent_is_multiform()`; both resolvers (`burrow_entry_parser.parse_language_sections`
  + `repair_burrow_corpus_glosses.repair_corpus`, kept in sync) now guard
  `resolvable = last_real_gloss and not multiform` → single-form antecedents resolve as before,
  multi-form keep the literal `id.` (faithful to Burrow). The guard can only *skip* resolution,
  never assert a new meaning. **Gold set = FULL 192-case multi-form population** (of 2,985 id.
  refs; 2,793 single-form already correct), all hand-labeled: A′ leave = 0 corruptions;
  B head-meaning = 74% correct but **26% fabricated-wrong (50/192)**; C nearest = 40% correct,
  60% wrong (antecedents often end in their own `id.`/citation); D whole-copy (old) = 100% wrong
  on multiform. Chose A′ under honesty>completeness (never fabricate to look complete). Finding
  re-verified vs source: which antecedent form `id.` points at is **editorial, not positional**
  (DED 7 needs last form `wïrxity`; DED 54/88 need head — direct contradiction; true form even
  non-cognate). **Verification:** corpus regen changed 202 glosses (all → literal `id.`/cleaner,
  never a new meaning); validator diff vs baseline = 0 rows added/removed, `entry_match_rate`
  **94.0% held**, `entries_matched` **18,184→18,185 (+1)**, exactly 3 Match-status changes ALL
  positive (DED 4847 Seoni Gondi `Language only→Yes`; DED 5554 Kui & DED 7 Kannada `aŋ-gāl`
  upgraded `gloss_secondary→substring/exact`), 0 lost; 141 rows changed only in displayed meaning.
  Accepted cost: ~139 head-would-be-right cases now show `id.` (e.g. DED 88 Toda `aḍky`). Prior
  corpus saved `burrow_corpus.cleaned.before-id-fix.json`. **TODO: re-sync
  `burrow_corpus.cleaned.json` to shared Box folder** (item 7 Sources reads it). Uncommitted.
  Writeup in `docs/dravidian_validator_progress.md` §7.

- **Vowel-headword + zh-fold + Pattern-G batch (2026-08-18, this session — commits `fc3104b`
  then `fae4cca`, pushed to `dravidilex-pilot`)** — three fixes surfaced by triaging
  `validation_audit_report.xlsx` `row_issues` (789 Language-only + 380 No rows). Discovery:
  the "No" bucket's core-language-absent rows traced to the deictic/pronominal base entries
  losing their headword language. **Fix 1 — `single-char-vowel-headword` (`fc3104b`, parser):**
  `burrow_entry_parser` line-611 `len(hw) > 1` filter dropped bare-vowel headwords, discarding
  the whole attestation when a chain reduced to single vowels; added `_VOWEL_HEADWORDS`
  frozenset("aāiīuūeēoō") guard (`len(hw) > 1 or hw in _VOWEL_HEADWORDS`). Regen +23 vowel slots
  across ~20 entries (DED 1,328,332,334,410,533,534,557,606,728,764,827,870,3684,3720,5160), 0
  losses; the 4 diff "losses" were before-images of attestations that GAINED a vowel. Validator
  18,185→**18,190 (+5)**, 94.0% held (chose vowel whitelist over the earlier `hw.isalpha()`
  proposal — keeps single consonants filtered). **Fix 2 — `zh-transcription-fold` (`fae4cca`,
  matcher, RESOLVES the pending ẓ/r̤ ruling):** Starling ẓ (z+U+0323) vs Burrow r̤ (r+U+0324) =
  same Tamil/Malayalam retroflex ழ; surgical fold both → U+1E93 in `textnorm.normalize_for_match`
  after NFKD, before combining-strip (ṛ = r+U+0323 deliberately untouched). Matcher-side, **zero
  corpus change**. Sim on 792 Language-only rows predicted 182 flips. The paired **ch→c fold was
  evaluated and DECLINED** (only +1 row, DED 46 achchānā/accānā, while reaching every "ch"
  headword — poor risk/reward). **Fix 3 — Pattern G (`fae4cca`, parser, closes
  `single-char-vowel-headword-ded1-ta`):** DED 1's own Ta. headword uses `<i><b>Ta.</b></i>` +
  plain-text lone-vowel headword `a` (Pattern C needs bold headword, Pattern E needs bare
  `<i>`); new `_PATTERNS` entry `<i><b>Lang.</b></i>\s+(lone-vowel chain)(?=[\s.;])`, lone vowel
  = quality guard, only 1 match corpus-wide. Regen +1 (DED 1 Ta.). **Combined validator run
  (Fixes 2+3): `entries_matched` 18,190 → 18,381 (+191), `entry_match_rate` 94.0% → 95.0%.**
  Backups `*.before-patternG-zh.*` and `*.before-vowel-headword-fix.*`. TODO still open: re-sync
  corpus to Box. See [[burrow-parser-status-current-open-issues]].

- **`A-suffix-ded-fold`** (systemic key, not a DED#) — `parser_bug_fixed` (2026-08-18, this
  session — commit `02b1fa2`, pushed to `dravidilex-pilot`). **Generalizes the earlier
  `ded-letter-suffix-normalization` (4896(a)/(b)) fix to the bare-letter suffix form.** Found by
  triaging `validation_audit_report.xlsx` — DED 3621 had 14 "No" rows (the skill's local-HTML-bug
  signature). NOT a parser bug and NOT a genuine divergence: Burrow marks some split entries with
  a **bare-letter DED suffix** (`3621A` "bug" vs `3621` "night"; also 583A, 1634A, 1693A, 3160A,
  3326A, 3431A, 4145A, 4265A, 5400A, 5410A), where Starling keys **both split halves on the plain
  base number** (confirmed: Starling's page-59/rec-6 bug record tags every branch
  `"Number in DED": "3621"`). The parser correctly extracts 3621A's forms; the validator just
  compared Starling's `3621` bug forms against Burrow's `3621` "night" paragraph and failed. Fixed
  **matcher-side**: extended `textnorm.clean_ded_number`'s suffix-fold regex from `\(a\)`-only to
  `(?:\([a-z]\)|[A-Za-z])$`, so the loader's setdefault+append merges each split half's
  attestations under the base key (mirrors the 4896 fold). The corpus keeps `3621A` distinct
  (`BurrowEntryParser.clean_ded_number` untouched) — **zero corpus regen**. Each Starling branch
  still only matches forms textually present in its half (a form matches where it exists; safe by
  construction, and the fold touches only the 13 A-entries so nothing else can move). Sim
  confirmed all 8 populated bases recover with **0 "No" remaining**; full validator:
  `entry_match_rate` 95.0% → **95.1%**, `entries_matched` 18,381 → **18,410 (+29)**,
  entries_with_ded unchanged (19,354), **0 losses**. Dominated by DED 3621 (14 No + several
  Language-only recovered). Backup: `tree_validation_summary.BEFORE-Afold.json`.

- **`totype-bold-embedded-marker`** (systemic key, not a DED#) — `parser_bug_fixed` (2026-08-18,
  this session — commit `44287fc`, pushed; **CORPUS REGEN'D — re-sync `burrow_corpus.cleaned.json`
  to Box**). **Closes the deferred To.-type plain-text variant** (the bold-outer sub-case; see the
  `totype-embedded-marker` entry below, which handled only the bold-INNER-headword flavour).
  Complement of `ee716f1`. Found by triaging `validation_audit_report.xlsx`: the Malayalam-heavy
  "No" cluster (44 Ma. rows) traced to Burrow's botanical/zoological entries folding the PREVIOUS
  language's trailing **scientific name** and the NEXT language's marker into one `<i>` span, with
  the next headword as **plain text** in a fresh bold run:
  `<b><i>Sesbania grandiflora. Ma.</i> akatti <i>S. grandiflora</i></b>`. Marker at the `<i>` end
  (behind the sci-name), headword bare text → fell in the gap between Pattern A (needs marker at
  `<i>` start), leading-qualifier, and `_STRICT_PATTERNS` (`_TOTYPE_NOT_BOLD` excludes a `<b>`
  before the `<i>`, and it needs a fresh-`<b>` headword). Language dropped entirely + its content
  bled into the previous language's gloss. Fixed with a **2nd `_STRICT_PATTERNS` entry** anchored
  on `<b>\s*<i>…Abbrev.</i> plain-text-headword`, gated by `_is_known_lang_abbrev` + a new
  `_FORM_FIRST` first-char guard. **The mandatory `<b>` before the `<i>` is the discriminator**
  vs the elided-form risk (DED 814 `erukku <i>Calotropis gigantea. Ma.</i> gigantic swallow-wort`
  — marker in running text, English gloss follows), deliberately NOT matched: the editor opens a
  fresh `<b>` only when a real headword follows. **Verification (full documented method):**
  read-only old-vs-new parser sim = **+224, 0 lost, 0 changed headwords, 0 English-gloss
  contamination** (all 224 inspected; only DED 1120 `kaṭala. ?` mildly messy = faithful capture of
  Burrow's own `?` uncertainty marker, not contamination; 9 space-containing headwords all genuine
  Toda/Kodagu compounds like `ke·re pa·mbï`). Corpus regen incremental diff vs backup = **+221, 0
  losses** (Ma 116, Ka 34, Te 27, Tu 15, Koḍ 10, To 5, Pa/Ko 4, +6; the 224→221 gap = 3 Appendix
  entries the validator skips). Full row-level validator diff (20,007 rows aligned): **No→Yes 62,
  Language-only→Yes 1, Yes→non-Yes 0** — the gloss-shortening caused **zero** `gloss_secondary`
  regressions. `entry_match_rate` 95.1% → **95.4%**, `entries_matched` 18,410 → **18,473 (+63)**,
  denominator unchanged. Backups: `burrow_corpus.cleaned.before-totype-embedded.json`,
  `tree_validation_summary.before-totype-embedded.json`. See [[burrow-parser-status-current-open-issues]].

- **`leading-qualifier-glued-abbrev-patternC`** (systemic key, not a DED#) — `parser_bug_fixed`
  (2026-08-18, this session — commit `57e038d`, pushed; **CORPUS REGEN'D — re-sync
  `burrow_corpus.cleaned.json` to Box**). **Extends `leading-qualifier-glued-abbrev` (`32aeb26`,
  which fixed only Pattern A / `<b><i>` order) to the `<i><b>` marker order.** Found by triaging
  the fresh post-fix "No" rows: DED 946 (8 rows, "to break") and DED 3682 (8 rows, "to be filled")
  were pure Gondi-dialect clusters where Burrow HAD a rich `Go.` attestation in full_text but the
  parser extracted **none** (`parsed Go. attestations: []`). Root cause: the PREVIOUS language's
  trailing grammatical qualifier (`(intr.)`/`(tr.)`/`(loc.)`) is glued in front of the next marker
  inside the italic, so the abbrev capture starts on `(` and fails `_is_valid_lang`, dropping the
  whole language — which then fails every per-dialect Starling row it should have fed (one dropped
  `Go.`/`Ga.` = 8 failing dialect rows). Two shapes: **(C)** `<i><b>(intr.). Go.</b></i> (Tr.)
  <b>wōṛānā</b>` (DED 946) → added `_OPT_LEADING_QUALIFIER` to Pattern C; **(C2)** `<i><b>(tr.).</b>
  Go.</i> (Tr.) <b>nindānā</b>` (DED 3682, qualifier bolded in its OWN `<b>`, marker plain-text) →
  new Pattern C2. Both headwords are `<b>`-bounded → contamination-proof (unlike the plain-text
  To.-type). Sized: shape C = 27 markers / 21 DEDs (**Ga 14, Go 9**, Kuwi 2, Te/Kol 1 — Gadba the
  biggest victim, NOT Gondi-specific), shape C2 = 1 (DED 3682 only). **Verification:** old-vs-new
  parser sim = +28, 0 lost, 0 changed headwords; corpus regen incremental = **+24 unique** (Go 10,
  Ga 10, Kuwi 2, Te 1, Kol 1; 28→24 = dup-scraped Ga at DED 990/1278 folding at load), 0 losses;
  full row-level validator diff (20,007 rows) = **No→Yes 42, No→Language-only 1, Yes→non-Yes 0**.
  `entry_match_rate` 95.4% → **95.7%**, `entries_matched` 18,473 → **18,515 (+42)**. Backups
  `burrow_corpus.cleaned.before-leadqual-C.json`, `tree_validation_summary.before-leadqual-C.json`.

- **Language-shape 4-lever batch (2026-08-18, this session — commits `e74bdb9`, `03d4e92`,
  `55135f2`, `1f9bf85`, pushed; **CORPUS REGEN'D — re-sync to Box**).** Driven by a NEW investigation
  method the user requested: instead of per-DED drilling, **group the remaining "No" rows by
  (Starling language → Burrow abbrev) and cluster by HTML shape** to find levers that fix many at
  once. Sourced from the (fresh, validator-regenerated) `validation_audit_report.xlsx`. Classified
  ~246 No rows: **~100 "marker in HTML but parser-dropped" (actionable)**, ~97 genuinely absent
  (cross-source divergence), ~15 form-differs, ~34 unmapped. Key insight: **Gondi (Go.) + Gadba
  (Ga.) dominate parser-drops** (complex consolidated multi-dialect markup), and
  **"scientific-name-before-marker" is a mega-theme spanning multiple tag orders**. Four levers, each
  a separate commit verified with a parser sim (0 lost/0 changed/0 suspicious) + full row-level
  validator diff (0 Yes→non-Yes regressions):
  1. **`leading-qualifier-plain-before-boldlang-patternC3`** (`e74bdb9`): `<i>(neut.). <b>Go.</b></i>`
     (5th tag-order of the leading-qualifier family). Sim +25, regen +17, +10 matched (1 Yes→Lang
     was a **false-positive correction**, DED 2402 Salur Gadba `saḍpi` absent from Burrow).
  2. **`citation-glued-marker`** (`03d4e92`): source citation opens `(` inside marker, closes after
     (`<i><b>Te. (TVB</b></i>`); dominant source `(LSB` meant **Belari + Kurumba were dropped
     corpus-wide**. Sim/regen +25 (Kurub 10, Bel 8, …), +5 matched.
  3. **`sciname-before-marker-bold-order`** (`55135f2`): `<i><b>Z. rugosa. Go.</b></i> <b>hw</b>`
     (bold-order analog of 44287fc). Strict pattern, +17 sim / +15 regen, +7 matched, 95.7→95.8.
  4. **`sciname-before-marker-running-text`** (`1f9bf85`): **closes the long-deferred running-text
     variant** (`<i>M. edule. Tu.</i> alimarů`, no `<b>` anchor). Risk = elided-gloss (DED 814
     `gigantic swallow-wort`); solved with a **LEXICAL guard `_rt_headword_ok`** (diacritic/length-dot
     OR not-an-English-gloss-word), verified to reject 814 and keep `alimarů`/`atti`. Sim +57 (63
     spans − 6 guard-rejected), regen +53, **+21 matched**, 95.8→95.9.
  **Batch total: +110 attestations, +43 matches, `entry_match_rate` 95.7% → 95.9%, 0 regressions.**
  Verification helper: `scratchpad/verify.py` (parser sim / row diff / corpus diff). Baselines saved
  `results.baselineN.csv`. STILL OPEN from this map: ~59 "other" parser-drops (finer split needed —
  consolidated-inline Gondi/Gadba dialect sigils are **matcher**-side, plus residual sci-name orders
  e.g. `<i>P. quadrifida. <b>Ma.</b></i>` sci-name-plain-before-boldlang, not yet done); ~97 genuinely-
  absent (divergence, not parseable); ~15 form-differs.

## Queue state / next candidates

**StarlingDB re-scrape reconciliation (2026-08-15):** re-ran the validator against a fresh
scrape (`data/dravidian/starling/starling_complete_data_scrape.json` — 2211 records, 10,905
unique entries, `backfilled_from_old: 47`) with the **unchanged v8 corpus / no code change**.
`entry_match_rate` **holds at 87.1%**; total entries 20,293→20,275, `entries_matched`
17,098→**17,080** (−18), branches 7,991→7,986. **Zero `Yes→No` regressions;** all deltas
confined to 10 DED#. The −18 is NOT data loss: the prior scrape double-listed some etymon
trees, so −30 rows were duplicate sub-entry branches collapsed (DED 5440 −13, 2591 −6, 4587
−5, 2891 −4, 3014 −1, +1 orphan blank-DED Konda; unique attestation sets verified identical),
while +12 are genuine NEW attestations (DED 1297 +6, 4980 +4, 3972 +1, 5511 +1). Baseline
preserved at `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.before-new-scrape.csv`; reconciliation
written up in the Obsidian progress report §6. Net: the refreshed source is strictly cleaner +
marginally richer; no regen/fix warranted. Figures below (17,098) are the pre-re-scrape baseline.

**Canonical transcription policy (2026-08-16, Kevin + Todd):** for genuine transcription
divergences, **DEDR/Burrow is the canonical transcription**; Starling retained as variant
("pick a version, changeable later"). Key insight: "language-only" rows split into (i)
*notational variant* (same form, diff glyph — `aṛpɨn`↔`aṛpïn`, `oʒ-`↔`oζ-`, `alraʔānā`↔`alra'ānā`;
safe to adopt DEDR spelling) vs (ii) *divergent form* (Starling's reflex isn't Burrow's —
`addalipuni`↔`adůruni,adaruni,aduruni`; MUST keep Starling or you corrupt data). Gate = a
transcription-equivalence fold; user chose the **conservative core** set only (`ɨ/ï ɫ/ł ʒ/ζ ʔ/'`
on top of shipped underscore/length-dot/eng folds); `ẓ/r̤` **RESOLVED 2026-08-18** as a fold
(commit `fae4cca`, +~190 rows → 95.0%; see the 2026-08-18 batch entry) — Starling ẓ and Burrow
r̤ are the same retroflex ழ, a notational variant, not a divergent form. `ch/c` was evaluated
the same session and **declined** (only +1 row, broad reach). Implemented **validator-layer only** (no regen, no match change,
87.1% held): `starling_tree_validator.py` gained `_CANONICAL_CORE_FOLD`, `_transcription_key`,
`_canonical_burrow_form`, `_canonical_fields` + 3 result columns `Canonical headword` / `Canonical
source` (burrow|starling) / `Transcription status` (identical|notational_variant|divergent_form|
no_burrow_match|no_ded). Dist (new scrape): notational_variant 9,853 (incl. 304 genuine
language-only swaps), identical 7,531, divergent_form 1,591, no_ded 664, no_burrow_match 636;
source burrow 17,384 / starling 2,891. **DEFERRED build-layer step:** `build_dravidilex_import.py`
is still Starling-sourced, so the *published pilot* is unchanged — join these columns to make the
pilot headword := `Canonical headword` (keep Starling as `Headword (Starling)` extra) once the
fold set is confirmed. Documented in Obsidian progress report §7. `_normalize_for_match` itself was
NOT touched (parser-sync invariant preserved); the core fold is validator-only.

**Current as of 2026-06-23 (most recent fix, supersedes the stale 86.6%/17,003 figures in
the paragraph below — those predate `qualified-id-chain-resolution`, `totype-embedded-
marker`, `inline-dialect-marker-meaning-noise`, and `multi-headword-gloss-leak`):**
`entry_match_rate` **87.1%**, `entries_matched` **17,098** (unchanged since `totype-
embedded-marker` — the two most recent fixes are display-only), triage queue **4,544
groups** (down from 4,658).

**Workflow shift (2026-06-19):** the user is now driving manual triage themselves (eyeballing
`triage_queue.csv` rows directly) rather than having Claude drive the `/triage-ded` 10-step
loop end-to-end. Claude's role going forward: hand over queue contents/summaries on request,
and give a second opinion on specific DED#/language pairs the user flags as confusing —
diagnosis is still reliable; full-corpus regression risk on any *fix* is what justified
slowing down to manual review (see the lessons in the `inline-dialect-marker-meaning-noise`
entry above for why "looks right on paper" isn't enough to trust a matcher-side change).

Resolved cumulatively: DED 0 (validator sentinel), Tamil-missing cluster (990/137/2674/410),
underscore-norm (+590), length-dot (+621), eng velar-nasal (+222), DED-letter-suffix/4896 (+9),
Naiki dialect-qualified abbrev (+214), Ma-type nested-tag-in-headword (+392), Kuwi-type embedded
forms (parser-side, commit `6eacbae`, ~48 entries — shipped without a memory update, observed
retroactively as the gap between 80.7%/15,841 and the 81.4%/15,983 baseline this session started
from), Kuwi/Kui inline-dialect-citation markers (matcher-side, +557), leading-qualifier-glued-
abbrev (parser-side, +75 validator / +124 corpus), headword-qualifier-after-lang-marker
(parser-side, +42 validator / +63 corpus), nested-tag-headword-bold-bcf (parser-side, +346
validator / +163 corpus), primary-dialect-marker-gloss-truncation (matcher-side, queue
-406 rows, no entry_match_rate change by design). `entry_match_rate` now **86.6%**,
`entries_matched` **17,003**. ALL big systemic matcher-side levers AND six parser-side levers
(Naiki, Ma-type nested-tag, Kuwi-type embedded forms, leading-qualifier,
headword-qualifier-after-lang-marker, nested-tag-headword-bold-bcf) are spent.

**Top next lead:** **To.-type embedded forms** (21 confirmed misses, e.g. Go. 5, Ka. 4) — see
below and [[burrow-parser-status-current-open-issues]]. Two fresh, smaller matcher-side leads
also open (found 2026-06-18 via DED 5440, not yet sized/fixed): Manda/Kui gloss over-extension,
and id.-elided dialect meanings — see below.

**Still unresolved (re-run orient to re-quantify before picking):**

*Parser-side (need corpus regen):*
- **Embedded secondary-language forms — To-type still open** (Kuwi-type closed via `6eacbae`,
  Ma-type closed via `af90534`, leading-qualifier-glued-abbrev closed via `32aeb26`). **To.-type**
  (precisely re-quantified 2026-06-18: **21 confirmed misses**, e.g. Go. 5, Ka. 4 — supersedes the
  earlier rough "~17, Tu. 5/Ka. 3" estimate) — abbrev buried mid-italic after lowercase text
  (`<i>incl.). To.</i>`), because the closing `</i>` lands after the abbreviation instead of
  before it AND no `<b>` immediately precedes the `<i>` (distinguishing it from the now-closed
  leading-qualifier pattern, which always has `<b>` or `<b>(` immediately before `<i>`). Confirmed
  concretely via DED 5154's raw HTML: `<b>em- <i>excl.</i>; am<-> <i>incl.). To.</i> em</b>`.
  Malformed source, likely rare/local — no fix shape precedent in this file yet. Also the original
  **`dakku`** case (DED 3014/Kannada, real form buried in Ka. gloss text).
- **Kui bare sense-suffix forms** (new lead, found 2026-06-18 via DED 5154) — forms tagged by
  grammatical sense with no dialect-abbreviation prefix, e.g. Kui's incl. form `āju we (incl.)`
  immediately following the excl. form/dialect citations in the same gloss, and Pengo's incl.
  forms `ās, āseŋ, āheŋ (incl.)` after the excl. form in DED 5154's Pe. attestation. Different
  mechanism than the dialect-citation-marker fix (`(X.) form` prefix) — here the form comes
  *first* and a bare `(excl.)`/`(incl.)` sense tag follows. Not yet built. Unquantified
  corpus-wide; only confirmed at DED 5154 so far.
- **Phase 2 single-char headword filter** — CLOSED 2026-08-18 (`fc3104b`) via `_VOWEL_HEADWORDS`
  whitelist (not the `hw.isalpha()` proposal — that would also readmit single consonants). +23
  vowel slots, 0 losses. See the 2026-08-18 batch entry above. DED 1's own Ta. (a *separate*
  `<i><b>Lang.</b></i>`+plain-text-headword gap) closed same session via Pattern G (`fae4cca`).

*Matcher-side (no corpus regen needed):*
- **Manda/Kui gloss over-extension** — Manda's case CLOSED via `periodless-language-gloss-
  boundary` (`6184828`, see above). `Kui`'s own case remains open for a DIFFERENT reason: its
  gloss is `", vejgu ( pl. veska) id."` — doesn't start with `"("` (a comma-prefixed headword-
  duplication artifact, distinct root cause), so it isn't touched by either of the two fixes
  above. Not sized corpus-wide; only confirmed at DED 5440.
- **`qualified-id-chain-resolution`** (systemic key) — `parser_bug_fixed` (2026-06-19, commit
  `0ac102d`). **Resolves the prior session's paused/uncommitted handoff.** Burrow glosses like
  `"(pl.) id."` (a grammatical/sense qualifier glued onto an "idem" reference) matched neither
  existing id.-resolution case (`g == "id."` / `g.startswith("id.")`), so the attestation showed
  a literal `"(qual) id."` placeholder instead of the chained-back meaning. Fixed with
  `_QUALIFIED_ID_RE` + a `_LEADING_PAREN_RE` strip (chained links like DED 5440 Konḍa→Pe.→Manḍ.
  each show `"(own qualifier) base meaning"` without accumulating prior links' qualifiers) in
  **both** `burrow_entry_parser.py` and `repair_burrow_corpus_glosses.py` (kept in sync). ~200
  attestations match the shape; **0 `(...) id.` residue corpus-wide** after the fix (e.g. DED
  5440 Manḍ. → `"(pl.) firewood, fuel."`, 5154 Pa. → `"(obl. am-) we."`, 3729 Tu. →
  `"(obl. nūta-) 100."`). Gloss-display-only: `entry_match_rate` unchanged **86.6%**,
  `entries_matched` 17,003, triage queue unchanged.

  **The prior session's blocking "3-case regression" (literal `"id."` at DED 5154/3729) was a
  phantom** — re-verified via a 3-stage pipeline trace (fresh parser → repair pass 1 → pass 2)
  plus a corpus-wide residue scan (0): the regression does not reproduce; the prior isolation
  experiment ran on an inconsistent code state. **A defensive pass-1 guard was prototyped and
  REJECTED**: stopping `_recover_attestation_gloss_from_full_text` from clobbering a resolved
  gloss with raw `"id."` text looked like a safe no-op but actually caused **real gloss data
  loss** — it returned the parser's 200-char-capped fallback in place of the full-text recovery
  whenever an attestation's primary form is `"id."` and its gloss exceeds the cap (DED 2237 Go.
  277→212, DED 3398 Te. 304→208 chars of lost dialect forms) **and** introduced 3 new
  `(...) id.` residue at the messy DED 178 entry. Reverted; shipped the original fix unchanged.
  **Lesson:** a "pure-robustness, zero-output-change" guard around `_recover` is not zero-change
  — it is entangled with pass-2 id-resolution and the parser's 200-char gloss cap.

  *Separate pre-existing lead found while verifying (NOT fixed):* the full-text recovery
  sometimes yields an unresolved `", putt-) id."`-style gloss that neither pass resolves (DED
  4344 Pa. → should be `"to know."`, 5516 Pa., 842 Pa., 4307 To., 4476 Br., 688 Ka.). Different
  root cause from the qualified-id shape; the rejected guard happened to fix these as a side
  effect. Worth a dedicated future pass.

- **`totype-embedded-marker`** (systemic key) — `parser_bug_fixed` (2026-06-19, commit `ee716f1`).
  Top open parser-side lead from prior sessions. A language abbrev buried at the END of an `<i>`
  span (after lowercase qualifier or scientific-name text), no `<b>`/`<b>(` right before the `<i>`
  — the `</i>` closes after the abbrev, so no anchored pattern could reach it and the whole
  language was dropped. Fixed parser-side: new `_STRICT_PATTERNS` list (one bold-headword
  To.-type regex) iterated after `_PATTERNS` in `_find_all_lang_spans`, gated by a NEW
  `_is_known_lang_abbrev` allow-list (essential — the pattern scans into an `<i>` span so it lacks
  the `<i>Abbrev` anchor; without the gate, italic citation titles/botanical authorities Volume/
  Sanskrit/Linn. were captured as bogus languages). **Read-only old-vs-new sim before shipping:
  +181 attestations / 134 DEDs, 0 losses** (Ma. 36, Ka. 24, Te. 17, Tu. 17, Go. 12, Kol. 10,
  Ko. 10, To. 9, …), 1 surviving-headword improvement (DED 2366 Ta.). Corpus regen +183 total,
  0 losses, qualified-id residue still 0. `entry_match_rate` 86.6% → **87.1%**, `entries_matched`
  17,003 → **17,098 (+95)**, not_attested 397→367, queue 4948→**4894 (−54)**. **Scoped to the
  bold-headword variant only**; the plain-text variant (~47 clean + ~7 English-gloss misparses,
  incl. DED 5154's OWN To.) is **deferred** pending a headword-quality guard — see
  [[burrow-parser-status-current-open-issues]]. The earlier "~21 To.-type misses" was a large
  undercount.

*Genuine divergences (log as `genuine_divergence`, NOT folds — never confirmed yet):*
- **Special-vowel residue** (quantified from the eng pass): `ɨ` U+0268 x339 (Toda/Kota central
  vowel), `ʔ` glottal x84, `ɫ` U+026B x41, `ʒ` x10. Real phonemic distinctions. Drill a few
  (e.g. Toda `twɨd_y`, `tǖs_`) to CONFIRM genuine vs. a further fold before logging.
- **Long-tail headword mismatches** — flat `row_count=2` groups (2883/Kannada, 399/Kodagu,
  4452/Telugu …) and **dialect mismatches** (3263/Parja Kuwi, 4587/Kuwi Israel, conf 0.95).
  Individual diacritic/orthographic divergences; no single dominant lever.

*Data-quality / deferred:*
- **Dedupe pass**: DED 410 (4 dup corpus entries, page 38) AND DED 4896 (2 dup copies). Both
  data-quality duplication independent of the matcher fixes.
- **Option C** (parser): single-letter qualifiers `P.`/`A.` (DED 2617, 1617) still captured by
  Pattern E — left for a structural fix.

**Lesson learned:** when a length/diacritic mark "should already be normalized" but isn't,
check whether it's a **standalone (non-combining) confusable** that the NFKD+strip-combining
path silently skips — and scan the corpus for the actual codepoint(s) before fixing, since
visually-identical glyphs (U+0387 vs U+00B7 here) can hide the dominant one. A literal glyph
typed into a probe/fix is unreliable — use explicit `\uXXXX` escapes.

---

## Appendix — 2026-08-18 → 08-20 tail, session log

Moved verbatim out of the frontmatter `description:` field on 2026-09-05, where it had grown to
24,701 characters (30% of this file) and broke recall. **Superseded on the three points listed
under FINAL STATE above** — in particular every rate in this log stops at 98.5% / 19,075.

"Status of the DED<->Starling cross-session triage ledger and queue (entry_match_rate **98.5%**; entries_matched **19,065** after 2026-08-20 **comma/slash multiform-delimiter fold** 7534128 [pushed, dravidilex-pilot; matcher-side/zero-regen; textnorm.normalize_for_match: extended the slash-whitespace-collapse regex to ALSO fold the top-level comma delimiter to '/' (\\s*/\\s* -> \\s*[,/]\\s*). Burrow joins the alternative stems of a multiform headword with '/' (irum/iṛum, sēlār/sēlāṛ) while Starling lists the same variants comma-separated (irum, iṛum), so the two notations never reconciled (Language-only). The fold runs AFTER in-paren commas are stripped so only genuine top-level form-separator commas are affected (tense parens (-pp-, -tt-) untouched); applied identically to both sides + order-preserving so merge-only. +4 all Language-only->Yes (19061->19065): DED 485 Muria + Chindwara Gondi (irum/iṛum, irup/iṛup), 2783 Betul Gondi (sēlār/sēlāṛ), 3748 Toda (nötš/nets̱ -- underscore vs combining-macron-below both NFKD to nets). No frozen 135, Language-only 158->154, 0 regressions. Ledger key comma-slash-delimiter-fold. FROM LANE-A shared-shape sweep of the 158 Language-only rows: this was the highest-count recoverable cluster; **INTRA-PAREN-SPACE-STRIP candidate ATTEMPTED & REVERTED (same session):** extending the in-paren comma-strip lambda to also .replace(' ','') recovered +5 (DED 1298x2 Muria/Maria kal (obl.kad-,pl.kalk) vs (obl. kad-, pl. kalk); 5153 Inscr.Telugu ēṇḍu (gen.ēṇṭi); 5154 Toda om (obl.om-); 5552 Kui vau (pl.vanga) -- all differ only by intra-paren spacing) BUT tripped +1 Yes->LO regression on DED 410 Chanda Gondi 'ēr (pl. ērk)' whose base Yes(substring,0.95) was a SPURIOUS match on the bare plural fragment 'pl. ēṛ' from a DIFFERENT dialect's form ('ēl (obl. ēn-), pl. ēṛ'); stripping Starling's '(pl. ērk)'->pl.erk dissolved that coincidental substring. Reverted per strict 0-regression rule (user's call). Attribution PROVED by 3-way validator diff (session-start BASE / comma-slash-only / both): comma-slash 0-regression clean, paren-space owns the sole regression. paren-space was SUPERSEDED later this session by the despaced-exact fix (below) which recovers the same DED 1298 rows via safe full-string exact-equality (no substring, so no 410 Chanda regression).** THREE MORE matcher fixes shipped this session, closing ALL sweep candidates (match rate now 98.5%, entries_matched 19,075, Language-only 144, No 135): (2) **split-token atom-exact** 384d8dd (_match_entry accepts a match when any '/'-atom of the Starling form exactly equals a Burrow token atom; Burrow SPLITS multiform hw into single tokens Ma. 'a, ā'->['a','ā'] the ≥2 length guard blocks; +3: DED 1+410 Ma 'a, ā'/'i, ī', 4572 Ta 'pō'; ledger split-token-atom-exact-match); (3) **'<->' scrape-artifact strip** 43259ca (normalize_for_match_variants strips \\s*<->\\s* before the infix search; +1 DED 5372 Te 'bratuku'; ledger arrow-artifact-strip; display artifact remains in 3 baked corpus hw, parser could clean later); (4) **despaced-exact** d166c4f (_match_entry accepts when the two normalized forms are equal with ALL spaces removed, full-string equality ONLY never substring; +6: DED 2331 Kuwi 'sap ta', 5151 Malto 'nére(h)', 1298 Muria+Maria, 4345 'pu · f'+4932 '-mil muṭy' Toda; ledger despaced-exact-match). Each verified by immediate before/after row-level diff, 0 regressions. **ALL recoverable candidates CLOSED. GENUINE-DIVERGENCE LOGGING DONE (2026-08-20):** the 144 residual classified into 6 categories (worksheet data/dravidian/cross-validating-dded-starling/tree_validation_output/genuine_divergence_worksheet.csv, gitignored); **132 logged language-scoped genuine_divergence** (review_ledger.json, gitignored, nothing to commit): spelling/phonological 76, distinct-reflex 40, morphological-citation (Gondi -ānā/Kuwi -ali vs bare stem) 13, Inscr.-Telugu ṛ/ḍ 3; grouped shared note per category, tag [Lane-A grouped genuine-divergence pass], reviewed_by=claude-triage; DED 410 now mixed whole-DED(parser_bug_fixed)+per-lang(genuine); ledger 142 genuine_divergence total (incl. pre-existing 9 Irula + 1 Adilabad-495). **HELD (not logged): 11 display_fallback_or_postposed rows** (matcher shows English gloss as 'form' since no hw matched: huppe→field-rat, kētul→hut, guḍḍī→tomb/temple, gumiya→grave, kohk-→thresh, vāŋg-→leak, eɵy→e) -- several are POSTPOSED-inline-marker cases (`huppe (M.)` after the form in Go. gloss) the matcher can't reach = OPEN LEAD (postposed-marker forward-anchored lever could recover some; 2026-08-19 muria backward-look attempt was reverted as fragile). **NEXT: postposed-inline-marker lever for the 11 held rows, or done.**]. Prior **98.5%**; entries_matched **19,061** after 2026-08-20 **Tamil comma-for-period marker** 766011a [pushed, dravidilex-pilot; **PARSER, CORPUS REGEN'D -- RE-SYNC TO BOX**; burrow_entry_parser.py new _COMMA_MARKER_PATTERN + gated loop for '<b><i>Ta</i>, par̤i (-pp-, -tt-)</b>' (DED 4002) where the abbrev terminal period is OCR'd as a comma OUTSIDE the italic. Pattern A's mandatory </i>\\s+ is a DELIBERATE guard (blocks '<i>SciName</i>; word' from bogus-language capture) so it can't admit the comma; SEPARATE pattern captures the period-less abbrev, loop reconstructs the period + gates on _is_known_lang_abbrev('Ta.') -- sci-name 'Artocarpus.' not known -> rejected, no FP hole. Isolated diff EXACTLY 1 entry (4002), +Ta.x1, 0 losses, no bogus langs. +1 matched (19060->19061), No 136->135, Language-only frozen 158, 0 regressions. Ledger key tamil-comma-for-period-marker. **SCATTERED-PARSER TAIL NOW CLOSED for this session** -- user's call: did the 6 clean/safe ones (2690/3918/1563/5006/4143/4002 + Naiki 75/1623/430 earlier), and DEFERRED the 5 remaining hard rows to manual Excel edit or genuine_divergence logging (4473 Malto Malt;-typo needs global _LANG_ABBREV change; 3535 Konda citation-in-italic needs sci-name lowercase-guard relaxation; 379 Muria/Go segment miss; 4885 Konda <b>-swallows-across-<i>Pe.</i>; 2116 Naiki plain-text-hw-after-qualified-marker -- each 1 row, high risk/complex). **NEXT: per-row genuine_divergence logging of ~46-lang Language-only spread (158), or user manually fixes Excel.**]). Prior **98.5%**; entries_matched **19,060** after 2026-08-20 **4 non-canonical-markup parser one-offs** 6f20bd2 [pushed, dravidilex-pilot; **PARSER, CORPUS REGEN'D -- RE-SYNC TO BOX**; burrow_entry_parser.py + dialect_mapping.py; from unmatched_diagnostics.md scattered-parser-tail. (1) NEW Pattern CQ '<i><b>Lang.</b> (biblio-qual)</i> <b>hw</b>' -- citation AFTER the bold abbrev but INSIDE the italic; Pattern C needs </b></i> immediately + CIT handles the (-opens-inside-bold inverse, neither fired (DED 1563 Tu. girige, 5006 Ta. muṟaḷai); GENERALIZES to Ma./Ka./Te. dict citations. (2) _OPT_HEADWORD_QUALIFIER now skips a bare '?' uncertainty mark between marker+headword (DED 4143 '<i><b>Tu.</b></i> ? <b>pēñci</b>'). (3) Pattern C tolerates abbrev trailing period OUTSIDE the bold '</b>\\.?</i>' (DED 3918 '<i><b>Koḍ</b>.</i>', via existing 'Koḍ' alias). (4) _ABBREV_ALIASES += 'Koḏ.'(macron-below-d U+1E0F)->'Koḍ.' so existing To.-type sci-name strict shape(2) surfaces DED 2690 '<b><i>L. vulgaris. Koḏ.</i> tore</b>'. Isolated regen diff: 15 entries changed, 0 losses, +18 attest (Tu.5/Ma.4/Ka.3/Te.2/Ta.2/Koḏ.1/Koḍ1 -- 10 BONUS beyond the 5 diagnosed). Match rate 98.4->98.5%, +7 matched (19053->19060), No 143->136, Language-only frozen 158, 0 regressions. Ledger key parser-oneoff-noncanonical-markup-batch. Recovered DED 2690/3918 Kodagu, 1563/4143 Tulu, 5006 Tamil. **REMAINING scattered-parser tail (still-No, in progress): 4002 Ta (<i>Ta</i>, comma-marker), 379 Muria/Go (Go segment miss + (Mu. Elwin)), 3535 Konda (citation LSI) before abbrev in <i>, no lowercase for To.-type guard), 4885 Konda ((BB) -- needs inspect), 2116 Naiki (plain-text headword after <i><b>Nk. (Ch.)</b></i> + (LSI 4.572)), 4473 Malto (Malt; semicolon typo).**]). Prior **98.4%**; entries_matched **19,053** after 2026-08-20 **Naiki (Ch.) between-marker-qualifier relabel** 5b4b16d [pushed, dravidilex-pilot; **PARSER, CORPUS REGEN'D -- RE-SYNC TO BOX**; burrow_entry_parser._find_all_lang_spans (main _PATTERNS loop): fold a dialect qualifier sitting BETWEEN the language marker and the headword (OUTSIDE the <i> marker) back into the abbrev when _is_known_qualified_abbrev(composite) is True. Shape '<i><b>Nk.</b></i> (Ch.) <b>aṛka</b>' (DED 75/430) / '<i>Nk.</i> (Ch.) <b>khīr</b>' (1623): _OPT_HEADWORD_QUALIFIER consumed+discarded '(Ch.)', so 'Nk. (Ch.)'=Naiki was stored as bare 'Nk.'=Naikri (WRONG dialect; form parsed correctly). Guard is airtight -- normalize_burrow test showed Nk.(Ch.) is the ONLY recognised between-marker composite; all other same-position tags (Go. (Tr.), Ga. (S.), Kuwi (F.), Te. (DCV), Konḍa (BB), (LSI 4.572)) return unchanged so are left alone. Isolated regen diff (before-naiki backup vs new cleaned): EXACTLY 4 entries relabeled Nk.->Nk.(Ch.), 0 content loss, total attest 29138 unchanged. +3 matched (19050->19053), No 146->143 (these 3 were Match=No not Lang-only), Language-only frozen 158, 0 regressions. Ledger key naiki-ch-between-marker-qualifier. Source: unmatched_diagnostics.md Naiki group (proposed fix followed ~verbatim). **DIAGNOSTICS FILE now ~worked out: verified ~35 of its parser_miss rows -- most already matched by shipped plaintext-marker/sci-name/multi-Go fixes; remaining still-No parser_miss = scattered one-off HTML shapes needing regen, no shared lever: Tulu 1563/4143 (reversed <i><b> nesting + biblio/? qualifier), Kodagu 2690 (macron-below Koḏ.+sci-name)/3918 (period-outside-bold <i><b>Koḍ</b>.</i>), Konda 3535/4885 (plain-text w/ source paren), Malto 4473 (Malt; semicolon typo), Tamil 5006 ((DCV) leading qualifier)/4002 (Ta , comma-marker), Muria 379 (Go. (Mu. Elwin) segment miss). Naiki 2116 = leading-position variant (harder). NEXT: per-row genuine_divergence LOGGING of the ~46-language Language-only spread, or pick off the scattered parser one-offs.**]). Prior **98.4%**; entries_matched **19,050** after 2026-08-20 **unterminated-paren comma-strip** 42edaaa [pushed, dravidilex-pilot; matcher-side/zero-regen; textnorm.normalize_for_match: made the closing ')' OPTIONAL in the intra-parenthetical comma-strip (line ~168) so a truncated headword with a DANGLING '(' is handled. Burrow's DSAL markup sometimes closes the bold span mid-parenthetical ('<b>āgu (ān-, āy-</b>, etc.)') so the parser stores a truncated form with an unbalanced open paren ('āgu (ān-, āy-'); its internal comma previously survived normalization and broke the substring match vs Starling's balanced 'āgu (ān-, āy-, etc.), agu'. Balanced parens byte-identical ([^()]* can't cross ')'). +7 all Lang-only->Yes (19043->19050), No frozen 146, Language-only 165->158, 0 regressions. Ledger key unterminated-paren-comma-strip. Recovered DED 333/530 Kannada, 4687 Kodagu, 4778 Brahui, 4572 Kodagu, 2781 Ollari Gadba, 3098 Tamil. Method: sized the lever by counting unmatched rows whose 'Matched Burrow form' has more '(' than ')' (12 rows), 8 were the paren-comma-survival shape, 7 flipped (4572 Tamil didn't: its STORED form has a gloss leaked into the headword -- separate parser artifact). Residual genuine divergences among the 12: 4968 Durg (molol/malōl vowel), 3655 Kota/Toda (Burrow truncated to bare 'n' + Starling [...] bracket notation), 5270 Durg (vā-/waiānā), 4572 Tamil (gloss-leak). **Systemic-fold phase confirmed ~exhausted -- Language-only bucket now spread across ~46 languages with no concentration; remaining are mostly genuine divergences -> row-by-row genuine_divergence LOGGING.**]). Prior **98.341%**; entries_matched **19,033** after 2026-08-20 **TWO-FIX Adilabad session** on dravidilex-pilot [both pushed]. (2) **200-char gloss-cap removal** a866dbe [**PARSER, CORPUS REGEN'D — RE-SYNC TO BOX**; burrow_entry_parser.parse_language_sections dropped the gloss=gloss_text[:200] cap. _extract_gloss already bounds the gloss at the next lang marker / DED(S) ref, so the cap only chopped legit in-span content — inline sub-dialect forms past 200 chars (DED 513 (ASu.) ḍiyyōr) were unreachable by the matcher's gloss extractor. Regen: attestation counts IDENTICAL 30,334; 252 glosses grew, 1 shrank (DED 5202 Koḍ. 199->3='id.', a CORRECTNESS GAIN — full antecedent now correctly seen as multiform so honest literal 'id.' kept vs gluing wrong Ka. meaning, per A′ policy). +12 incl. 513, 19,021->19,033, No frozen 146, 0 regressions. Ledger key gloss-200char-cap-removal. 112 attestations had been hitting the cap]. (1) **ASu. Adilabad-Su. inline sigil** 1f06a1a [matcher-side/zero-regen; GONDI_INLINE_ABBREVS gained 'ASu.'->Adilabad Gondi, the (ASu.) counterpart to the shipped SR. fix a3de7c0. Burrow tags Adilabad forms (ASu.)=Adilabad Su. inline in the consolidated Go. gloss (170 occ corpus-wide, always Gondi context, distinct token from bare Su. which stays skipped for the Koya collision). Adilabad Language-only 22->5, +17 all Lang-only->Yes, 19,004->19,021, No frozen 146, 0 regressions. Ledger key adilabad-ASu-inline-sigil]. **RESIDUAL after BOTH fixes: Adilabad Language-only 22->4 (513 now Yes via gloss-cap) — 495 velkī genuine_divergence (LOGGED per-language, absent from Burrow: Gondi has only (Ma.)lēki/(M.)leke/(LuS.)lèkee), 1850 gudd- + 410 vēr/isar gunḍi both blocked by NEXT LEAD = multi-Go-segment/best_att-only scan (DED 1850 is 3 corpus entries, (ASu.)gudd- in section-b not scanned; also blocks Muria 4083 + Koya 990; matcher should iterate ALL Go. attestations, not just best_att) + 410 additionally the known dup-entry issue + multi-word isar gunḍi.** Prior **98.2%**; entries_matched **19,004** after 2026-08-20 **Konda glottal-modifier-trailing-space fix** 9b45580 [pushed, dravidilex-pilot; matcher-side/zero-regen; textnorm.normalize_for_match: after GLOTTAL_FOLD reduces glottals to ʔ sentinel, re.sub('ʔ +','ʔ') drops a space immediately after it. Burrow's SOURCE HTML typesets the modifier glottal as its own unit + trailing space (Konda 'muˀ er','loˀ i','riˀ -/ri-' -- VERIFIED in raw_html, NOT a parser artifact so no regen/no display defect) vs Starling tight 'muʔer'/'loʔi'. Both-sided so merge-only. +8 matched (Konda 5 + Kuwi-Israel 1 + Konda-Burrow/Bhattacharya 1 + Manda 1, all Lang-only->Yes), No frozen 146, 18996->19004, 0 regressions. Ledger key glottal-modifier-trailing-space. Konda Lang-only 7->2, residual genuine (2617 sītel dubu compound, 4205 pir_a mundi/pite). **METHOD INFLECTION (2026-08-20): scan of next 4 groups (Muria 10/Kuwi-Fitz 8/Konda 7/Inscr.Telugu 7) found 3 of 4 = genuine-divergence TAIL (Muria=diff Gondi-dialect forms in consolidated Go.; Kuwi-Fitz=-ali infinitive vs verb root; Inscr.Telugu=ṛ/ḍ dialect corresp, UNSAFE to fold since contrastive), only Konda systemic. Systemic-fold phase now ~exhausted; remaining Language-only bucket (~204) is mostly genuine -> switch to row-by-row genuine_divergence LOGGING, grouping only to detect any last shared shape.**]. Prior **98.2%**; **18,996** after 2026-08-20 **Kui derivation-paren + HTML-entity fix** 99a1b69 [pushed, dravidilex-pilot; TWO-PART. (1) matcher-side textnorm.normalize_for_match: html.unescape + drop any parenthetical beginning with derived-from arrow '(< ...)' -- etymology metadata Starling carries identically, so match keys on headword proper + spacing diff around arrow ((<kūkp- vs (< kūkp-) no longer blocks. (2) **parser-side burrow_entry_parser, CORPUS REGEN'D -- RE-SYNC TO BOX**: headwords are regex-sliced from str(blockquote) which PRESERVES html entities unlike the gloss get_text path, so html.unescape each headword at construction; decoded 18 stored headwords '(&lt; ...)'->'(< ...)' (Kui 13, Te 2, Pa/Kuwi/Pe), display-only fix, 0 attestation losses/0 structural change in regen. Kui Language-only 12->6, +7 matched (Kui 6 + Pengo 1, all Lang-only->Yes), No frozen 146, 18989->18996, 0 regressions. Ledger key kui-derivation-paren-html-entity. Residual 6 Kui = genuine (3 gloss-as-headword 'hoe'/'a kind of bee'/'to draw water...', 3 diff forms: ī vs ianju/iaru/īri, (W.) dialect variant ḍrāḍu vs grāḍu, vau vs vaspa). Method: Kui 12 Language-only lead, shape table -> Pattern A (6 deriv-paren) vs B (3 gloss-headword, genuine) vs C (3 diff, genuine). Next Language-only leads: Muria Gondi 10, Kuwi(Fitz) 8, Konda 7, Inscr.Telugu 7]. Prior **98.1%**; **18,989** after 2026-08-20 **Telugu optional-infix-paren expansion** 06482a6 [pushed, dravidilex-pilot; matcher-side/zero-regen; textnorm.py normalize_for_match_variants + _OPTIONAL_INFIX_RE, wired into starling_tree_validator._match_entry direct-headword compare. Burrow writes a variant pair compactly as an INFIX parenthetical X(Y)Z = {XZ, XYZ} (k(r)ovvu={kovvu,krovvu}, pur(u)gu, tal(l)i, ven(n)u, esp. Telugu); Starling lists ONE resolution (sometimes dropped, sometimes kept). normalize_for_match alone flattened parens to spaces ('k r ovvu') and matched neither; now compares every resolution's key while still REPORTING Burrow's published k(r)ovvu form (display-faithful). Guard narrow -- infix flanked by word chars, 1-3 non-space chars -- skips tense parens (-pp- -tt-), obl. notes, trailing sigils (B). Telugu Language-only 41->7, +37 all Lang-only->Yes (36 Telugu + 1 Telugu-Krishnamurti), No frozen 146, 18952->18989, 0 regressions. Ledger key telugu-optional-infix-paren. Residual 7 Telugu = genuine (ā/ī/ē demonstrative bases vs vāḍu/vīḍu/evaḍu, bratuku/braduku d-t, two (?)-uncertain forms, turugu/tuṭṭe). Method: Telugu Language-only lead, char/shape table over the 41 rows -- ~34 were the paren pattern. Next Language-only leads: check remaining bucket (219 total) for next concentration]. Prior **97.9%**; **18,952** after 2026-08-20 **Toda consonant-fold** f8ab3fc [pushed, dravidilex-pilot; matcher-side/zero-regen; textnorm.py TODA_CONSONANT_FOLD: Starling ɫ U+026B + Burrow ł U+0142 -> l, Starling ʒ U+0292 ezh + Burrow ζ U+03B6 Greek-zeta -> z. Starling's single ɫ maps to Burrow ł/ḷ/plain-l and ʒ to ζ/plain-z so plain l/z is the only common target; vowel-length macron-vs-middle-dot was ALREADY reconciled by LENGTH_DOTS. Each char 100% Toda-confined per dataset (ɫ/ʒ Starling-only, ł/ζ Burrow-only) so the 1:1 folds only merge Toda, never split a match. Toda Language-only 57->9, +48 all Toda->Yes, No frozen 146, 18904->18952. Residual 9 Toda = genuine (om/em) + barred-o ɵ (poɵan/poqan, separate fold candidate ɵ↔q) + tes/tik + pūf spacing. Ledger key toda-consonant-fold. Method: found via Toda Language-only lead from prior session, char-correspondence table over the 57 rows. Next Language-only lead: Telugu 41]. Prior **97.7%**; **18,904** after 2026-08-19 **Adilabad-Gondi SR-sigil fix** a3de7c0 [pushed, dravidilex-pilot; matcher-side/zero-regen; GONDI_INLINE_ABBREVS gained 'SR.'->Adilabad Gondi -- Burrow tags Adilabad forms '(SR.)' in the consolidated Go. gloss (DED 910 '(SR.) yeḍung'), previously fuzzy-matched base Go. primary as Language-only; Adilabad Language-only 62->22, +40 matched 18864->18904, 0 regressions; Su. deliberately excluded (0 gain, collides with Koya's '(Koya Su.)'); the 22 residual Adilabad rows are genuine divergences not SR-tagged in Burrow. Method: triage_mismatches inflated queue is mostly already-matched 'issue' rows -- the real unmatched set = Match No (146) + Language only (304); pattern-hunt within THAT. Next concentrated Language-only leads: Toda 57, Telugu 41]. Prior **97.5%**; **18,864** after 2026-08-19 diagnostics-driven batch on dravidilex-pilot [**8 commits, pushed**, from unmatched_diagnostics.md; +95 matched 18,769->18,864, 0 net regressions shipped]. Commits 5-8 (after the first 4 below): (e) c799d51 sciname-before-marker-bold-abbrev (**parser, CORPUS REGEN'D**: 4th _STRICT_PATTERNS entry for '<i>P. quadrifida. <b>Ma.</b></i> <b>hw</b>' bolded-abbrev-inside-italic-sciname; +19 attest/12 entries 0 lost, +7 matched, 0 Yes->No; closes sciname-plain-before-boldlang for Ma/Tu/Ka/Pa); (f) 791c65b koya-spelled-out-inline-marker (matcher: gloss_extraction._DIALECT_MARKER_GROUP_RE +optional leading 'Koya '; dialect_mapping 'Koya.'->Koya Gondi; +25 ALL Koya Gondi, 0 Yes->No; recovers '(Koya Su.)'/'(Koya T.)'); (g) ef0538b muria-east-west-inline-labels (matcher: GONDI_INLINE_ABBREVS MuE./MuW.->Muria Gondi; +3, 0 Yes->No); (h) 37bb56b kuwi-diacritic-and-page-inline-markers (matcher: _MARKER_ABBREV_TOKEN admits diacritic letters + 'p. NNN' page tail so '(Ṭ. Isr.)'/'(Isr. p. 127)' pass the gate; +5 Kuwi Israel 228/4010/4275 + Parja Kuwi 837/4711, 0 Yes->No). **User chose matcher-side Kuwi over the STRUCTURAL SPLIT of (F.)/(S.)/(Su.)/(Isr.) [reversed the earlier 'do 2'] since the existing KUWI_INLINE_ABBREVS extraction already works.** Also **logged 9 Irula rows as genuine_divergence** (per-DED --language Irula: 1572/410/524/530/445/555/2559/2625/5259 -- Burrow published no Ir. reflex, tier-3 documentation). **TWO REVERTED attempts** (both regressed, confirming extract_gloss_forms_for_abbrevs fragility): muria postposed-marker diacritic-backward-look (8 Yes->No, grabbed a preceding form of a DIFFERENT dialect) + kuwi paradigm-paren-attach '(āt-)' onto form (DED 333 +4 but regressed DED 4325 Maria Gondi -- lengthening a form breaks a substring match where Burrow's ˀ-paren spacing differs from Starling ʔ; the memory's 'never lengthen the form' hazard). Backups in data/dravidian/cross-validating-dded-starling/tree_validation_output/: results.before-{koya,muria,kuwimarker,parenattach}.csv + before-{plaintext,sciname4}. Commits 1-4 [97.3%/18,824]: (a) df1349f paren-comma-strip (textnorm, matcher-side/zero-regen: strip commas INSIDE parentheticals only so Starling (-pp-, -tt-) == Burrow (-pp- -tt-), preserving top-level comma headword lists per import commit 703f775; +5; Tamil 34/905/1588/4922 + Kodagu 2654/4027/587); (b) bb89170 abbrev-alias-trailing-period-variants (dialect_mapping _ABBREV_ALIASES: Kui.->Kui, Kod.->Koḍ., Konḏa.->Konḍa, Ko..->Ko.; +3; 3684 lang-resolved but headword-blocked); (c) 52ac72a slash-spacing + IPA_VOWEL_FOLD (textnorm: collapse space around '/' + ǝ U+01DD->ə schwa / ɔ U+0254->o; matcher-side; Naiki 1208/1291, Kota 1369, Kui 3790, Telugu 3793, Kannada 3480/190/3498); combined (b)+(c) validator +13 = 97.0->97.1; (d) cf29542 plaintext-running-text-language-markers (**parser, CORPUS REGEN'D - re-sync to Box**: new _PLAINTEXT_MARKER_PATTERNS for untagged markers 'Tu. <b>hw</b>' / '<b>Tu. hw</b>' glued into prior gloss, gated by (?<=\\.\\s) boundary + _is_known_lang_abbrev + 2 false-pos guards _plaintext_marker_is_crossref [Cf./cf./see/s.v./esp./viz./under/e.g./=] and _plaintext_marker_in_parenthetical [open-paren sub-forms/etym notes/Gondi-qualifier mislabel]; +65 attestations/55 entries, 0 lost; row-level diff 0 Yes->No, +37 No->Yes; 97.1->97.3; Tulu/Konda/Malayalam/Malto/Parji/Kodagu). Backups burrow_corpus.cleaned.before-plaintext.json + tree_validation_summary.before-plaintext.json. Prior 97.0%/18,769 after 2026-08-18 category-A abbrev/base-name batch 178ed8d (matcher-side dialect_mapping.py, zero-regen, found via --primary language triage; new _ABBREV_ALIASES: plain-d Mand. DED34->Manḍ./Manda, period-less Koḍ DED215/2826/5297->Koḍ./Kodagu; Ir. base Iruḷa->Irula since Starling uses plain-l Irula 33x/retroflex 0x, DEDR Iruḷa kept as baked corpus display; +6 matched, total 19,354 frozen = 0 regression; ledger key abbrev-orthography-variants); prior 96.9%/18,763: spacing-diacritic-strip 872da4f (SPACING_DIACRITICS strips ˜ U+02DC / ˘ U+02D8; +23 Telugu/Kurukh/etc; 0 regression) + glottal-stop-fold 852c9dc (GLOTTAL_FOLD ʔ/ˀ/apostrophe->ʔ in normalize_for_match; matcher-side/zero-regen; Kurukh 26->6, Kuwi-Israel 28->10, Maria-Gondi 15->6; +67, 0 regression) + hyphen-normalization fix 630f65a (normalize_for_match: delete internal citation hyphen instead of spacing it; matcher-side/zero-regen; Brahui Language-only 107->5, +115 matches, No/N-A frozen = 0 regression; language-group triage found it) + subgroup-DB orphan exclusion ad5da93 (653 no-DED N/A rows all from non-DED-keyed subgroup DBs cross-linked from dravet: gndet 635 Konda + kuiet/telet/ndret/braet/kogaet/gonet; verified on live Starling pages; validator gates orphan-reporting on _url basename, counts in summary.subgroup_db_orphans_excluded; rate unchanged/never in denominator; N/A queue 653->0); prior 95.9%/18,558 language-shape 4-lever batch 1f9bf85 (Pattern C3 + citation-glued + sci-name bold-order + sci-name running-text w/ lexical guard; +110 attestations, +43 matches, CORPUS REGEN'D - re-sync to Box); prior 95.7%/18,515 leading-qualifier-<i><b> 57e038d, 95.4%/18,473 To.-type bold-embedded 44287fc, 95.1%/18,410 bare-letter-DED-suffix 02b1fa2, 95.0%/18,381 zh-fold+Pattern-G+vowel-headword fae4cca/fc3104b); user now drives manual triage, Claude assists on request"
