# Dravidian Validator — Cross-Validation Progress Report

**Scope:** Burrow DED ↔ StarlingDB cross-validation (`src/dravidian/scripts/cross-validating-dded-starling/`).
**Period:** 2026-06-18 → 2026-08-21 (pilot frozen for the AsiaLex submission).
**Final headline:** `entry_match_rate` **69.9% → 98.6%**; `entries_matched` **~13,733 → 19,083** of
**19,354** in-scope Starling entries carrying a DED number. Residual **271** rows:
**136 `Language only`**, **135 `No`**. Source of truth: `data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_summary.json`
(2026-08-21, tracked in git).

> ### Read this number with its three caveats
>
> **1. The denominator is scoped.** 19,354 is the *in-scope* population. The validator excludes
> **653** orphan rows from non-DED-keyed subgroup databases (`ad5da93`; gndet 635, kuiet 5, telet 4,
> ndret 4, braet 3, kogaet 1, gonet 1) that carry no DED number to match on. Against the full
> **20,007**-row scrape the same 19,083 matches are **95.4%**. Never cite 98.6% without stating the
> exclusion.
>
> **2. It measures agreement, not accuracy.** This is headword agreement between two sources *after
> normalization* — not correctness against a gold standard. A large share of the 69.9% → 98.6%
> climb comes from matcher-side folds this project introduced (underscore, length-dot, eng, glottal,
> spacing-diacritic, despacing, optional-infix expansion). The `Side` column in §1 and §10
> separates those from parser-side recoveries, which change what is actually extracted from Burrow.
>
> **3. The residual is adjudicated, not matched.** The 136 `Language only` rows are logged in
> `review_ledger.json` as `genuine_divergence` — real transcription/dialect variation rather than
> matcher gaps. **135 of the 147 language-level ledger records carry `reviewed_by: claude-triage`**:
> they were classified by an AI triage pass on 2026-08-20 and have had **no human spot-check**. They
> are deliberately *not* counted in the 19,083, and must not be folded into a published rate.

The validator (`starling_tree_validator.py`) walks each StarlingDB etymon tree, resolves the
DED number on each branch, and compares Starling's per-language headwords against the
attestations parsed from the Burrow corpus (`burrow_entry_parser.py` →
`burrow_corpus.cleaned.json`). Each fix below was triaged through the `/triage-ded` workflow:
one diagnosis → one fix → regen/revalidate → ledger entry → commit. Findings are recorded in
`data/dravidian/burrow_ded/review_ledger.json` (gitignored).

---

## 1. Match-rate progression

Each row is one shipped fix, in chronological (commit) order. "Side" distinguishes a
**matcher** fix (`starling_tree_validator.py` only — changes how forms are compared, no corpus
regen) from a **parser** fix (`burrow_entry_parser.py` — changes what gets extracted, requires
a corpus regen).

| # | Fix (ledger key) | Commit | Side | Rate before → after | Δ matched |
|---|---|---|---|---|---|
| 1 | DED-0 sentinel | `9d3acb0` | matcher | 69.9% → 70.0% | small |
| 2 | Pattern E false positives | `99b6206` | parser | — (quality only) | 0 |
| 3 | Sub-entry markers ("Tamil missing": 990/137/2674/410) | `7f8a958` | parser | 70.0% → 70.3% | +60 |
| 4 | Underscore normalization | `f272c3f` | matcher | 70.3% → 73.3% | +590 |
| 5 | Length-dot normalization | `128ca03` | matcher | 73.3% → 76.4% | +621 |
| 6 | Eng (ŋ) velar-nasal normalization | `e1250f2` | matcher | 76.4% → 77.6% | +222 |
| 7 | DED letter-suffix (4896) | `e6346ef` | matcher | 77.6% → 77.6% | +9 |
| 8 | Naiki dialect-qualified abbrev (`Nk. (Ch.)`) | `f29ec78` | parser | 77.6% → 78.7% | +214 |
| 9 | Nested tag in headword bold (Ma-type) | `af90534` | parser | 78.7% → **80.7%** | +392 |

**Session split:** rows 1–6 were the prior session; rows 7–9 were the current session.

---

## 2. Fix details

### Session 1 (rows 1–6)

#### 1. DED-0 sentinel — `9d3acb0` (matcher)
StarlingDB uses a literal `"Number in DED": "0"` as its own sentinel for "no Burrow
correspondence" (9 occurrences). `_clean_ded_number` treated `"0"` as a real DED, so the
validator looked it up in Burrow (which numbers from 1) and emitted false "DED 0 not found"
mismatches. Fixed by normalizing `"0" → None`. `ded_not_in_corpus` branches 26 → 17. Also
shipped the `/triage-ded` tooling (`triage_mismatches.py`, `inspect_ded_entry.py`,
`review_ledger.py`).

#### 2. Pattern E false positives — `99b6206` (parser, quality)
Pattern E (plain-text headwords) was picking up capitalized non-language tokens (text titles,
botanical genera, bibliographic abbreviations). `_is_valid_lang` now strips trailing `.`/`)`/`(`,
rejects cleaned tokens >10 chars, and `_INVALID_LANG_ABBREVS` was extended (Language, Gramm,
Divy, Sanskrit/Old-Kannada titles, ~12 botanical genera). 0 attestations lost; false positives
removed.

#### 3. Sub-entry markers / "Tamil missing" — `7f8a958` (parser)
DSAL HTML glues a lettered sub-entry marker onto the sub-entry-initial language inside one
`<i>` tag (e.g. `<b><i>(a) Ta.</i> oru</b>`). `_LANG_CHAR` must start uppercase, so every
pattern skipped these spans — dropping mostly **Tamil** (196 of 246 glued spans across 77 DEDs).
Fixed with a non-capturing `_OPT_SUBENTRY` prefix in all four patterns. Reparse: 505 entries
changed, 0 losses, Tamil gained on 69 DEDs.

#### 4. Underscore normalization — `f272c3f` (matcher)
Starling encodes diacritics in ASCII with a trailing underscore (`in_r_u` = Burrow `iṉṟu`).
`_normalize_for_match` stripped diacritics via NFKD but left underscores, so the two sides never
reconciled. Added `.replace("_", "")` (in both copies of `_normalize_for_match`). 0/34,875 Burrow
headwords contain underscores, so strictly additive on the Starling side. **+590** across ~299 DEDs.

#### 5. Length-dot normalization — `128ca03` (matcher)
Burrow marks vowel length with a raised dot after the vowel (`te·l` = `tēl`); Starling uses a
macron, already removed by NFKD. The dots are standalone (non-combining) characters, so they
survived. **Two visually-identical confusables** occur — U+0387 (dominant, 1,071) and U+00B7 (3)
— both folded via a `_LENGTH_DOTS` translate table (+ IPA length marks). Matcher-side, symmetric,
no regressions. **+621** (biggest matcher lever).

#### 6. Eng (ŋ) velar-nasal normalization — `e1250f2` (matcher)
Starling writes /ŋ/ as eng (U+014B); Burrow uses `ṅ` (U+1E45 → NFKD `n`). Eng has no
decomposition, so it survived. Folded `ŋ/Ŋ → n` via `_ENG_FOLD` — extending the collapse NFKD
already applies to `ṅ`. **+222.**

### Session 2 (rows 7–9, current session)

#### 7. DED letter-suffix (4896) — `e6346ef` (matcher)
Burrow's DDSA page numbers DEDR split-entry 4896 as `4896(a)` (and mislabels its (b) sub-entry
the same, yielding two corpus copies). The validator's `_clean_ded_number` left `4896(a)` intact
(int-parse fails), so Starling's lookup for plain `4896` missed → all 12 rows fell to "DED 4896
not found". Strip a trailing parenthetical letter (`^(\d+)\s*\([a-z]\)$` → base) so
`4896(a)`/`4896(b)` index under `4896`; both copies fold into one paragraph via `setdefault`.
Only letter-suffixed entry in the 5,685-entry corpus. **+9**, `ded_not_in_corpus` 17 → 10.

#### 8. Naiki dialect-qualified abbrev — `f29ec78` (parser)
The parser dropped **every** Naiki attestation. Burrow marks Naiki as `Nk. (Ch.)` (vs. Naikri
`Nk.`), but `_LANG_CHAR` forbids the internal space, so all four patterns stopped the abbrev at
`Nk.` and failed on the ` (Ch.)` — ~314 markers in all three wrapper forms. Added an optional
in-marker qualifier (`_OPT_LANG_QUALIFIER` / shared `_LANG_ABBREV`); `_clean_lang_abbrev` keeps
the qualifier only when the inventory recognizes the full form (`Nk. (Ch.)` = Naiki), else strips
to the base — which also recovered ~25 bibliographic-tag markers (`Te. (SAN)`, `Ka. (DCV)`,
`Ta. (lex.)`). Regen: **+327 attestations**, 0 losses. **+214** matched.

#### 9. Nested tag in headword bold (Ma-type) — `af90534` (parser)
Pattern A required the headword run to end at `</b>`, so any nested tag inside the headword's
bold span broke `([^<]+)</b>` and silently dropped the language (no fallback recovers it). Three
markup flavors, one root cause: nested `<i>obl.</i>` qualifiers (DED 1), italicised scientific
names (DED 62 *Sphaeranthus indicus*), `<at>…</at>` encoding artifacts (DED 11). **613 dropped
languages / 703 attestations.** Changed the terminator `</b> → (?=<)` (stop at the next tag —
transparent for clean markup) and trimmed a trailing `(` artifact in headword cleaning (431/703
needed it). Regen: **+703 attestations, 0 losses, 0 headword-value changes** for already-parsed
languages (verified by faithful in-memory sim). **+392** matched — biggest single lever.

---

## 3. Worked examples — where to verify each fix

For a live demo, pick one DED number per fix and open it on each side.

- **DDSA (Burrow):** `https://dsal.uchicago.edu/cgi-bin/app/burrow_query.py?qs=<DED#>` — searches
  the Burrow DED; the result links straight to the dictionary page.
- **Starling:** browse the Dravidian etymology at `starlingdb.org` and search by the **proto-form
  or meaning** (Starling entries are keyed by their own text number, not by DED#).
- **Your xlsx (`tree_validation_results.xlsx`):** filter the **`Validation DED #`** column to the
  number, then read **`Starling lexical headword`**, **`Matched Burrow form`**, **`Match`**, and
  **`Validation note`**.

| # | Fix | DED # | Burrow / DDSA shows | Starling shows | What the fix demonstrates |
|---|---|---|---|---|---|
| 1 | DED-0 sentinel | — | n/a (Burrow has no DED 0) | sub-entries with `Number in DED = 0` | No longer surface as false "DED 0 not found" rows in the xlsx |
| 2 | Pattern E false positives | 144 | `Ta. atti … Ficus glomerata` (italic genus) | Tamil `atti` "country fig" | The italic genus `Ficus` is no longer mis-listed as a "language" |
| 3 | Sub-entry / Tamil-missing | 990 | `(a) Ta. oru` "one" | Tamil `oru` | Tamil (glued to the `(a)` marker) is now extracted and matches |
| 4 | Underscore normalization | 410 | Tamil `iṉṟu` | Tamil `in_r_u` | Same form — ASCII-underscore vs diacritic — now reconciled |
| 5 | Length-dot normalization | 2891 | Kota `te·l` (raised dot) | Kota `tēl` (macron) | Raised-dot length mark = macron — now matches |
| 6 | Eng (ŋ) velar-nasal | 2591 | Tulu `ediṅke` (ṅ) | Tulu `ediŋke` (ŋ) | Two notations of /ŋ/ — now matches |
| 7 | DED letter-suffix | 4896 | numbered **`4896(a)`** (and listed twice); Tamil `mukku` | DED 4896, Tamil `mukku` | `4896(a)` now indexes under `4896`; all 12 rows recovered |
| 8 | Naiki dialect abbrev | 5154 | `Nk. (Ch.) ām(e)` (= Naiki) | Naiki `ām(e)` | `Nk. (Ch.)` (space in the abbrev) is now parsed as its own language |
| 9 | Nested tag in headword | 1 | Tulu `āye … (obl. ay-)`; also DED 62 Ma. `aṭakkā-maṇiyan` (ital. *Sphaeranthus indicus*) | Tulu `āye` | A nested `(obl.)`/italic-species/`<at>` tag no longer truncates the entry |

**Most obvious for colleagues:** DED **4896** (the `(a)` suffix is visible right next to the
number on the DDSA page) and DED **5154** (the `Nk. (Ch.)` marker is plainly in the text) — both
show the discrepancy at a glance without needing the xlsx.

---

## 4. Corpus version history

Matcher-side fixes change only comparison logic — no regen. Parser-side fixes rewrite the
~5,685-entry cleaned corpus.

| Corpus | Date | Trigger | Net attestation Δ |
|---|---|---|---|
| v4 | 2026-04-05 | Pattern E added (Kota `aṛy` recovered) | — |
| v5 | 2026-06-18 | Pattern E false-positive cleanup | 0 (false positives removed) |
| v6 | 2026-06-18 | Sub-entry markers (Tamil-missing) | 505 entries changed, 0 losses |
| v7 | 2026-06-18 | Naiki dialect-qualified abbrev | +327, 0 losses |
| v8 | 2026-06-18 | Nested tag in headword bold | +703, 0 losses |
| v9 | 2026-08-17 | `id.` over-glue guard (option A′, §9) | 202 glosses → literal `id.` |
| v10 | 2026-08-18 | To.-type sci-name markers; leading-qualifier C/C2; running-text markers (`44287fc` `57e038d` `1f9bf85`) | +221 attestations behind sci-names |
| v11 | 2026-08-19 | Plain-text running-text markers; sci-name bolded abbrev (`cf29542` `c799d51`) | +44 matches |
| v12 | 2026-08-20 | Kui derivation-paren + HTML-entity unescape (`99a1b69`) | 18 headwords unescaped |
| v13 | 2026-08-20 | 200-char gloss-cap removal (`a866dbe`) | 252 glosses grew, 1 corrected |
| v14 | 2026-08-20 | Naiki `(Ch.)` relabel (`5b4b16d`) | 4 relabels, 0 loss |
| v15 | 2026-08-20 | Four non-canonical-markup one-offs (`6f20bd2`) | 15 changed, +18 attestations |
| **v16** | **2026-08-20** | **Tamil comma-for-period marker (`766011a`) — current** | **1 entry, +Ta.** |

**Current corpus state.** `_reparse_meta` on `burrow_corpus.cleaned.json` reads
`2026-08-20T12:28:29`, 5,685 entries processed, 1,650 changed — that run *is* the regen for
`766011a` (committed 12:29:52, ~80 s later). Every parser commit through `766011a` is baked in;
`c5f78ff`, the last fix of the pilot, is matcher-side and needs no regen. **The corpus is in sync
with the parser.** Totals: 5,685 entries, 30,354 attestations, 4,804 repaired glosses.

> **Box re-sync.** The v12–v16 regens carried `RE-SYNC TO BOX` flags through the tail; those are
> **taken as done as of 2026-09-05**. One further re-sync is planned once the current session's
> work is wrapped up — the corpus hashes have not moved (`6b6dc5dc6d17a5bb`), but the
> `tree_validation_output/` reports will change once the validator is re-run. Verify against
> `docs/SYNC.md` Tier 2 after that upload.

- `burrow_corpus.json` — raw scrape, source of truth, never modified.
- `burrow_corpus.cleaned.json` — currently **v8**.
- Rebuild: `python src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py data/dravidian/burrow_ded/burrow_corpus.json --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json`

---

## 5. Still open / deferred

| Item | Class | Side | Est. impact | Notes |
|---|---|---|---|---|
| Embedded forms — **Kuwi-type** | Parser bug | parser | ~48 misses | `<b>… <i>Lang</i></b> (q) <b>hw</b>`: text precedes the `<i>` marker; headword in a later `<b>`. |
| Embedded forms — **To-type** | Parser bug | parser | ~17 misses | `<i>incl.). To.</i>`: abbrev buried mid-italic after lowercase; malformed source, likely rare. |
| `dakku` secondary form | Parser bug | parser | unquantified | Real Kannada form buried in the Ka. gloss text (DED 3014). |
| **Phase 2 single-char filter** | Parser bug | parser | unquantified | `len(hw) > 1` drops legit one-letter headwords (Tamil `i` in DED 410). Proposed `len(hw) > 1 or hw.isalpha()` — quantify admits first. |
| **Special-vowel divergences** | Genuine divergence | n/a | `ɨ` ×339, `ʔ` ×84, `ɫ` ×41, `ʒ` ×10 | Real phonemic distinctions (Toda/Kota), not orthographic variants — log as `genuine_divergence`, do **not** fold. Confirm a few before logging. |
| Long-tail headword mismatches | Mixed | n/a | flat `row_count=2` tail | Individual diacritic/orthographic divergences + dialect mismatches (conf 0.95). No dominant lever. |
| ~~**Dedupe pass**~~ **CLOSED** | Data quality | matcher (loader) | 988 attestations folded | Duplicate concatenation resolved on load — see §8. |
| Option C qualifiers | Parser (deferred) | parser | DED 2617, 1617 | Single-letter `P.`/`A.` still captured by Pattern E; left for a structural fix. |
| ~~**id.-expansion over-glue**~~ **CLOSED** | Corpus gloss quality | corpus (parser + repair) | 192 multiform cases | Resolved 2026-08-17 — leave ambiguous `id.` literal (option A′); see §9. |

~~All large systemic levers … are now spent.~~ **Superseded — that was true of the 80.7%-era
levers only.** The 08-16 refactor and the fresh scrape exposed a second generation of levers worth
a further **+899** matches (94.0% → 98.6%). See **§10** for the full tail; every item in the table
above is resolved there except the To.-type running-text TEXT variant, which remains the one open
parser shape. Kuwi-type and the To.-type bold variant were both closed (`44287fc`).

---

> **Ported from the Obsidian vault (`DravidianLEX/cross-validating-dded-starling/`) on
> 2026-09-05.** These two sections existed only in the vault copy; §8–§9 existed only in the
> repo copy. The two documents had diverged after 2026-08-15. This file is now the superset.

## 6. StarlingDB re-scrape reconciliation (2026-08-15)

Re-ran the validator against a fresh StarlingDB scrape
(`data/dravidian/starling/starling_complete_data.json` — 2211 records, 10,905 unique
entries, `backfilled_from_old: 47`) with the unchanged **v8** cleaned corpus. No parser or
matcher code changed; this is a pure source-data refresh.

> **Input repointed on port (2026-09-05).** The vault original named this input
> `starling_complete_data_scrape.json` — a real scrape at the time, since **superseded** by
> `starling_complete_data.json` (and the markup superset `starling_complete_data_markup.json`).
> The numbers in this section still stand; only the filename changed. Every reference has been
> repointed, `starling_tree_validator.py`'s docstring included.

```
python src/dravidian/scripts/cross-validating-dded-starling/starling_tree_validator.py \
  data/dravidian/starling/starling_complete_data.json \
  --corpus data/dravidian/burrow_ded/burrow_corpus.cleaned.json \
  --output-dir data/dravidian/cross-validating-dded-starling/tree_validation_output
```

| Metric                 | Prior scrape | New scrape | Δ    |
| ---------------------- | ------------ | ---------- | ---- |
| `entry_match_rate`     | 87.1%        | **87.1%**  | 0.0  |
| Total language entries | 20,293       | 20,275     | −18  |
| Entries with DED #     | 19,628       | 19,611     | −17  |
| `entries_matched`      | 17,098       | 17,080     | −18  |
| Branches               | 7,991        | 7,986      | −5   |

**Match rate is stable and there are zero `Yes → No` regressions.** All deltas are confined to
**10 DED numbers**; the baseline was preserved to
`data/dravidian/cross-validating-dded-starling/tree_validation_output/tree_validation_results.before-new-scrape.csv`.

**−30 rows removed — de-duplication, not data loss.** The prior scrape emitted some etymon trees
with a duplicated sub-entry branch, so every language row under them was listed twice. The new
scrape collapses the duplicate branch; the *unique* attestation set for each is byte-for-byte
identical (verified row-by-row). Affected: DED **5440** (−13), **2591** (−6), **4587** (−5),
**2891** (−4), **3014** (−1), plus one orphaned blank-DED Konda row (−1). This is the Starling
side of the "Dedupe pass" open item.

**+12 rows added — genuine new attestations** the prior scrape missed: DED **1297** (+6, e.g.
Ta. `kal (kar_p-, kar_r_-)`, Ma. `kalkka`, Ka., Kota, Tulu, To.), **4980** (+4: Ta. `muruku`,
Ma. `moriyuva`, Ka. `muruṇṭu`, Tu. `muripuni`), **3972** (+1: To. `par mēṇ`), **5511** (+1:
Te. `ver_r_i`). All matched on ingest.

**Net:** the drop in `entries_matched` reflects only the removal of inflated duplicate branches;
the refreshed source is strictly cleaner and marginally richer. No corpus regen or code fix was
warranted.

---

## 7. Canonical transcription policy (2026-08-16)

**Decision (Kevin + Todd):** where a Starling reflex and its matched Burrow/DEDR attestation are
the *same form written in different notation*, **DEDR is the canonical transcription**; Starling's
spelling is retained as a variant. "Pick a version and go with it for now; changes can be made
later." Reversible by design — nothing is overwritten, both spellings are kept.

**Why divergences split into two classes (and why the swap must be gated).** Every genuinely
"language-only" row (language present on both sides, headword differs) already carries its paired
Burrow form, so adopting the DEDR spelling is a mechanical column swap. But those rows are not one
bucket:

- **(i) notational variant** — same form, different glyph convention (`aṛpɨn`↔`aṛpïn`,
  `oʒ-`↔`oζ-`, `alraʔānā`↔`alra'ānā`). Safe to adopt DEDR's spelling.
- **(ii) divergent form** — Starling's reflex simply isn't Burrow's (`addalipuni`↔`adůruni,
  adaruni, aduruni`; `aŋ-gāl`↔`ōragitti`). Swapping here would replace a real reflex with an
  unrelated form — so these **keep Starling's headword**.

The gate is a transcription-equivalence fold: swap only when the two sides normalize to the same
string. A deliberately **conservative core** set is used — the unambiguous IPA-vs-diaeresis /
glottal pairs `ɨ/ï`, `ɫ/ł`, `ʒ/ζ`, `ʔ/'` (on top of the shipped underscore / length-dot / eng
folds). Broader pairs (`ẓ/r̤`, the `ch/c` digraph) are **intentionally deferred** to a linguistic
ruling and stay classified as `divergent_form` for now.

**Implementation — validator layer only** (no corpus regen, no matching change, match rate
unchanged at **87.1%**). `starling_tree_validator.py` gained `_CANONICAL_CORE_FOLD`,
`_transcription_key`, `_canonical_burrow_form`, `_canonical_fields`, and three new result columns:

| Column | Meaning |
| ------ | ------- |
| `Canonical headword` | DEDR spelling when canonical, else Starling's |
| `Canonical source` | `burrow` / `starling` |
| `Transcription status` | `identical` / `notational_variant` / `divergent_form` / `no_burrow_match` / `no_ded` |

**Distribution (new scrape, 20,275 rows):** `notational_variant` 9,853 · `identical` 7,531 ·
`divergent_form` 1,591 · `no_ded` 664 · `no_burrow_match` 636. `Canonical source` = `burrow`
17,384 / `starling` 2,891. Of the 9,853 notational variants, **304 are the genuine language-only
divergences** now adopting DEDR spelling; the rest are matched rows reconciled by the shipped
folds. The **1,591 `divergent_form`** rows correctly stay on Starling.

**Deferred — build layer.** `build_dravidilex_import.py` is still Starling-sourced, so the
*published pilot* headwords are unchanged. Making the pilot show the DEDR canonical spelling is a
follow-on step: join these columns into the build (headword := `Canonical headword`, keep Starling
as a `Headword (Starling)` extra field). Held until the conservative fold set is confirmed.

---

## 8. Corpus-duplication reconciliation (2026-08-16)

Two of the 2026-08-16 batch follow-ups — both about the ~117 DED numbers that carry
more than one corpus entry — resolved **loader-side** (`load_burrow_corpus`), no corpus
regen, no re-scrape. `entry_match_rate` holds at **94.0%** (18,184 matched); verified by a
full row-level diff (20,007 rows both sides, 0 added/removed, exactly **1** Match-status
change — an eliminated false positive).

**Audit of the 104 duplicate-DED groups** (5,633 DEDR entries → 5,516 unique numbers):

- **84 partial-overlap groups** = same-page **double-scrapes**, one copy truncated at an
  `(a)`/`(b)` sub-entry boundary (e.g. two DED 137 records on p.14). The loader's
  `setdefault`+append **concatenated** both, so overlapping languages were counted twice.
- **18 disjoint groups**, of which **10** are the Appendix mislabel below; the rest are
  genuine same-page complementary splits (kept).
- **2 identical groups** (byte-identical re-scrapes).

**Fix A — attestation dedup on load.** After concatenation + gloss-repair, each paragraph's
attestation list is folded on the `(language_abbrev, headwords, gloss)` tuple. Byte-identical
duplicates collapse; genuine `(a)`/`(b)` forms of one language keep distinct headwords/gloss
and survive; order-preserving. **988 duplicate attestations folded.** Match-rate-neutral by
construction (removing duplicate Burrow forms cannot change whether a Starling form matches);
the win is de-doubled coverage/audit counts.

**Fix B — Appendix page-mislabel.** Ten Appendix supplement entries (pp.509–512, DED
1/3/4/7/27/44/45/47/49/50) were mislabelled `edition="DEDR"`: `editions.detect_edition_from_text`
matched the `DED(S) N` backward-reference they carry and overrode the authoritative page≥509
classification. Their IA-loanword reflexes were being merged into the real DEDR paragraph for
numbers the Appendix reuses. The loader now skips `edition=="Appendix" OR page>=APPENDIX_START_PAGE`
(page is authoritative: 62 Appendix / 5623 DEDR is a clean partition). One eliminated false
positive: Starling Tamil `a` (DED 1) had matched "Language only" against Burrow `akkaṭa`
"excl. of wonder", which existed **only** in the DED(S) 21 supplement, not DEDR DED 1 (no Tamil)
→ now correctly "No".

**Deferred — id.-expansion over-glue** (resolved 2026-08-17; see §9).

---

## 9. id.-expansion over-glue — resolved (2026-08-17)

**The bug.** Both `id.` (idem) resolvers — `burrow_entry_parser.parse_language_sections` and
`repair_burrow_corpus_glosses.repair_corpus` — expanded an `id.` reference by copying the
**entire** previous attestation's gloss. When that antecedent section lists more than one
**form**, this over-glues: DED 7 `Ka. ōragitti, vāragitti id.` opened with *all* of Toda's gloss
("to be caught, be got; … wïrxity …") instead of the one sense `id.` points at ("husband's
brother's wife").

**Why no string rule works (given, re-verified against the source).** `id.` = "same meaning as
understood in context" — which antecedent form is editorial, not positional:
- DED 7 → Toda's **last** form `wïrxity` "husband's brother's wife" ("nearest" right, head wrong).
- DED 54/88 → Tamil **head** form (`añcal` "relay…", `aṭaikkāy` "areca-nut") ("head" right, nearest wrong).

Head vs. nearest **directly contradict** across real entries; the true antecedent is even
non-cognate (DED 7 `ōragitti` is phonologically unlike `wïrxity` — the link is purely semantic).

**Method — gold set over the full population, not a sample.** Enumerating from `full_text` (via
anchor-based section slicing) found **192 multi-form-antecedent `id.` references** (of 2,985 total;
the other 2,793 have a single-form antecedent and were already resolved correctly). All 192 were
hand-labeled against the entry text and each candidate heuristic scored:

| Option | correct | corruptions (wrong meaning asserted) | corrupt-rate |
|---|---|---|---|
| **A′ leave ambiguous `id.` unresolved** | 0 | **0** | **0%** |
| B head-meaning (first `;`-segment) | 139 | 50 | 26% |
| C nearest-form meaning | 76 | 113 | 60% |
| D whole-copy (previous behavior) | 0 | 189 | 100% |

C is broken (antecedents frequently end in their own `id.`/citation, so "nearest" returns `id.`
or noise). B is the best meaning-provider but **fabricates a wrong meaning in 26% of cases**, and
those are indistinguishable from its correct outputs — unacceptable for a display-quality resource
where "honesty outranks completeness; never fabricate to look complete."

**Fix (option A′).** Added `textnorm.antecedent_is_multiform()` — a `;`-segment after the first
that begins with a transliterated Dravidian form token ⇒ ambiguous. Both resolvers now guard on
`resolvable = last_real_gloss and not antecedent_is_multiform(last_real_gloss)`: single-form
antecedents resolve as before; multi-form antecedents keep the literal `id.` (faithful to Burrow).
The guard can only *skip* resolution — it never asserts a new meaning, so no gloss can become more
wrong.

**Verification (corpus regen + full validator diff vs. baseline).**
- Corpus: 202 attestation glosses changed, **all** toward literal `id.`/cleaner (never a new
  meaning). Single-form cases untouched. Anchors correct (DED 7 Ka. = `id.; aṅ-gāl …`; DED 54/88 =
  `id.`). Parser and repair sides produce consistent output.
- Validator: **0 rows added/removed**; `entry_match_rate` **94.0% held**, `entries_matched`
  **18,184 → 18,185 (+1)**; exactly **3** Match-status changes, all positive (DED 4847 Seoni Gondi
  `Language only → Yes`; DED 5554 Kui and DED 7 Kannada `aŋ-gāl` upgraded `gloss_secondary →
  substring/exact` as the over-glued forms stopped masking the direct headword match). **0 matches
  lost.** 141 rows changed only in the displayed `Matched Burrow meaning` — the intended display fix.

**Cost / tradeoff (accepted).** A′ also leaves the ~139 cases where "head" *would* have been right
(DED 88 Toda `aḍky` now shows `id.` rather than "areca-nut"). This is the deliberate
honesty-over-completeness choice: better a faithful `id.` the reader resolves from the same entry
than a resolved gloss that is silently wrong 1-in-4 times.

**Files:** `textnorm.py` (helper), `burrow_entry_parser.py` + `repair_burrow_corpus_glosses.py`
(guarded resolvers). Corpus regenerated (`burrow_corpus.cleaned.json`; prior kept as
`burrow_corpus.cleaned.before-id-fix.json`). **The regenerated corpus must be re-synced to the
shared Box folder** (item 7 Sources reads it).

---

## 10. The tail: 94.0% → 98.6% (2026-08-18 → 2026-08-21)

Everything after §9 was a long tail of narrow levers. §1's "all large systemic levers are now
spent" was true of the *80.7%-era* levers only — the 08-16 refactor (`project_refactor_roadmap`)
and the re-scrape exposed a second generation of them. Method throughout: group the remaining
`No` / `Language only` rows by **language × HTML shape** from `validation_audit_report.xlsx`, find
the shared shape, ship one fix, re-validate, require **0 regressions**, log to `review_ledger.json`.

| Date | Fix (ledger key) | Commit | Side | Matched → | Δ |
|---|---|---|---|---|---|
| 08-16 | Six-bug batch (biggest: `gloss_secondary` in-gloss form scan) | `ff6370b` | both | 18,184 | +1,104 |
| 08-18 | Retroflex zh (`ẓ`/`r̤`) fold + DED 1 Ta. Pattern-G; single-char vowel headwords | `fae4cca` `fc3104b` | both | 18,381 | +~197 |
| 08-18 | Bare-letter DED suffix (`3621A` → base) | `02b1fa2` | matcher | 18,410 | +29 |
| 08-18 | To.-type bold marker behind scientific names | `44287fc` | parser | 18,473 | +63 |
| 08-18 | Leading qualifier in `<i><b>` order (Patterns C/C2) | `57e038d` | parser | 18,515 | +42 |
| 08-18 | Running-text markers behind sci-names, lexical guard | `1f9bf85` | parser | 18,558 | +43 |
| 08-18 | **Subgroup-DB orphan exclusion** — denominator 20,007 → 19,354 | `ad5da93` | scope | — | 0 |
| 08-18 | Internal-hyphen deletion in `normalize_for_match` | `630f65a` | matcher | — | +115 |
| 08-18 | Glottal-stop fold (`ʔ` / `ˀ` / `’`) | `852c9dc` | matcher | — | +67 |
| 08-18 | Spacing nasalization/breve strip | `872da4f` | matcher | 18,763 | +23 |
| 08-18 | Category-A abbrev/base-name gaps (`Mand.`, `Koḍ`, `Ir.`) | `178ed8d` | matcher | 18,769 | +6 |
| 08-19 | Diagnostics-driven batch, 8 commits (`df1349f`…`37bb56b`) | 8 commits | both | 18,864 | +95 |
| 08-19 | Adilabad Gondi `(SR.)` inline sigil | `a3de7c0` | matcher | 18,904 | +40 |
| 08-20 | Toda consonant fold (`ɫ`/`ł`, `ʒ`/`ζ`) | `f8ab3fc` | matcher | 18,952 | +48 |
| 08-20 | Telugu optional-infix paren expansion (`k(r)ovvu`) | `06482a6` | matcher | 18,989 | +37 |
| 08-20 | Kui derivation-paren drop + HTML-entity unescape | `99a1b69` | both | 18,996 | +7 |
| 08-20 | Konda glottal trailing-space | `9b45580` | matcher | 19,004 | +8 |
| 08-20 | Adilabad `(ASu.)` sigil + 200-char gloss-cap removal | `1f06a1a` `a866dbe` | both | 19,033 | +29 |
| 08-20 | Inline-form scan across *all* matched attestations | `1a04774` | matcher | 19,043 | +10 |
| 08-20 | Unterminated-paren comma strip | `42edaaa` | matcher | 19,050 | +7 |
| 08-20 | Naiki `(Ch.)` between-marker qualifier | `5b4b16d` | parser | 19,053 | +3 |
| 08-20 | Four non-canonical-markup parser one-offs | `6f20bd2` | parser | 19,060 | +7 |
| 08-20 | Tamil comma-for-period marker (DED 4002) | `766011a` | parser | 19,061 | +1 |
| 08-20 | Comma/slash multiform-delimiter fold | `7534128` | matcher | 19,065 | +4 |
| 08-20 | Split-token atom-exact match | `384d8dd` | matcher | 19,068 | +3 |
| 08-20 | `<->` scrape-artifact strip | `43259ca` | matcher | 19,069 | +1 |
| 08-20 | Space-insensitive exact match | `d166c4f` | matcher | 19,075 | +6 |
| 08-20 | **Postposed dialect markers in inline forms** | `c5f78ff` | matcher | **19,083** | +8 |

**Two attempts were reverted** under the strict 0-regression rule, both recorded here because the
lesson is reusable: an intra-paren space-strip recovered +5 but broke DED 410 Chanda Gondi (its
prior match was a *spurious* substring hit on a plural fragment), and a Muria postposed **backward**
look grabbed the neighbouring dialect's form. `c5f78ff` later solved the same postposed problem
**forward-anchored** on both the segment boundary and the target marker — the safe shape.

### The Lane-A adjudication (2026-08-20) — where the residual went

With systemic levers exhausted, the remaining `Language only` rows were swept once for recoverable
shapes (yielding the last four matcher fixes above), and the rest were **classified, not fixed**:

| Category | n | Same lexeme? |
|---|---|---|
| `partial_shared_root` | 52 | shares a root only — Te. `ā` vs `vã̄ḍu` |
| `distinct_reflex` | 40 | **no** — Kota `gud-` vs `kur-`, Muria `gumiya` vs `koṛpanj` |
| `close_spelling_vowel_consonant` | 25 | yes |
| `morphological_citation` | 13 | yes — Gondi `-ānā` / Kuwi `-ali` vs bare stem |
| `display_fallback_or_postposed` | 11 | n/a — matcher showed an English gloss as the "Burrow form" |
| `inscr_telugu_r_d_corresp` | 3 | yes — Inscriptional Telugu `ṛ`/`ḍ` |

Worksheet: `data/dravidian/cross-validating-dded-starling/tree_validation_output/genuine_divergence_worksheet.csv` (tracked). **It is one revision
stale**: it lists 144 rows, but `c5f78ff` then recovered **8** of the 11
`display_fallback_or_postposed` rows (DED 2675, 1655, 1818, 1416, 3808, 3884, 3399) — those were
the postposed-marker cases, not display bugs. **Live residual is 136 `Language only`**, with 3
`display_fallback_or_postposed` left. Regenerate the worksheet before using it as a review queue.

Per-language diagnostics for the unmatched rows are in `data/dravidian/cross-validating-dded-starling/tree_validation_output/loop_findings.md`
(19 of 52 language groups worked; `loop_worklist.json` tracks the other 33 as `pending`). Each
group carries a dominant-verdict classification and a proposed fix, so the work is resumable.

---

## 11. Final state (pilot frozen 2026-08-21)

```
total_language_entries   19,354   (in scope; 653 subgroup-DB orphans excluded)
entries_matched          19,083   98.6%
  Language only             136   logged genuine_divergence — AI-triaged, unreviewed
  No                        135
unique_branches           7,948   fully_attested 7,740 · not_attested 44 · ded_not_in_corpus 10
```

**How to cite this in the AsiaLex paper.** The defensible sentence is: *"98.6% of the 19,354
DED-bearing Starling entries in scope matched a Burrow & Emeneau attestation; 653 orphan rows from
non-DED-keyed subgroup databases were excluded from scope."* Do **not** report a figure that folds
the 136 adjudicated divergences into the numerator — they are unreviewed AI classifications, and
40 of them are, by their own label, genuinely different reflexes.

**What would raise confidence, in order of value:**

1. **Human spot-check of the `genuine_divergence` set.** 135 of 147 language-level ledger records
   are `reviewed_by: claude-triage`. A stratified sample (~20 rows across the 6 categories),
   confirmed by a Dravidianist, converts the residual from asserted to evidenced.
2. **Regenerate the worksheet and the row-level artifacts.** `tree_validation_results.csv/.xlsx`,
   `validation_audit_report.xlsx` and `coverage_by_ded_paragraph.xlsx` in `data/dravidian/cross-validating-dded-starling/tree_validation_output/`
   are **stale (2026-07-07)** — they predate the entire §10 tail. Only
   `tree_validation_summary.json`, `loop_*`, and the worksheet are current (2026-08-21).
3. **Finish `loop_worklist.json`** — 33 of 52 language groups are still `pending`, covering 170 of
   the 585 diagnosed rows.

**Deferred by decision, not oversight:** the `ẓ`/`r̤` and `ch`/`c` folds await a linguistic ruling
(§7); the canonical-transcription build-layer join is unbuilt, so published pilot headwords remain
Starling-sourced (§7); the To.-type running-text TEXT variant and Kui bare sense-suffix forms have
no safe lever (see `.claude/memory/project_burrow_parser_status.md`).
