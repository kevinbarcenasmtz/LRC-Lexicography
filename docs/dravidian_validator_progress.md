# Dravidian Validator — Cross-Validation Progress Report

**Scope:** Burrow DED ↔ StarlingDB cross-validation (`src/dravidian/scripts/cross-validating-dded-starling/`).
**Period:** 2026-06-18 (two working sessions).
**Headline:** `entry_match_rate` **69.9% → 80.7%**; `entries_matched` **~13,733 → 15,841** out of 19,628 Starling entries carrying a DED number.

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

## 3. Corpus version history

Matcher-side fixes change only comparison logic — no regen. Parser-side fixes rewrite the
~5,685-entry cleaned corpus.

| Corpus | Date | Trigger | Net attestation Δ |
|---|---|---|---|
| v4 | 2026-04-05 | Pattern E added (Kota `aṛy` recovered) | — |
| v5 | 2026-06-18 | Pattern E false-positive cleanup | 0 (false positives removed) |
| v6 | 2026-06-18 | Sub-entry markers (Tamil-missing) | 505 entries changed, 0 losses |
| v7 | 2026-06-18 | Naiki dialect-qualified abbrev | +327, 0 losses |
| v8 | 2026-06-18 | Nested tag in headword bold | +703, 0 losses |

- `burrow_corpus.json` — raw scrape, source of truth, never modified.
- `burrow_corpus.cleaned.json` — currently **v8**.
- Rebuild: `python src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py data/dravidian/burrow_ded/burrow_corpus.json --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json`

---

## 4. Still open / deferred

| Item | Class | Side | Est. impact | Notes |
|---|---|---|---|---|
| Embedded forms — **Kuwi-type** | Parser bug | parser | ~48 misses | `<b>… <i>Lang</i></b> (q) <b>hw</b>`: text precedes the `<i>` marker; headword in a later `<b>`. |
| Embedded forms — **To-type** | Parser bug | parser | ~17 misses | `<i>incl.). To.</i>`: abbrev buried mid-italic after lowercase; malformed source, likely rare. |
| `dakku` secondary form | Parser bug | parser | unquantified | Real Kannada form buried in the Ka. gloss text (DED 3014). |
| **Phase 2 single-char filter** | Parser bug | parser | unquantified | `len(hw) > 1` drops legit one-letter headwords (Tamil `i` in DED 410). Proposed `len(hw) > 1 or hw.isalpha()` — quantify admits first. |
| **Special-vowel divergences** | Genuine divergence | n/a | `ɨ` ×339, `ʔ` ×84, `ɫ` ×41, `ʒ` ×10 | Real phonemic distinctions (Toda/Kota), not orthographic variants — log as `genuine_divergence`, do **not** fold. Confirm a few before logging. |
| Long-tail headword mismatches | Mixed | n/a | flat `row_count=2` tail | Individual diacritic/orthographic divergences + dialect mismatches (conf 0.95). No dominant lever. |
| **Dedupe pass** | Data quality | corpus | DED 410 (×4), 4896 (×2) | Duplicate corpus entries from the scrape/repair. |
| Option C qualifiers | Parser (deferred) | parser | DED 2617, 1617 | Single-letter `P.`/`A.` still captured by Pattern E; left for a structural fix. |

All large systemic levers — every matcher-side fold (underscore, length-dot, eng, letter-suffix)
**and** the two biggest parser-side recoveries (Naiki, nested-tag headword) — are now spent. The
remaining tail is narrower; Kuwi-type and To-type are the last sizeable parser buckets.
