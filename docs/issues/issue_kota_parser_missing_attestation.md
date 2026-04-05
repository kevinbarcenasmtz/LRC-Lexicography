---
type: issue
topics: [dravidian, cross-validating, burrow&ded, parser]
title: kota_missing_attestation_parser_bug
date: 04-05-2026
status: closed
resolved: 04-05-2026
---

## Summary

Ko. (Kota) attestation for DED #72 is missing from `burrow_corpus.cleaned.json`. The data exists in the source HTML but is never extracted as a standalone attestation — it bleeds into the Malayalam (Ma.) gloss instead.

**Symptom in validation audit report:**
```
14  72  Proto-Nilgiri  *aḍĭ  Kota  aṛy  foot (measure)  No  [empty]
Kota not in DED 72; Burrow has: Gadba, Gondi, Kannada, Kodagu, Konda, Malayalam, Tamil, Telugu, Toda, Tulu
```

---

## Root Cause

The Burrow HTML for DED #72 around the Ko. entry is:

```html
<b><i>fem.</i> aṭiyātti. <i>Ko.</i> aṛy</b> foot (measure);
<b>ac</b> place below; <b>acgaṛ</b> place beneath an object ...
```

`fem.` opens a `<b>` tag. `Ko.` and its headword `aṛy` are plain text inside that same `<b>`, not wrapped in their own bold element. None of the four patterns in `BurrowEntryParser._find_all_lang_spans` match this structure:

- Pattern A: `<b><i>Lang.</i> headword</b>` — doesn't fire because the `<b>` belongs to `fem.`
- Pattern C: `<i><b>Lang.</b></i> ... <b>headword</b>` — doesn't fire, no separate bold headword
- Pattern B/D: `<i>Lang.</i> ... <b>headword</b>` — doesn't fire, `aṛy` is not in `<b>`

`fem.` is correctly rejected by `_is_valid_lang` (starts lowercase), but because it consumed the opening `<b>`, `Ko.` is invisible to the span finder. The Ko. data ends up embedded in the Ma. gloss:

```
"fem.aṭiyātti.Ko.aṛyfoot (measure);acplace below;acgaṛplace beneath..."
```

---

## Scope

This is likely not unique to DED #72. Any DED entry where a `fem.`, `masc.`, `pl.`, or similar grammatical qualifier appears inside a `<b>` block before a language marker will exhibit the same miss.

---

## Fix Applied

**File:** `src/dravidian/scripts/cross-validating-dded-starling/burrow_entry_parser.py`

Added **Pattern E** to `_PATTERNS` (lines 86–95):

```python
# Pattern E: <i>Lang.</i> plain-text-headword (no <b> wrapper on headword)
# Negative lookbehind avoids re-matching <b><i>Lang.</i> headword</b> (Pattern A).
re.compile(
    r"(?<!<b>)<i>(" + _LANG_CHAR + r"\.?)</i>"
    r"\s*(?:\([^)]*\)\s*)*"
    r"([^\s<;(][^<;(]*?)(?=\s*[;<(]|\s*</?[bi])",
    re.DOTALL,
),
```

The negative lookbehind `(?<!<b>)` prevents double-matching structures already
caught by Pattern A. The headword group stops at the first `<`, `;`, or `(`
delimiter, which correctly terminates at `</b>` in the `fem.` block case.

**Corpus regenerated** using `reparse_burrow_corpus.py` (added as part of this
fix). The repair script (`repair_burrow_corpus_glosses.py`) alone was not
sufficient because it only patches existing attestation glosses — it cannot add
attestations that the original parser never extracted.

**Result:** `burrow_corpus.cleaned.json` rebuilt from `burrow_corpus.json`
raw HTML cache. DED #72 now yields 11 attestations (was 10), with Ko. `aṛy`
"foot (measure)" correctly extracted.

---

## Side Effect — Pattern E False Positives

The reparse of all 5,685 entries revealed that Pattern E also picks up
botanical names, bibliographic short titles, and other capitalised
non-language tokens that happen to follow an `<i>...</i>` tag and pass
`_is_valid_lang` (starts uppercase, not in `_INVALID_LANG_ABBREVS`).

Examples observed in the diff output:

| Gained token | DED entry | Nature |
|---|---|---|
| `Language` | 1180, 1611, 2130, 2147, 4112, 4312, 4377, 4630, 5301, 5549 | section heading artefact |
| `Gramm.)` | 557, 895 | bibliographic abbreviation with stray `)` |
| `Wrightiaantidysenterica` | 1650 | botanical species name (collapsed) |
| `B.flabellifer` | 4037 | botanical abbreviation |
| `Ficus` | 2697 | genus name |
| `Oxalis` | 4326 | genus name |
| `Physalis` | 3009 | genus name |
| `Tribulum` / `T.terrestris` | 2928 | botanical names |
| `Jasaharacariu` | 1265, 2248 | Old Kannada text title |
| `Mahāpūrāṇa` / `Mahāpurāṇa` | 718, 1110, 1684, 1992, 2248 | Sanskrit text title |
| `Śabdaratnākara` | 2694, 4458 | Sanskrit lexicon title |
| `Yaśastilaka` | 1379, 4482 | Sanskrit text title |
| `Nachträge` | 705 | German supplement reference |
| `Divy.` | 4838 | Divyāvadāna (Buddhist Sanskrit) |
| `Uṇ.` / `Uṇ` | 1242, 3367, 4971 | *Uṇādi-sūtra* grammatical text |

None of these were previously attested so there is **no regression** — the
validator will simply encounter new unmatched language names it already handles
gracefully. However they inflate attestation counts and may produce noise in
future audit reports.

**Follow-up issue:** Extend `_INVALID_LANG_ABBREVS` or add a length/format
guard to `_is_valid_lang` to exclude multi-word tokens, tokens longer than
~12 characters, and known bibliographic abbreviations. Track as a separate
issue: `issue_pattern_e_false_positives.md`.
