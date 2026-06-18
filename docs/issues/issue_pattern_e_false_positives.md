---
type: issue
topics: [dravidian, cross-validating, burrow&ded, parser]
title: pattern_e_false_positive_lang_markers
date: 04-05-2026
status: resolved
resolved: 06-18-2026
---

## Resolution

Fixed in `burrow_entry_parser.py` via **Option A + Option B** (Option C deferred):

- `_is_valid_lang` now strips trailing `.`/`)`/`(` before comparison (also fixes
  the secondary stray-`)` issue on `Gramm.)`) and rejects any cleaned abbreviation
  longer than 10 characters.
- `_INVALID_LANG_ABBREVS` extended with `Language`, `Gramm`, `Divy`, `Nachträge`,
  `Uṇ`, the Sanskrit/Old Kannada titles, and the botanical genus names.

The v5 reparse surfaced eight more botanical genera not in the table below
(`Anaphilis`, `Avicennia`, `Leucas`, `Oryza`, `Phlomis`, `Phoenix`, `Polygala`,
`Stromatens`); these were added to the block-list as the same class of false
positive. After the fix the reparse diff shows **0 lost attestations** and **none**
of the listed false-positive tokens are gained.

Residual / deferred: single-letter dialect qualifiers (`P.`, `A.`) captured by
Pattern E (e.g. DED 2617, 1617) remain ambiguous and are left for the deferred
**Option C** (structural restriction to inside an outer `<b>` block) rather than
block-listed, since single letters can be genuine markers elsewhere.

## Summary

Pattern E (added in `burrow_entry_parser.py` to fix the Ko. missing attestation
bug) picks up capitalised non-language tokens as false language markers. These
pass `_is_valid_lang` because they start uppercase and are not in the
`_INVALID_LANG_ABBREVS` block-list.

Discovered during the v4 reparse of `burrow_corpus.cleaned.json` (418 entries
changed). All false positives are gains — no previously-working attestations
were removed — so there is no regression. The validator handles unmatched
language names gracefully, but they inflate attestation counts and add noise to
audit reports.

---

## Examples

| Token | Type | Affected DED entries |
|---|---|---|
| `Language` | Section heading artefact | 1180, 1611, 2130, 2147, 4112, 4312, 4377, 4630, 5301, 5549 |
| `Gramm.)` | Bibliographic abbreviation with stray `)` | 557, 895 |
| `Mahāpūrāṇa` / `Mahāpurāṇa` | Sanskrit text title | 718, 1110, 1684, 1992, 2248 |
| `Wrightiaantidysenterica` | Botanical species name (collapsed) | 1650 |
| `Jasaharacariu` | Old Kannada text title | 1265, 2248 |
| `Śabdaratnākara` | Sanskrit lexicon title | 2694, 4458 |
| `Yaśastilaka` | Sanskrit text title | 1379, 4482 |
| `Nachträge` | German supplement reference | 705 |
| `Ficus`, `Oxalis`, `Physalis`, `Tribulum` | Genus / species names | 2697, 4326, 3009, 2928 |
| `Uṇ.` / `Uṇ` | *Uṇādi-sūtra* grammatical text | 1242, 3367, 4971 |
| `Divy.` | *Divyāvadāna* (Buddhist Sanskrit) | 4838 |

The `Gramm.)` token also exposes a secondary issue: a stray `)` is being
included in the abbreviation, suggesting a bracket in the source HTML is not
being consumed by the optional-qualifier regex `(?:\([^)]*\)\s*)*`.

---

## Root Cause

`_is_valid_lang` in `burrow_entry_parser.py` currently only checks:

1. The abbreviation is not in `_INVALID_LANG_ABBREVS`
2. It is not empty
3. Its first character is uppercase

Patterns A–D rarely produce false positives because they require a `<b>`-wrapped
headword after the language marker, which happens to be a structural signal
exclusive to language attestations. Pattern E has no such structural requirement
— it matches plain text after `<i>...</i>`, which appears for bibliographic
citations and botanical references as well as language markers.

---

## Fix Options

### Option A — Extend `_INVALID_LANG_ABBREVS`

Add known false positives explicitly. Precise but requires manual curation as
new cases appear.

```python
_INVALID_LANG_ABBREVS = {
    ...,
    "Language",
    "Gramm",
    "Divy",
    "Nachträge",
    # Sanskrit / Old Kannada text titles
    "Mahāpūrāṇa", "Mahāpurāṇa", "Śabdaratnākara", "Yaśastilaka",
    "Jasaharacariu", "Uṇ",
}
```

### Option B — Add a length guard to `_is_valid_lang`

Real Dravidian language abbreviations are short (2–6 characters before the
period). Botanical genus names, text titles, and species strings are longer.

```python
def _is_valid_lang(abbrev: str) -> bool:
    clean = abbrev.rstrip(".").strip()
    if len(clean) > 10:          # "Wrightiaantidysenterica" is 23 chars
        return False
    ...
```

This alone would eliminate the longest false positives without manual curation.

### Option C — Restrict Pattern E to entries inside an outer `<b>` block

The false positive cases appear to occur in plain `<i>...</i>` text outside any
`<b>` context. Pattern E was designed for tokens *inside* an outer `<b>` block
opened by a grammatical qualifier. A positive lookahead or a two-pass approach
that only applies Pattern E within identified `<b>` spans would prevent it from
firing on free-standing citations.

This is the most targeted fix but requires more structural parsing logic.

### Recommended approach

Apply **Option B first** (length guard, low risk) combined with **Option A**
for the known short false positives like `Language`, `Divy.`, and `Nachträge`.
Option C can be deferred unless the false positive rate increases after a future
parser change.

---

## Fix Location

**File:** `src/dravidian/scripts/cross-validating-dded-starling/burrow_entry_parser.py`
**Function:** `_is_valid_lang` and/or `_INVALID_LANG_ABBREVS`

After the fix, re-run `reparse_burrow_corpus.py` to regenerate
`burrow_corpus.cleaned.json`. The v4 diff output can serve as a baseline to
confirm the false positive entries are no longer gained.
