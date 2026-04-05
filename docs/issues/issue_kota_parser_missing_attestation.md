---
type: issue
topics: [dravidian, cross-validating, burrow&ded, parser]
title: kota_missing_attestation_parser_bug
date: 04-05-2026
status: open
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

## Fix Needed

**File:** `src/dravidian/scripts/cross-validating-dded-starling/burrow_entry_parser.py`

Add a Pattern E to `_PATTERNS` (or a post-processing step) that handles:

```
<i>Lang.</i> headword   (plain text headword, no <b> wrapper)
```

when it appears inside an outer `<b>` context. One approach: after the existing span-finding, scan the raw text of each `<b>` block for embedded `<i>Lang.</i> form` sequences that weren't caught by the primary patterns.

After the parser fix, the repair script must be re-run to regenerate `burrow_corpus.cleaned.json`.
