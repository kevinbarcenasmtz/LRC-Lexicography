---
type: issue
topics: [dravidian, cross-validating, burrow&ded]
title: gondi_dia_cv_issue
date: 04-04-2026
status: resolved
---

**Summary**

Cross-validation false negatives for Gondi dialect entries in DED #72. Two specific rows flagged as `Language only (conf: 0.95)` (headword mismatch) when the headword data is actually present in the Burrow corpus.

**Affected entries:**
- Koya Gondi `aṛgi` "underneath"
- Maria Gondi (Mitchell) `aḍ(ḍ)i` "below, low"

**Reference Data (DED #72 excerpts)**

**Burrow Go. attestation (from `burrow_corpus.cleaned.json`):**
```
language_abbrev: "Go."
headwords: ["aḍi"]
gloss: "beneath; (Mu.) aḍit below; aḍita lower; aṛke below; (Ma.) aḍita, aḍna lower; (M.) aḍ(ḍ)i below, low; (L.) aḍī down; (Ko.) aṛgi underneath; aṛgita lower ( Voc. 33)."
```

**Burrow source text (DED website):**
```
Go. (G.) aḍi beneath; (Mu.) aḍit below; aḍita lower; aṛke below;
(Ma.) aḍita, aḍna lower; (M.) aḍ(ḍ)i below, low; (L.) aḍī down;
(Ko.) aṛgi underneath; aṛgita lower (Voc. 33).
```

**Starling dialect entries (from starling json, `_depth: 2` under Proto-Gondi-Kui):**
- Gommu Gondi: `aḍi` "beneath"
- Muria Gondi: `aḍit` "below"
- Maria Gondi: `aḍita, aḍna` "lower"
- Koya Gondi: `aṛgi` "underneath"
- Maria Gondi (Mitchell): `aḍ(ḍ)i` "below, low"
- Maria Gondi (Lind): `aḍī` "down"

---

**Root Cause**

Burrow encodes all Gondi dialect forms inside a single `Go.` attestation using inline dialect sigilla from *Voc.* (Burrow & Bhattacharya 1960). The validator resolves these via `get_inline_abbrevs_for_starling_dialect()` → `GONDI_INLINE_ABBREVS` in `dialect_mapping.py`. Two bugs in that map:

**Bug 1 — Koya Gondi (`Ko.`) not in `GONDI_INLINE_ABBREVS` **

`get_inline_abbrevs_for_starling_dialect("Koya Gondi")` returns `[]`.
The gloss-dialect extraction block is skipped. `(Ko.)aṛgi` in the Go. gloss is never consulted.
Falls through to `language_only`.

`Ko.` is absent because it also abbreviates Kota at the top-level attestation layer, but within a `Go.` gloss context `Ko.` unambiguously means Koya Gondi.

**Bug 2 — Maria Gondi (Mitchell) mapped to wrong sigil (`Ma.` instead of `M.`)**

`get_inline_abbrevs_for_starling_dialect("Maria Gondi (Mitchell)")` returns `["Ma."]`.
The validator extracts the `(Ma.)` group from the gloss, finding `aḍita` (plain Maria form).
Compares `aḍita` against `aḍ(ḍ)i` — no match.

The correct sigil for Mitchell's Maria is `M.` (A. N. Mitchell, *A Grammar of Maria Gondi*, 1942),
confirmed in §31 of the DEDR frontmatter. `Ma.` = plain Maria Gondi only.
`M.`, `L.`, `G.`, `Ko.` are all missing from `GONDI_INLINE_ABBREVS`.

---

**Source authority: DEDR frontmatter §31 **

Confirmed sigilla from *Voc.* (Burrow & Bhattacharya 1960) and other §31 sources:

| Inline sigil | Source                           | Dialect / Notes                                  |
| ------------ | -------------------------------- | ------------------------------------------------ |
| `Tr.`        | Chenevix Trench (1919-21)        | Betul Gondi                                      |
| `W.`         | H. D. Williamson (1890)          | Mandla dialect                                   |
| `L.`         | Abraham A. Lind (1913)           | Maria dialect                                    |
| `M.`         | A. N. Mitchell (1942)            | Maria Gondi, Bison Horn/Dandami Marias of Bastar |
| `Pat.`       | S. B. Patwardhan (1935)          | Chanda dialect                                   |
| `SR.`        | P. Setumadhava Rao               | Adilabad dialect                                 |
| `A`          | Burrow & Bhattacharya fieldnotes | Adilabad, 1951                                   |
| `G.`         | Stephen A. Tyler (1969)          | Gommu dialect of Koya                            |
| `Ko.`        | DGG (Subrahmanyam 1968)          | Koya Gondi                                       |
| `Mu.`        | —                                | Muria Gondi (already correct)                    |
| `Ma.`        | —                                | Maria Gondi only (Hill-Maria per §55)            |

`Ph.` (Phailbus, Mandla) is **not listed in §31** and should be flagged as uncertain.

`Voc.` = CVOTGD = *A Comparative Vocabulary of the Gondi Dialects*, JAS 2.73-251 (1960).
"Number in CVOTGD: 33" in Starling = `Voc. 33` in the DED entry.

---

## Fix Plan

**File:** `src/dravidian/scripts/cross-validating-dded-starling/dialect_mapping.py`

1. Add missing entries to `GONDI_INLINE_ABBREVS`:
   - `"Ko.": ["Koya Gondi"]`
   - `"M.": ["Maria Gondi (Mitchell)"]`
   - `"L.": ["Maria Gondi (Lind)"]`
   - `"G.": ["Gommu Gondi"]`

2. Fix the `Ma.` entry — remove Mitchell, Lind, Smith variants:
   - `"Ma.": ["Maria Gondi"]` (only)

3. Flag `Ph.` as uncertain (not in §31; keep but add comment).

After updating `dialect_mapping.py`, re-run the cross-validation to confirm the two rows now resolve to full headword matches.

---

## Resolution

**Fixed in:** `dialect_mapping.py`, `repair_burrow_corpus_glosses.py`, `starling_tree_validator.py`

- `GONDI_INLINE_ABBREVS` corrected: added `Ko.`, `M.`, `L.`, `G.`; fixed `Ma.` to plain Maria only
- `_recover_attestation_gloss_from_full_text` updated in both repair script and validator to use a regex marker search that tolerates parenthetical qualifiers (e.g. `Go. (G.) aḍi`) between abbreviation and headword — this fixed the garbled Go. gloss spacing
- `burrow_corpus.cleaned.json` regenerated; Go. gloss for DED #72 now properly spaced
- Both failing rows no longer appear in the validation audit report

**Remaining related issue:** Ko. (Kota) attestation for DED #72 is still missing from the corpus due to a separate parser bug (`fem.` qualifier trapping Ko. inside Ma.'s HTML block). Tracked in `docs/issues/issue_kota_parser_missing_attestation.md`.
