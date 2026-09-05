---
name: burrow-parser-status-current-open-issues
description: "Burrow parser status — corpus at v16 (766011a, 2026-08-20) and in sync with the parser. Closed: Ko./Pattern-E/Tamil-missing/Naiki/Ma-type/Kuwi-type/leading-qualifier/nested-tag-bold/To.-type-bold (44287fc)/single-char-vowel (fc3104b)/Pattern-G (fae4cca). Open: To.-type running-text TEXT variant, Kui bare sense-suffix forms, DED 410/4896 duplicate entries"
metadata: 
  node_type: memory
  type: project
  originSessionId: 4c15ee36-3791-4871-bfb4-b07e45e829b0
  modified: 2026-09-05T00:00:00.000Z
---

## Corpus state (2026-09-05)

`burrow_corpus.cleaned.json` is at **v16** — last reparse `2026-08-20T12:28:29`, which is the
regen for `766011a` (committed 80 s later). 5,685 entries, 30,354 attestations, 4,804 repaired
glosses. **In sync with the parser**: `c5f78ff`, the final fix of the pilot, is matcher-side and
needs no regen. Version table: `docs/dravidian_validator_progress.md` §4.

**Closed since this file was last written:** To.-type bold-embedded marker behind scientific names
(`44287fc`), single-char vowel headwords (`fc3104b`), DED 1 Ta. Pattern-G (`fae4cca`), Naiki
`(Ch.)` between-marker qualifier (`5b4b16d`), four non-canonical-markup one-offs (`6f20bd2`),
Tamil comma-for-period marker (`766011a`).

**Still open:** the To.-type **running-text TEXT variant** (no `<b>` before `<i>`, incl. DED 5154's
own To. and elided-form DED 814 — needs a headword-quality guard the `<b>` anchor cannot provide);
Kui bare sense-suffix forms; DED 410/4896 duplicate entries.

**Box re-sync** for the v12–v16 regens is taken as done (2026-09-05); one more upload is planned
after the current session wraps. See `docs/SYNC.md` Tier 2.

---

## Closed — Ko. missing attestation (issue_kota_parser_missing_attestation.md)

**Fixed 2026-04-05.** Pattern E added to `burrow_entry_parser.py` (`_PATTERNS`, lines 86–95).
Recovers language markers embedded inside outer `<b>` blocks opened by grammatical qualifiers
(`fem.`, `pl.`, etc.). `burrow_corpus.cleaned.json` rebuilt as v4 using `reparse_burrow_corpus.py`.
DED #72 now has 11 attestations (Ko. `aṛy` recovered).

## Closed — Pattern E false positives (issue_pattern_e_false_positives.md)

**Fixed 2026-06-18.** `_is_valid_lang` now strips trailing `.`/`)`/`(` and rejects cleaned
abbreviations >10 chars; `_INVALID_LANG_ABBREVS` extended with `Language`, `Gramm`, `Divy`,
`Nachträge`, `Uṇ`, Sanskrit/Old Kannada titles, and botanical genera (Ficus/Oxalis/Physalis/
Tribulum plus 8 more surfaced by the v5 reparse: Anaphilis, Avicennia, Leucas, Oryza, Phlomis,
Phoenix, Polygala, Stromatens). v5 reparse diff: 0 attestations lost, all listed false-positive
tokens gone. Deferred (Option C): single-letter qualifiers `P.`/`A.` (DED 2617, 1617) still
captured — left for a future structural fix rather than block-listed.

## Closed — sub-entry marker glued to first language ("Tamil missing" systemic bug)

**Fixed 2026-06-18.** Lettered sub-entry markers `(a) (b) (c)…` are glued onto the
sub-entry-initial language inside one `<i>` tag in the DSAL HTML (e.g.
`<b><i>(a) Ta.</i> oru</b>`). `_LANG_CHAR` must start uppercase, so every `_PATTERNS`
entry silently skipped these spans — dropping mostly **Tamil** (196 of 246 glued spans
across 77 DEDs; Tamil is the sub-entry-initial language). Fix: added non-capturing
`_OPT_SUBENTRY = r"(?:\(\s*[a-z]\s*\)\s*)?"` right after the opening `<i>` in all four
patterns so `group(1)` captures the bare abbrev. (An earlier `_clean_lang_abbrev` /
`_SUBENTRY_MARKER_RE` edit was kept as harmless defense-in-depth but is not the real
fix — the marker-glued span was never captured for it to clean.) v6 reparse:
505 entries changed, **0 losses**, Ta. gained on 69 DEDs (140 attestations).

## Closed — Naiki dropped via dialect-qualified abbrev ("Nk. (Ch.)")

**Fixed 2026-06-18 (commit `f29ec78`).** The parser dropped every Naiki attestation. Burrow
marks Naiki as `Nk. (Ch.)` (vs. Naikri `Nk.`), but `_LANG_CHAR` (`[A-Z][a-z.()]*`) forbids the
internal space, so all four `_PATTERNS` stopped the abbrev at `Nk.` and failed on the ` (Ch.)`
before the headword — ~314 markers across the corpus in all three wrapper forms (`<b><i>` 259,
bare `<i>` 54, `<i><b>` 1). Fix: shared `_LANG_ABBREV` capture with an optional in-marker
qualifier `_OPT_LANG_QUALIFIER = r"(?:\s*\([^)<]*\))?"` in all four patterns; `_clean_lang_abbrev`
keeps the qualifier only when `normalize_burrow` recognises the full form (`Nk. (Ch.)` = Naiki),
else strips it to the base abbrev. The strip also recovers ~25 bibliographic-tag markers
(`Te. (SAN)`, `Ka. (DCV)`, `Ta. (lex.)`) as their base languages. v7 reparse: +327 attestations
(Nk. (Ch.) +298), **0 losses**; validator `entry_match_rate` 77.6% → 78.7% (+214 entries). See
[[triage-ded-ledger-status]].

## Closed — Ma-type nested-tag in headword bold span

**Fixed 2026-06-18 (commit `af90534`).** Pattern A required the headword run to end at `</b>`,
so any nested tag inside the headword's bold span broke `([^<]+)</b>` and silently dropped the
language (no fallback: Pattern E's `(?<!<b>)` blocks it, B/D needs a separate `<b>`). Three
markup flavors, one root cause: nested `<i>obl.</i>` qualifiers (DED 1), italicised scientific
names (DED 62 *Sphaeranthus indicus*), `<at>…</at>` encoding artifacts (DED 11). **613 dropped
languages / 703 attestations.** Fix: terminator `</b>` → `(?=<)` (stop the run at the next tag,
transparent for clean markup) + trim trailing `(` artifact in headword cleaning (431/703 needed
it). v8 reparse: +703, **0 losses, 0 headword-value changes** for already-parsed langs (faithful
in-memory sim). `entry_match_rate` 78.7% → **80.7%** (+392). See [[triage-ded-ledger-status]].

## Closed — Kuwi-type embedded forms via stray-bold-close pattern

**Fixed (commit `6eacbae`, before 2026-06-18 session that recorded this update).** Added
Pattern F: `<i>Lang.</i></b> ... <b>headword</b>` — language marker's `</i>` immediately
followed by a stray `</b>`, then after optional qualifiers a fresh `<b>headword</b>`. Recovered
the ~48 misses (Go. 16, Kuwi 12, Ga. 8) quantified below. This explains why the validator's
observed baseline at the start of the 2026-06-18 Kuwi-dialect-marker session (81.4%,
entries_matched 15,983) was already higher than this file's last recorded post-Ma-type number
(80.7%, 15,841) — the +142 gap is this fix's effect, shipped without a memory update at the time.

## Closed (bold variant) — To.-type embedded secondary-language forms

**Fixed 2026-06-19 (commit `ee716f1`).** Same family as Ma-type/Kuwi-type. A language abbrev
buried at the END of an `<i>` span (after lowercase qualifier or scientific-name text), with no
`<b>`/`<b>(` immediately before the `<i>` — the closing `</i>` lands *after* the abbrev, so no
anchored pattern (A/B/D/C/F/E, all needing the abbrev at the `<i>` start) could reach it; the whole
language was dropped. Two real shapes confirmed: sense-qualifier-before-abbrev (DED 5154
`…am<-> <i>incl.). To.</i>`) and scientific-name-before-abbrev (DED 5
`<i>Tu.</i> agase-mara <i>Agati grandiflora. Te.</i></b> (B) <b>agase-…`). Fix: new module-level
`_STRICT_PATTERNS` list (one bold-headword To.-type regex, lookbehind `(?<!<b>)(?<!<b>\()` +
`<i>[^<]*?[a-z][^<]*?` prefix scanning to the trailing `_LANG_ABBREV`) iterated in
`_find_all_lang_spans` AFTER `_PATTERNS`, gated by a NEW `_is_known_lang_abbrev` allow-list check
(stricter than `_is_valid_lang`). The allow-list gate is essential: because the pattern scans into
an `<i>` span it lacks the `<i>Abbrev` anchor, so without it italic citation titles / botanical
authorities (Volume, Sanskrit, Linn.) get captured as bogus languages (caught in the read-only
sim — they vanished once gated by `normalize_burrow` recognition). Read-only old-vs-new sim before
shipping: **+181 attestations / 134 DEDs, 0 losses** (Ma. 36, Ka. 24, Te. 17, Tu. 17, Go. 12,
Kol. 10, Ko. 10, To. 9, …), 1 surviving-headword improvement (DED 2366 Ta.). Corpus regen +183
total, 0 losses, qualified-id residue still 0. `entry_match_rate` 86.6% → **87.1%**,
`entries_matched` 17,003 → **17,098 (+95)**, not_attested 397→367, queue 4948→4894. The earlier
"~17/21 To.-type misses" estimate was a large undercount. See [[triage-ded-ledger-status]].

## OPEN — To.-type TEXT variant (deferred) + dakku

**Deliberately deferred** when shipping the bold variant above. The plain-text-headword shape
(headword bare text after `</i>`, not in its own `<b>` — e.g. **DED 5154's own To.**
`<i>incl.). To.</i> em</b>`) — ~54 candidates in the sim, ~47 clean but ~7 misparses where a form
is elided and the marker is followed directly by the English gloss (DED 814 Ma.
`<i>Calotropis gigantea. Ma.</i> gigantic swallow-wort…` → captured as a bogus headword). Needs a
**headword-quality guard** (reject English-gloss/multi-word/empty-gloss text captures) before the
text variant can ship safely. So the canonical DED 5154 To. is NOT yet recovered. Plus the original
**`dakku`** case (DED 3014/Kannada, real form buried in Ka. gloss). Parser-side → needs regen.

## Note — Kuwi/Kui *dialect-citation* markers are a separate, matcher-side issue

Not to be confused with the parser-side "Kuwi-type embedded forms" above. DED 5154 also surfaced
a distinct bug: Burrow's single consolidated `Kuwi` attestation cites multiple dialect sources
inline (`(F.) mārrō`, `(S. Isr.) māro`, etc.), but the **matcher** only compared against the one
stored headword, so Starling's per-dialect entries (Kuwi Fitzgerald/Schulze/Israel) mismatched on
their incl.-sense forms. Fixed 2026-06-18 by generalizing `GONDI_INLINE_ABBREVS` in
`dialect_mapping.py` to `KUWI_INLINE_ABBREVS`/`KUI_INLINE_ABBREVS` — see
[[triage-ded-ledger-status]] for the full writeup and impact numbers. Matcher-side, no parser
change, no corpus regen.

## Closed — leading qualifier glued onto the front of a language abbreviation

**Fixed 2026-06-18 (commit `32aeb26`).** Burrow glues grammatical/sense qualifiers
-- `(tr.)`, `(intr.)`, `(loc.)`, occasionally a botanical name -- onto the FRONT of
the next language's abbreviation inside the same `<i>` tag, e.g.
`<b><i>(loc.). Ka.</i> akkuḷisu</b>` (DED 25: `Ka.` was glued into `Ta.`'s gloss
text). `_LANG_CHAR` must start uppercase, so no `_PATTERNS` entry could anchor --
same failure family as Naiki/Ma-type. Found while sizing the (deferred) To.-type
bug below: a corpus-wide structural scan distinguishing this shape from To.-type
found **97 confirmed misses** (95 with the qualifier's `(` inside `<i>`, e.g.
`<i>(tr.). Ka.</i>`; 2 with it leaking just outside, e.g. `<b>(<i>intr.). Ka.</i>`).
Fix: new `_OPT_LEADING_QUALIFIER = r"(?:\(?[^()<>]*\)\.\s*)?"` consumed right
after `<i>` in Pattern A only (this bug's shape is exclusively Pattern A's), plus
an optional `\(?` right after `<b>` for the paren-leaks-outside variant. v9
reparse: **+124 attestations, 0 losses, 107 entries changed** (the script's own
printed diff is cumulative since the raw scrape -- see
[[triage-ded-ledger-status]] for why; true incremental count came from diffing
the pre/post `cleaned.json` files directly). `entry_match_rate` 84.3% → **84.6%**
(+75 entries). One known residual: DED 1147's `(a)). Ka.` has a doubly-nested
cross-reference paren the simple shape doesn't cover -- accepted, not fixed.

## Closed — headword-qualifier glued onto a single-bold-span language marker

**Fixed 2026-06-18 (commit `e6082c2`).** Diagnosed the ~126-case lead noted below
(now resolved as this section). Pattern A's headword capture
(`</i>\s+([^<]+)(?=<)`) matched `Go.`/`Kuwi` fine in DED 107/83
(`<b><i>Go.</i> (Tr.) aḍrai id.</b>`, `<b><i>Kuwi</i> (S.) aḍḍe ānai</b>`), but
its capture group greedily swallowed the `(Tr.)`/`(S.)` dialect-citation
qualifier into the headword text (no separating tag to stop at, since lang
marker and headword share one `<b>` span). The downstream headword-cleanup
filter then dropped the WHOLE attestation because it drops any string starting
with `(` as a bogus qualifier-only token -- not a regex-anchoring miss like
Naiki/Ma-type, a headword-cleaning miss. Fix: shared `_OPT_HEADWORD_QUALIFIER`
constant (the qualifier-skip already used by Patterns B/C/F) added to Pattern A
-- but with a **mandatory** `\s+` boundary kept before it, not `\s*`. Patterns
B/C/F are safe with `\s*` because they require a literal `<b>` anchor right
after; Pattern A's capture has no such anchor, so an initial `\s*` version
matched with **zero** whitespace too and introduced false-positive "languages"
from italicised non-language tokens glued directly to following punctuation
with no space: `Artocarpus` (DED 15), `Cyprinus` (DED 1252), `Grammar` (DED
1303, a citation work title), `Nāyadh.` (DED 4531). Caught via the corpus-regen
diff (gained-but-suspicious abbrevs) before shipping, corrected to `\s+`, then
verified the four false positives disappeared while Go./Kuwi recovery held.
v10 reparse: **+63 attestations / 59 entries, 0 losses** (true incremental,
diffed pre/post cleaned-corpus files directly). `entry_match_rate` 84.6% →
**84.9%**, `entries_matched` 16,615 → **16,657 (+42)**. See
[[triage-ded-ledger-status]].

**Lesson:** when reusing a shared regex sub-pattern across multiple
`_PATTERNS` entries, check whether each pattern has a literal anchor tag
downstream of the optional piece. A `\s*`-led qualifier-skip is safe right
before a required literal tag (match just fails if absent); the same `\s*`
is unsafe right before an unanchored `[^<]+` capture, since it can start
matching with zero boundary and run into adjacent prose.

## Closed — nested grammatical-qualifier tag inside Pattern B/D, C, F headwords

**Fixed 2026-06-18 (commit `791f89b`).** Same root-cause family as the Ma-type
fix above (`af90534`), but that fix only patched **Pattern A**. Patterns C,
B/D, and F still required the headword to end at a literal `</b>` with zero
tags inside (`<b>([^<]+)</b>`), so a nested grammatical qualifier mid-headword
(`<i>pl.</i>`, `<i>obl.</i>`) broke the match and silently dropped the
language entirely. Surfaced via DED 5440's `Kuwi`
(`<i><b>Kuwi</b></i> (F.) <b>vegū (<i>pl.</i> veska)</b>` — Pattern C shape);
confirmed systemic via DED 1's `Go.` (`<i><b>Go.</b></i> (Tr.) <b>ōr/ōl
(<i>obl.</i> ōn-), <i>pl.</i> ōṛ/ōṛk</b>`), which had the identical shape and
was still missing.

Pattern A's existing fix (`(?=<)` lookahead) does NOT transfer to these three
patterns: Pattern A's headword has no closing tag of its own to lean on, but
B/D, C, F's headword sits in its OWN bounded `<b>...</b>` span, so a naive
`(?=<)`-style swap truncates real headword content that comes *after* the
nested tag closes. Fix: new `_HEADWORD_SPAN_ACROSS_NESTED = r"(?:[^<]|<(?!/b>)
(?!i>" + _LANG_CHAR_FIRST + r"))+"` spans across the nested tag-pair instead
of stopping at it, then strips the nested-tag markup via `_HTML_TAG_RE` during
headword cleanup.

**Caught a regression before shipping** (same discipline as the
headword-qualifier-after-lang-marker fix's `\s*`→`\s+` correction): a first
version without the `(?!i>UPPERCASE)` guard spanned across *any* non-`</b>`
tag, including a DIFFERENT language's own `<i>NextLang.</i>` marker, whenever
the preceding language's headword span didn't close until after it — DED 4900:
`<b>muŋgi pōtu. <i>Go.</i></b>` swallowed `Go.` whole into a bogus `Ga.`
headword, losing `Go.` entirely (also DED 5161). Caught via a full-corpus
old-vs-new parser comparison (2 losses) before regenerating the real corpus.
Fixed by adding the second negative lookahead, since grammatical qualifiers
are always lowercase and language abbreviations always start uppercase.

v11 reparse: **+163 attestations / 121 entries, 0 losses** (true incremental,
diffed pre/post cleaned-corpus files directly). `entry_match_rate` 84.9% →
**86.6%**, `entries_matched` 16,657 → **17,003 (+346)** — a much bigger
validator jump than the corpus delta alone suggests, because recovered
Go./Kuwi/Konḍa attestations each satisfy many Starling per-dialect rows at
once (Gondi has 17 tracked dialects, Kuwi has 7). See
[[triage-ded-ledger-status]].

**Lesson:** "span across a nested tag instead of stopping at it" is not
inherently safe just because the outer literal closing tag is unambiguous —
check what KINDS of things can legitimately appear nested. Here, the same
`<i>...</i>` shape is used for both grammatical qualifiers (safe to span) and
a different language's own marker (must NOT be spanned), and the two are only
distinguishable by a content-level signal (lowercase vs. uppercase-initial),
not a structural one.

## Closed — single-char vowel headwords dropped (Phase 2)

**Fixed 2026-08-17 (commit `fc3104b`, dravidilex-pilot branch).** The `len(hw) > 1`
headword filter in `parse_language_sections` (was ~line 349, now ~line 611) dropped
legitimate single-character Dravidian headwords, discarding the headword LANGUAGE
wholesale on ~20 deictic/pronominal-base entries whose whole chain reduces to single
vowels (DED 1 `a`, 328, 332, 334, 410 `i`, 533, 534, 557 `u`, 606, 728 `ū`, 764, 827,
870 `ē`, 3684, 3720, 5160). Fix chose a **vowel whitelist** over the broader
`hw.isalpha()` proposed earlier: new module-level `_VOWEL_HEADWORDS = frozenset("aāiīuūeēoō")`,
guard becomes `len(hw) > 1 or hw in _VOWEL_HEADWORDS` (single consonants/stray letters stay
filtered as noise). Read-only sim + corpus regen: **+23 vowel-headword slots, 0 attestations/
languages lost** (the 4 diff "losses" were before-images of attestations that GAINED a vowel,
e.g. DED 328 Ta. `['aṃ']`→`['a','aṃ']`). Validator `entries_matched` 18185→**18190 (+5)**,
`entry_match_rate` **94.0% held**. Modest +5 because many recovered attestations still differ
from Starling by the pending `r̤`/`ẓ` transcription and land at "Language only". See
[[triage-ded-ledger-status]].

**Closed (follow-up) — DED 1's own Ta. headword (commit `fae4cca`).** The whitelist recovered
DED 1's Ma. (`<b><i>Ma.</i> a, ā</b>`, Pattern A) but NOT its Ta.: the leading marker is
`<i><b>Ta.</b></i>` (Pattern-C-style lang) followed by a **plain-text** headword `a`, not a
`<b>`-wrapped one. No existing `_PATTERNS` entry covered `<i><b>Lang.</b></i>` + plain-text
headword (Pattern C needs `<b>headword</b>`; Pattern E needs a bare `<i>Lang.</i>`). Added
**Pattern G** to `_PATTERNS`: `<i><b>Lang.</b></i>\s+(lone-vowel chain)(?=[\s.;])`. A lone
vowel is the quality guard (English prose is never a single vowel), so it cannot capture the
following gloss; **only 1 match corpus-wide** (DED 1 Ta. `a`). This is the safe, narrow subset
of the deferred To.-type TEXT variant — the general plain-text-headword case is still open (see
that section). Corpus regen +1, 0 losses.

## OPEN flag 2 — DED 410 has duplicate corpus entries

`burrow_corpus.cleaned.json` holds **4 entries all numbered 410** (edition DEDR, page 38)
with cumulatively growing attestation counts (13/25/35/44 = sub-entry a / a+b / a+b+c /
a+b+c+d). Data-quality duplication from the scrape/repair, independent of the parser
fix above. Worth a dedicated dedupe pass.

## Corpus state

- `burrow_corpus.json` — raw scrape, source of truth, never modified
- `burrow_corpus.cleaned.json` — currently **v11**, rebuilt 2026-06-18 (nested-tag-headword-
  bold-bcf fix; +163 attestations / 121 entries, 0 losses. v10 was the headword-qualifier-
  after-lang-marker fix; v9 was the leading-qualifier-glued-abbrev fix; v8 was the Ma-type
  nested-tag-in-headword fix.)
- Rebuild command (from repo root):
  `python src/dravidian/scripts/cross-validating-dded-starling/reparse_burrow_corpus.py data/dravidian/burrow_ded/burrow_corpus.json --output data/dravidian/burrow_ded/burrow_corpus.cleaned.json`

**Why:** `reparse_burrow_corpus.py` was added because `repair_burrow_corpus_glosses.py` only patches
existing attestation glosses — it cannot add attestations the parser originally missed.
