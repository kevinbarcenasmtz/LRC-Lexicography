# DED↔Starling unmatched-entry diagnostics (per-language)

Diagnosis of the remaining unmatched StarlingDB↔Burrow-DED attestations, produced
2026-08-18/19 by working the failing rows one Starling-language group at a time
(largest first). A *failure* = a `tree_validation_results.csv` row whose `Match`
starts with `No` or `Language only`; total was **585 unmatched rows across 52
language groups** at match rate 97.0% (18,769 matched). Diagnosis-only: no code,
corpus, ledger, or commit was changed in producing this.

**Coverage: 19 / 52 groups (415 / 585 rows) — the highest-volume groups.**
Remaining pending (33 groups, ~170 rows), next up: Kuwi (Schulze), Telugu
(Krishnamurti), Sunkarametta Kuwi, Salur Gadba, Betul Gondi, Inscriptional Telugu…
Live worklist/status: `data/dravidian/cross-validating-dded-starling/tree_validation_output/loop_worklist.json` (gitignored).

## Cross-cutting systemic levers (recurring across groups)

- **Plain-text / non-canonical language markers** (parser, `burrow_entry_parser.py`)
  — biggest single bug: a `Tu.`/`Ma.`/`Pa.`/`Koḍ.`/`Konḍa` marker not in the
  canonical `<b><i>Lang.</i> hw</b>` shape is swallowed into the prior gloss.
  Hits **Konda, Malayalam, Tulu, Malto, Parji, Kodagu**. Structural → needs regen.
- **Sci-binomial boundary** (parser) — a language marker right after an italic Latin
  binomial + period (DED 3824 alone recurs in Malayalam/Tulu/Kannada). → regen.
- **Gloss-buried sub-dialect forms** (matcher-side, `gloss_extraction.py`) — forms
  tagged `(ASu.)`/`(Koya Su.)`/`(Mu.)`/`(Isr.)` inside a consolidated Go./Kuwi gloss;
  extractor needs marker-after-form order, non-ASCII/`p.###` markers, multi-word
  forms, all-`Go.`-segment scan. Hits Adilabad/Koya/Muria Gondi + both Kuwi. Zero-regen.
- **Small transcription folds** (matcher-side, `textnorm.py`) — comma-in-parenthetical
  `(-pp-, -tt-)`, `/`-spacing, schwa/open-o/ɨ codepoints, glottal-space. Zero-regen.
- **Mapping aliases** (matcher-side, `dialect_mapping.py`) — trailing-variant keys
  `Kui.`, `Kod`, `Ko..`, `Konḏa.`, `MuE./MuW.`. Zero-regen.
- **Genuine divergences** — Irula (10/10) and scattered rows Burrow never published;
  no fix possible.

**Open decision:** the two Kuwi groups can be fixed either by splitting Kuwi
sub-sources `(F.)/(S.)/(Su.)/(Isr.)` structurally (regen, fixes both groups) OR by
matcher-side gloss extraction (zero-regen). Decide before implementing.

---

## Adilabad Gondi (64 rows)

**Dominant verdict:** Systemic **parser_miss** — 10/10. Starling's "Adilabad Gondi" is exactly Burrow's `(ASu.)` / `(SR.)` Gondi sub-forms, which are present in `full_text` but buried in the Go. gloss and never broken out as attestation headwords, so the matcher (correctly resolving Go.=Adilabad Gondi at conf 0.95) falls back to the first Go. form and reports a spurious headword mismatch.

| ded | class | evidence (token in full_text vs. matched) |
|---|---|---|
| 340 | parser_miss | Go. gloss `(ASu.) āg- to stop, stand`; only `āng-` stored → matched `āng-` |
| 859 | parser_miss | `(SR.) ertānā to thrash`; only `ērīstānā` stored → matched `ērīstānā` |
| 2023 | parser_miss | `(Tr. W. SR.) kai id.`; only `kay` stored → matched `kay` (Starling hw literally `kai (SR)`) |
| 1780 | parser_miss | `(ASu.) kurkūm pustule, blister, boil`; only `kurpum` stored → matched `kurpum` |
| 5082 | parser_miss | `(SR.) miṭānā to apply`; only `maṛhuttānā` stored → matched `maṛhuttānā` (Starling hw `miṭānā (SR)`) |
| 2906 | parser_miss | `(SR.) nāḍvānj water worms`; only `narwānj` stored → matched `narwānj` |
| 3821 | parser_miss | `(ASu.) pahānā wet; raw, green`; only `pahna` stored → matched `pahna` |
| 4167 | parser_miss | `(ASu.) pihk- id.; pihkā fart`; only `pittānā` stored → matched `pittānā` |
| 2303 | parser_miss | `(ASu.) seṭṭā shoulder-blade`; only `saṭṭā` stored → matched `saṭṭā` |
| 3450 | parser_miss | `(SR.) tāhānā to lift`; only `tēd-` stored → matched `tēd-` |

**Proposed fix:** `burrow_entry_parser.py`, in `parse_language_sections` (per-span loop, ~L782-810): after building the primary `headwords` from the bold chain, run a secondary harvest over `gloss_text` for qualifier-prefixed forms — regex `\((?:ASu\.|SR\.|Tr\.|Ph\.|Ko\.|Mu\.|Koya\s*Su\.|W\.|G\.|Ma\.|M\.)\s*\)\s*(\S+)` — and append captured forms to `att.headwords` (or a new `secondary_headwords` list that `find_matching_attestation` at L875 also iterates). A dialect_mapping-only change cannot fix it since the target token is absent from `att.headwords`; the language mapping (Go.→Adilabad Gondi) is already correct.

## Toda (61 rows)

**Dominant-pattern verdict:** transcription_divergence — 9/10 sampled DEDs matched Toda at the language level (conf 1.00) and differ only in the Toda transcription system (Starling `ɫ̣ / ɵ / ō ē ā / _` vs Burrow `ł̣ / ḷ / o· e· a· / ·`, plus OCR "Q"→ɵ and "ζ"→ʒ artifacts in the cached corpus). Only DED 895 is a true parser_miss.

| ded | class | evidence |
|-----|-------|----------|
| 295 | transcription_divergence | Star `aɫ̣- (aɫ̣ɵ-)` vs Bur `ał̣- (ał̣…)`: `ɫ̣`↔`ł̣`, and `ɵ` stored as the "Q" OCR artifact (`ał̣ Q -`) which also truncates the match |
| 2562 | transcription_divergence | Star `twāloʒn` vs Bur `twa·loʒn.` (`ā`↔`a·`; ezh `ʒ` stored as Greek zeta `ζ` → `twa·loζn.`; trailing `.`) |
| 895 | parser_miss | full_text has `To. ö Q k- (ö Q ky-) to jump.` but the To. segment is absent from both stored and reparse attestations |
| 1274 | transcription_divergence | Star `kōɫ̣` vs Bur `ko·ł̣` (`ō`↔`o·`, `ɫ̣`↔`ł̣`); leading `?` qualifier didn't block language match |
| 1818 | transcription_divergence | Star `kwēɫ̣` vs Bur `kwe·ḷ` (`ē`↔`e·`, `ɫ̣`↔`ḷ`) |
| 4932 | transcription_divergence | Star `-mil muṭy` vs Bur `-milmuṭy` — differ only by an internal space |
| 3748 | transcription_divergence | Star `nötš, nets_` vs Bur `nötš/nets̱` (`,`↔`/` separator; combining macron-below `s̱` rendered as `nets_`) |
| 4322 | transcription_divergence | Star `püɫ̣y` vs Bur `püḷy` (`ɫ̣`↔`ḷ`) |
| 3245 | transcription_divergence | Star headword `teṣ t/ɵwɨ̄r_ (?)` is a To. sub-lexeme buried inside Burrow's To. segment body; matcher matched the primary headword `tï·k- (tï·ky-)` instead (`ɵ`=Q, `ɨ̄`≈`ï·`) |
| 5313 | transcription_divergence | Star `paɫ̣f- (paɫ̣t-)` vs Bur `paḷf- (paḷt-)` (`ɫ̣`↔`ḷ` only) |

**Proposed fix:** No systemic parser/mapping fix — genuine Toda transcription divergences (Starling `ɫ̣/ɵ/ō-macron-length/_`-macron-below` vs Burrow `ł̣/ḷ/o·-mid-dot`, plus corpus OCR artifacts `Q`=ɵ and `ζ`=ʒ). If recovery is wanted it belongs in `textnorm.py::normalize_for_match` as a Toda fold (`ɫ→ḷ`, normalize `ɵ`↔`Q`, `ō/ē/ā`↔`o·/e·/a·`, strip internal spaces), not the parser or dialect map. The lone parser_miss (DED 895) is an isolated edge case: To. headword `ö Q k-` — single-char glyph `ö` (not in `_VOWEL_HEADWORDS`) + the `Q` artifact — yields no surviving headword token, so the segment is dropped at `if not headwords: continue` in `parse_language_sections`.

## Telugu (56 rows)

**Dominant-pattern verdict:** transcription_divergence — 7/10 sampled rows are Burrow's intra-word optional-consonant parenthetical `X(r)Y` (Starling resolves it to `XrY` *or* `XY`); `normalize_for_match` turns `(`/`)` into spaces, so `t(r)ampi` → `"t r ampi"` matches neither `tampi` nor `trampi`. One parser_miss (non-italic lang marker) and one genuine_divergence round out the group.

| ded | class | evidence (Starling hw vs Burrow) |
|-----|-------|------|
| 74 | parser_miss | full_text has `Te. aḍalu`, but raw_html marks it plain-text `Te.` + plain-bold `Ka. aḍalu` (non-italic); parser splits only on `<i>lang.</i>`, so Ka.+Te. fold into the Ta. gloss — stored/reparse hold only Ta.+Kol. Note: "Telugu not in DED 74". |
| 3451 | transcription_divergence | Starling `trē̃cu` vs Burrow 2nd Te. form `t(r)ē̃cu`; matcher wrongly bound to 1st Te. form `dēvu`. `(r)` notation. |
| 5151 | genuine_divergence | Starling abstracts base `ē`; Burrow Te. lists only compounds `evãḍu/ēvã̄ḍu/…/etãḍu` — no standalone `ē` headword. |
| 1822 | transcription_divergence | `kuḷḷu` vs `k(r)uḷḷu` — Starling dropped the optional r. |
| 4866 | transcription_divergence | `mriŋgu` vs `m(r)iṅgu` — optional r kept + ŋ/ṅ nasal (`(r)` blocks match). |
| 3793 | transcription_divergence | `noccu (novv-/nōv-/nō-)` vs `noccu (novv-/ nōv-/nō-)` — differ only by a stray space after the first slash. |
| 4430 | transcription_divergence | `prēlu` vs `p(r)ēlu` — optional r kept. |
| 3115 | transcription_divergence | `tampi` vs `t(r)ampi` — optional r dropped. |
| 3367 | transcription_divergence | `turugu (B)` = Burrow in-gloss `(B.) turugu` (inside `tuṭṭe` Te. gloss); matcher bound to first form `tuṭṭe`. |
| 5368 | transcription_divergence | `vrālu` vs `v(r)ālu` — optional r kept. |

**Proposed fix:** Systemic, matcher-side (zero regen). In `textnorm.py`, add `optional_consonant_variants(form)` detecting intra-word single-letter parenthetical `X(c)Y` (one alphabetic letter flanked directly by word chars, no adjacent space) → yield BOTH `XcY` and `XY`; guard so dialect qualifiers `(K.)`/`(B.)`/`(S.)` and space-delimited alternative blocks like `(novv-/ nōv-/nō-)` are untouched. Wire into `starling_tree_validator.py` at Burrow-form compare sites (`bhw_norm` ~L444, `form_norm` ~L505, `segment_norm` ~L657). Resolves 3115/4430/4866/1822/5368/3451 (6 rows). DED 74 is a separate parser fix (recognize non-italic/plain-bold `Ka.`/`Te.` markers); 3793 (stray space around `/`), 3367 (in-gloss `(B.)`), 5151 (genuine) out of scope.

## Koya Gondi (32 rows)

**Dominant-pattern verdict: `mapping_gap` (10/10).** Every Koya Gondi form Starling cites *is present* in Burrow, nested inline inside the consolidated **Go. (Gondi)** gloss under a Koya sub-dialect qualifier — `(Koya Su.)`, `(Koya T.)`, or `(Ko.)`. The validator already maps Go. → Koya Gondi (conf 0.95, "language matched") but can't surface the specific Koya form, so it falls back to the Gondi headword and reports a headword mismatch. Root cause: spelled-out `Koya Su.`/`Koya T.` are **not registered** in `GONDI_INLINE_ABBREVS` (only short sigil `Ko.` is), and are additionally rejected by the dialect-marker regex before the abbrev match is reached.

| ded | class | evidence (Starling hw ↔ Burrow in-gloss token, buried in Go. gloss) |
|-----|-------|--------------------------------------------------------------------|
| 131  | mapping_gap | Starling `annāl (Su.)` ↔ `(Koya Su.) annāl id.`; matcher chose Go. head `tannāl` |
| 905  | mapping_gap | `ēn-` ↔ `(Koya Su.) ēn- to receive…`; chose `ētānā` |
| 1407 | mapping_gap | `gansk-` ↔ `(Koya Su.) gansk- to dream`; chose `kanckānā` |
| 1425 | mapping_gap | `kākāḍ (T)` ↔ `(Koya T.) kākāḍ id.`; chose `kākaṛ` |
| 4764 | mapping_gap | `merka` ↔ `(Koya Su.) merka female young of goat`; chose `marrī` |
| 4981 | mapping_gap | `muṭō, moṭō (Su.)` ↔ `(Koya Su.) muṭō, moṭō id.`; chose `mur maṛā` |
| 990  | mapping_gap | `onḍ` ↔ section-(d) Go. gloss `(Ko.) onḍ one`; chose `oror` — sigil registered but form is in a different Go. sub-block than the best-matched attestation |
| 4452 | mapping_gap | `boḍga (Su.)` ↔ `(Koya Su.) boḍga id.`; chose `pohpī` |
| 2411 | mapping_gap | `alla (Su.)` ↔ `(Koya Su.) alla id.`; chose `hulla` |
| 689  | mapping_gap | `dus-` ↔ `(Koya Su.) dus- to comb hair`; chose `uṛ-` |

Not parser_miss (token text is retained in the stored Go. gloss) and not transcription_divergence (each Starling headword is an *exact* string match to its in-gloss Koya token; mismatch is only against the wrong Gondi headword).

**Proposed fix (systemic — matcher-side, no corpus regen):**
1. `dialect_mapping.py` → `GONDI_INLINE_ABBREVS` (~L525-538; feeds `get_inline_abbrevs_for_starling_dialect` → `extract_gloss_forms_for_abbrevs`): register spelled-out Koya qualifiers next to existing `"Ko.": ["Koya Gondi"]` — add `"Koya Su."`/`"Koya T."` (or generic `Koya`) → `["Koya Gondi"]`.
2. `gloss_extraction.py` → `_DIALECT_MARKER_GROUP_RE` (~L68, `^(?:[A-Za-z]+\.)+(?:\s+[A-Za-z]+\.)*$`): widen to accept a leading bare word `Koya` before the dotted sigil; as written it rejects `Koya Su.`/`Koya T.` (first token has no trailing dot), discarding those parentheticals before the abbrev-key comparison in `extract_gloss_forms_for_abbrevs` (~L127-216).
3. DED 990 caveat: its `(Ko.) onḍ` lives in the entry's `(d)` sub-block, a *different* Go. attestation than the best-matched one — so the inline scan (currently on `best_att`'s gloss in `starling_tree_validator.py` ~L470-520 `gloss_dialect_exact` path) must also iterate the other Go. attestation segments (or full_text), or 990 still misses after 1-2.

## Kui (25 rows)

**Dominant-pattern verdict:** MIXED — no single systemic pattern dominates, but the largest actionable cluster is **parser_miss / surfacing gaps** (4 rows: Kui form sits in Burrow's `full_text` but is never carved into a matchable Kui attestation), plus one clean **mapping_gap** (837). The other 5 rows are non-systemic (2 transcription, 2 genuine, 1 Starling-side artifact).

| ded | class | evidence |
|-----|-------|----------|
| 837 | mapping_gap | Kui attestation stored fine (`ḍīmbu`, equal to Starling) but its `language_abbrev` is `"Kui."` **with trailing period**; `match_languages("Kui.", …)` does `_BURROW_TO_INFO.get("Kui.")` → miss → note "Kui not in DED 837". Map key is bare `"Kui"`. |
| 410 | parser_miss | Starling head `ī`; Burrow Kui heads `ianju, iaru, īri`. `ī` is in full_text but only inline as `"…adj. ī; imba here…"` — not extracted (also single-vowel base, likely filtered). |
| 2064 | other | Starling lexical headword is literally `"hoe"` (the English gloss, not a form); Burrow correctly has Kui `(K.) koḍi hoe`. Starling-side record artifact. |
| 1807 | genuine_divergence | `no page URL found for DED 1807` — DED 1807 absent from Burrow corpus entirely. |
| 1123 | parser_miss | Starling wants `ḍrāḍu (pl. ḍrāṭka) (W)`; Burrow Kui head is `grāḍu` (K.) with `(W.) ḍrāḍu (pl. ḍrāṭka)` inside the gloss. `ḍrāḍu` in full_text but never split out (`(W.)`/Warangal inline dialect not a registered Kui inline abbrev — only `K.`). |
| 3790 | transcription_divergence | Starling `nolpa (nolt-/noṭ-)` vs Burrow `nolpa (nolt-/ noṭ-)` — differ only by one space after the slash. |
| 931 | transcription_divergence | Starling `opka (okt-)` vs Burrow `opka (< ok-p-; okt-)`; base `opka` identical, Burrow adds etymology (stored form carries raw `&lt;` HTML entity). |
| 3499 | parser_miss | full_text `Konḍa ḍoṇḍa C. indica. Kui ḍōnḍi pumpkin.` — Kui block swallowed into Konḍa's gloss; `Kui ḍōnḍi` never becomes its own attestation; language marker after sci-name abbrev `C. indica.` not detected. |
| 695 | parser_miss | Panels 2/3 empty — entry parses to zero attestations. full_text has `Kui ḍupka (< ḍuk-p-; ḍukt-)…`; whole-entry parse failure (first head `Te. ūḍ(u)cu` has parens right after). |
| 5323 | genuine_divergence | Burrow 5323 has Tu./Te./Pa./Konḍa/Kuwi only — no Kui section. Starling `vāru` genuinely unattested in Burrow's Kui. |

**Proposed fix (two systemic levers; rest are divergences):**
1. **mapping_gap (837)** — `dialect_mapping.py`, `_ABBREV_ALIASES` (~L296): add `"Kui.": "Kui"`, mirroring existing `"Koḍ": "Koḍ."` trailing-period alias; or strip a trailing `.` in `match_languages` before `_BURROW_TO_INFO.get(burrow_lang)`. One-line, zero-regen, matcher-side.
2. **parser_miss (3499, 695)** — `burrow_entry_parser.py` language-boundary detection: (a) recognize a language marker (`Kui`) following a scientific-name genus abbrev + period (`… C. indica. Kui …`) so it starts a new attestation; (b) handle a paren-bearing first headword (`Te. ūḍ(u)cu`) so the entry doesn't collapse to zero attestations. (410/1123 are softer matcher-side gloss-scan enhancements — surfacing inline `adj. ī` / `(W.) ḍrāḍu` sub-forms — not parse bugs.)

## Konda (18 rows)

**Dominant verdict: `parser_miss`** — in 6 of 11 failing rows the Konḍa section is bare *plain text* in raw HTML (not wrapped in `<i>`), sitting right after a `(Voc. N).` citation, so `_find_all_lang_spans` never opens a Konḍa attestation and glues its forms onto the preceding Go./Pa. gloss. The rest: glottal-transcription mismatches (ʔ vs ˀ + stray space), one trailing-period mapping gap, one genuine absence.

| ded | class | evidence (quoted token) |
|---|---|---|
| 260 | parser_miss | HTML `...(Voc. 97). Konḍa <b>al- (aṭ-, aṇ-)</b>...` — "Konḍa" untagged, absorbed into Go. gloss |
| 260 (BB) | parser_miss | same run: `(BB) aṛ-` never reaches its own span |
| 382 | parser_miss | HTML `...Voc. 2604...). Konḍa <b>āli mrānu</b> pipal tree.` — bare Konḍa inside Go. gloss |
| 3535 | parser_miss | full_text `...LSI). Konḍa doRk- to be got...` glued into Go. |
| 4885 | parser_miss | `Go. ... mīn (Voc. 2852). Konḍa (BB) mīn (pl. mīnga)` absorbed into Pa. row |
| 644 | parser_miss | `Go. ... ul id. Konḍa mūl- (mūṭ-) to urinate` glued into Go. |
| 474 | transcription_divergence | Starling `riʔ-/ri-` vs Burrow `riˀ -/ri-` — glottal ʔ vs modifier ˀ + inserted space |
| 5052 | transcription_divergence | Starling `muʔer` vs Burrow `muˀ er` (ʔ/ˀ + space) |
| 3655 | transcription_divergence | Starling `nālʔer` vs Burrow `nālˀ er` (ʔ/ˀ + space) |
| 3684 | mapping_gap | HTML marker `<b><i>Konḏa.</i> nīn</b>` — trailing period yields language key `Konḏa`, not folded to `Konda` |
| 5339 | genuine_divergence | full_text has no Konḍa token (Ta./Ma./To./Ka./Tu. only); Starling head `toko` absent |

**Proposed fix:** systemic lever is `parser_miss`. In `burrow_entry_parser.py`, `_find_all_lang_spans`, add a pattern (alongside `_STRICT_PATTERNS`/`_RT_SCINAME_PATTERN`) recognizing a **bare, untagged known-language abbrev** (`Konḍa`) in running text after a `(Voc. N).`/sentence-final-period boundary immediately followed by a `<b>`-wrapped headword, gated by `_is_known_lang_abbrev` (same guard the To.-type/running-text patterns use). Splits the glued Go./Pa. gloss and emits the missing Konḍa attestation. Secondary: the 3 transcription rows need the remaining glottal `ˀ`+**inserted-space** strip in `textnorm.normalize_for_match` (the ʔ↔ˀ char fold already ships, but the space `muˀ er` still blocks); 3684 via a trailing-period `Konḏa.`→`Konda` alias in `dialect_mapping.py`.

## Malayalam (17 rows)

**Dominant pattern: `parser_miss`** — the fixable systemic failure. In DEDs 3824/3755/4250 the Malayalam segment is introduced by "Ma." *immediately after an italic Latin binomial terminated by a period* (`… P. quadrifida.`, `… Phyllanthus emblica.`, `… Trichosanthes anguina.`). The parser fails to open a new language span there, so `Ma.` (and following `Ka./Tu./Te.`) get swallowed into the preceding Tamil gloss and never reach the attestation list. Remaining "No" rows are genuine absences; two are edition/coverage artifacts. (Note: only 13 of the 17 rows are Malayalam across these 10 DEDs; the other 4 are Malto rows, a separate language.)

| ded | class | evidence (quoted token) |
|-----|-------|--------------------------|
| 23 | genuine_divergence | No `Ma.` in full_text; Starling `akka` = Burrow's Ta/Ka form; the `(Ma.)` present is Maria-Gondi inside `Go.`, not Malayalam. |
| 1 | other | Cached DED 1 = appendix `akkaṭa` (page 509), no `Ma.`; Starling DED 1 = `a, ā "that, yonder"` (real DEDR 1). Page-509 appendix-mislabel/numbering, not a data gap. |
| 360 | genuine_divergence | No `Ma.`; Starling Ma `āmaṇakku` is actually Burrow's Tamil form `Ta. āmaṇṭam, amaṇṭalam, āmaṇakku`. |
| 3824 | parser_miss | `…vayaḷai purslane, P. quadrifida. Ma. pacaḷa, paśaḷa Basella…` — Ma (+Ka, Tu, Te) dropped; only Ta parsed. |
| 3755 | parser_miss | `…emblic myrobalan, Phyllanthus emblica. Ma. nelli id.` — `Ma. nelli` swallowed into Ta gloss. |
| 4250 | parser_miss | `…snakegourd, Trichosanthes anguina. Ma. puṭṭal, piṭṭal id.` — `Ma.` swallowed into Ta gloss. |
| 410 | transcription_divergence | Ma parsed fine (`innu`, `ittiri` = Yes). `i, ī` reported "Language only" though Burrow Ma headword is literally `i, ī` (identical) — matcher headword-normalization artifact. |
| 2764 | genuine_divergence | No `Ma.` (Ta, Te only). Starling hedges: `caṭṭuvam "shoulderbone (or with DEDR 2309?)"`. |
| 5330 | other | `DED 5330 not found in cached corpus` — entry absent from Burrow corpus (coverage/edition). |
| 4509 | genuine_divergence | `puta` matches `Yes (exact)`. Extra Starling Ma `vitampuka "to long for"` genuinely absent from DED 4509; its "Language only" is spurious. |

**Proposed fix:** systemic for 3824/3755/4250 — `burrow_entry_parser.py`, `_find_all_lang_spans` (the `_RT_SCINAME_PATTERN` running-text sci-name branch guarded by `_rt_headword_ok`). Extend it to open a new language span when a known abbrev (`Ma.`, `Ka.`, `Tu.`, `Te.`) follows an `<i>`-italicized scientific binomial ending in a period, so the post-binomial `Ma.` form is captured instead of absorbed into the preceding Tamil gloss. This is the known-open "To.-type TEXT variant / running-text sub-case." The genuine_divergence (23, 360, 2764, 4509-vitampuka), edition (1), and coverage (5330) rows need no parser change.

## Tamil (16 rows)

**Dominant pattern: transcription_divergence** — the single largest homogeneous cause (34, 905, 1588, 4922) is an unstripped comma inside the tense-marker parenthetical: Starling writes `(-pp-, -tt-)`, Burrow writes `(-pp- -tt-)`. `normalize_for_match` strips `* _ - ( )` but not `,`, so the base headword matches yet the parenthetical tokens (`pp,` vs `pp`) diverge. A secondary **parser_miss** cluster (5006, 4002, partly 4572/399) drops the Ta. row entirely, plus two genuinely-absent entries.

| ded | class | evidence |
|-----|-------|----------|
| 34 | transcription_divergence | Starling `aŋkā (-pp-, -tt-)` vs Burrow `aṅkā (-pp- -tt-)`; eng `ŋ`/`ṅ` (already ENG_FOLD'd) + unstripped comma |
| 399 | parser_miss | Starling's 2nd Ta. headword `āṇ` lives inside Burrow's Ta. gloss ("...`āṇ` male, manliness..."), never surfaced as a matchable form → matched to `āḷ` by mistake |
| 905 | transcription_divergence | Starling `ēl (ēr_p-, ēr_r_-; ēlv-, ēn_r_-)` vs Burrow `ēl (ēṟp-, ēṟṟ-; ēlv- ēṉṟ-)`; `r_`/`n_` reconcile via `_`-strip+NFKD, residual mismatch is the comma in `ēlv-,` |
| 1588 | transcription_divergence | Starling `kiḷai (-pp-, -tt-)` vs Burrow `kiḷai (-pp- -tt-)`; comma only |
| 4922 | transcription_divergence | Starling `muṭi (-v-, -nt-)` vs Burrow `muṭi (-v- -nt-)`; comma only |
| 5006 | parser_miss | full_text has `Ta. (DCV) muṟaḷai` but stored/reparse omit Ta.; `(DCV)` leading qualifier after `Ta.` dropped the whole Tamil row (Starling `mur_aḷai` = muṟaḷai) |
| 4002 | parser_miss | full_text `4002 Ta , par̤i (-pp-, -tt-)` — language marker is `Ta ,` (comma, not period); parser fails to recognize it, drops Tamil (Starling `paẓi` = par̤i present) |
| 4572 | parser_miss | Burrow full_text has unbalanced parenthetical `pō (pōv-/pōkuv-/pōtuv-, pōṉ-/ reach a destination...`; missing close-paren makes headword field overrun the gloss, so `pō` never isolates cleanly (source OCR gap) |
| 5330 | genuine_divergence | `van_n_i` (vaṉṉi); "DED 5330 not found in Burrow corpus", no live page — entry absent (numbering gap) |
| 5382 | genuine_divergence | `viku (-pp-, -tt-)`; "DED 5382 not found in Burrow corpus", no live page — entry absent |

**Proposed fix:** `textnorm.py` → `normalize_for_match` → add `.replace(",", "")` to the `base` chain (alongside existing `-`/`_` deletions), so `(-pp-, -tt-)` == `(-pp- -tt-)`; reconciles 34, 905, 1588, 4922 as pure zero-regen matcher-side normalization. The parser_miss cluster (5006 `(DCV)` leading qualifier, 4002 `Ta ,` comma-marker variant) is a separate `burrow_entry_parser.py` lever — the language-marker regex/qualifier handling would need to accept a trailing comma and a leading `(…)` qualifier before the headword — NOT covered by the textnorm change.

## Muria Gondi (15 rows)

**Dominant verdict: `parser_miss` (systemic).** Burrow consolidates every Gondi sub-dialect under one `Go.` segment whose headword is only the *first* form; the Muria-specific forms live inside that segment's meaning text and are never emitted as attestations. The match-time inline extractor (`extract_gloss_forms_for_abbrevs`) only recovers them when the `(Mu.)` marker *precedes* the form — so the frequent "form (markers) meaning" order, the `MuE./MuW.` labels, multi-`Go.` homophone splits, and one dropped `Go.` segment all leak through. A minority are transcription/genuine divergences.

| ded | class | evidence (quoted token) |
|-----|-------|--------------------------|
| 379 | parser_miss | `Go. (Mu. Elwin) ārk Setaria italica` in full_text but **no Go. attestation at all** — swallowed into the Te. segment's meaning. Match=No. |
| 2675 | parser_miss | Muria form `huppe … (Mu.) field-rat`; form precedes marker chain, extractor took meaning `field-rat` as the form. |
| 1818 | parser_miss | `gumiya (D. Mu.) pit` — form precedes `(D. Mu.)`; extractor took `pit`, matcher fell back to `koṛpanj`. |
| 485 | transcription_divergence | `(Mu.) irum/iṛum` present and matched; Starling `irum, iṛum` differs only by separator (comma vs slash). |
| 1298 | transcription_divergence | `(Mu. M.) kal (obl. kad-, pl. kalk)` present; Starling `kal (obl.kad-,pl.kalk)` differs only by whitespace. |
| 1416 | parser_miss | `kētul (Mu. Ma.) hut in field for watching` — form precedes marker; extractor took meaning `hut`. |
| 4968 | parser_miss (mapping_gap flavour) | Muria forms labelled `(MuE.) maloṛ` and `(MuW.) malol, molol`; inline-abbrev table maps only `Mu.`→Muria Gondi, not `MuE./MuW.`, so never targeted (matched first Tr. form `malōl`). |
| 495 | genuine_divergence | DED 495 Gondi has only `(Ma.) lēki, (M.) leke, (LuS.) lèkee`; Starling's `vallek` occurs nowhere. |
| 4083 | parser_miss | `(Mu.) pīplī` present but in the **second** Go. segment (homophone split `(c) … Go. (SR.) piprī, (Mu.) pīplī`); matcher scans only best_att = first Go. `pāpe` segment. |
| 4275 | transcription_divergence | No `Mu.` form; nearest Gondi `(Tr.) punō / (L.) punā` differs from Starling `pūna` only by macron placement. |

**Proposed fix (systemic, matcher-side — the corpus intentionally keeps Gondi consolidated, so NOT a `burrow_entry_parser.py` re-segmentation):**
1. `gloss_extraction.py` → `extract_gloss_forms_for_abbrevs`: handle **marker-after-form** order — when the token following a matched marker group is a gloss stopword/meaning rather than a form, take the form token immediately **preceding** the marker group instead of falling back to `primary_headword`. Recovers `huppe` (2675), `gumiya` (1818), `kētul` (1416).
2. `dialect_mapping.py` → `_INLINE_ABBREV_TO_DIALECTS`: add `"MuE."`/`"MuW."` → `["Muria Gondi"]` (East/West Muria), fixing 4968.
3. Inline-extraction loop in `starling_tree_validator.py` `_match_entry` should iterate over **all** `Go.` attestations (`(a)/(b)/(c)` homophone splits), not just `best_att`, fixing 4083.
4. 379 is the one true segmentation miss — `burrow_entry_parser.py` `_find_all_lang_spans` failed to open a `Go.` span at `Go. (Mu. Elwin) ārk` (source-name parenthetical after the abbrev); separate boundary-detection fix.

DEDs 485, 1298, 4275 (transcription) and 495 (genuine) need no parser change.

## Tulu (14 rows)

**Dominant verdict: `parser_miss`** — 8 of 10 DEDs (12 of 14 rows). Burrow *does* contain the Tulu form in `full_text`, but the `Tu.` marker is not in the canonical `<b><i>Tu.</i> hw</b>` markup the parser keys on, so no new attestation is opened and the Tu. form is glued into the preceding language's gloss body. The 2 exceptions (4187, 4205) parse Tu. correctly and fail only because Starling's headword is a *secondary form inside* Burrow's Tu. gloss, not the parsed headword.

| ded | class | evidence (token / markup) |
|---|---|---|
| 126 | parser_miss | `<b>Tu. aṇḍi</b>` — plain-bold marker (no `<i>`); glued into Koḍ. gloss "…mango stone. Tu. aṇḍi…". |
| 400 | parser_miss | `Tu. <b>āḍe</b>` — abbrev fully untagged, only headword bold; glued into Ka. gloss. |
| 3824 | parser_miss | `<i>P. quadrifolia. <b>Tu.</b></i> <b>basalè</b>` — abbrev bold but inside sci-name italic span; whole entry collapsed into Ta. |
| 809 | parser_miss | `<b>Tu. ettāvuni, ettāḍuni</b>` — plain-bold marker; glued into Koḍ. gloss. |
| 1563 | parser_miss | leading `<i><b>Tu.</b> (Eng.-Tulu Dict.)</i>` — reversed `<i><b>` nesting + bibliographic qualifier after abbrev; entire leading Tu. `girige` dropped. |
| 4143 | parser_miss | `<i><b>Tu.</b></i> ? <b>pēñci</b>` — reversed nesting + stray `?` before headword; glued into Ka./Te. |
| 664 | parser_miss (1 of 2 rows) | (a) `Tu. <b>uruṇṭů, uruṇḍulu…</b>` untagged marker glued into Koḍ. → `uruṇṭụ…` unmatched; (b) `<b><i>Tu.</i>… uṇḍè</b>` did parse and `uṇḍè` matched. |
| 758 | parser_miss | `Tu. <b>ūḷiga</b>` — untagged marker glued into Ka. gloss "…male servant. Tu. ūḷiga…". |
| 4187 | other (in-gloss secondary form) | Tu. parsed fine (`<b><i>Tu.</i> pīṅkuḍuni…</b>`); Starling `puḷuku, (B-K) puḷku` is a secondary form *inside* Burrow's Tu. gloss body; language matched (conf 1.00). Not a parser bug. |
| 4205 | other (in-gloss secondary form) | Tu. parsed fine, `pira` matched; second Starling row `pini, pinni` is a secondary form inside Burrow's Tu. gloss. Not a parser bug. |

**Proposed fix:** `burrow_entry_parser.py` — extend the compiled language-marker pattern table (`_LANG_*` / the `<b>\(?<i>…_LANG_ABBREV…` patterns ~L254-283 feeding the split loop) to also recognize three non-canonical markup shapes for a known-inventory abbreviation immediately followed by a bold headword span: (1) plain-bold `<b>Tu. hw</b>` (126, 809), (2) untagged abbrev with bold headword `Tu. <b>hw</b>` (400, 664a, 758), (3) reversed/embedded nesting `<i><b>Tu.</b></i>` incl. a leading marker with trailing `(...)` qualifier (1563, 3824, 4143). Guard strictly — bare `Tu.`/`<b>Tu.` also occur mid-gloss as cross-refs — by gating on abbrev ∈ language inventory AND a bold headword span following. This is the documented open "To.-type TEXT/plain-marker variant" family. Rows 4187 & 4205 need no parser change (matcher-side secondary-form-in-gloss limitation).

## Kannada (13 rows)

**Dominant-pattern verdict:** Not a parser or mapping problem — in 8 of 10 non-trivial rows the Kannada form IS parsed and stored correctly; failures are dominated by **transcription_divergence** (schwa/open-o codepoints, slash-spacing, packed multi-form headwords). Only ONE real parser_miss (3824, the recurring sci-binomial tail) and three genuine language-absences. (CSV holds 12 Kannada rows for these DEDs, not 13.)

| ded | class | evidence |
|-----|-------|----------|
| 190 | transcription_divergence | Ka. `əyb` parsed fine; Starling `ǝyb` uses U+01DD turned-e vs Burrow U+0259 schwa — same phone, different codepoint |
| 399 (āḷ) | already matches | `Yes (exact)`; Ka. `āḷ` == Starling `āḷ` |
| 399 (āṇ) | other | `āṇ` present in Burrow Ka. gloss "āḷ, **āṇ** male" as secondary form; matcher compares only headword `āḷ` |
| 334 | transcription_divergence | Starling packs `ā (pl.ākal), āvu`; Burrow splits headword `ā` + gloss "(pl. ākal), āvu cow" — leading form `ā` matches, only segmentation differs |
| 3824 | **parser_miss** | full_text has "**Ka. basaḷe** Malabar nightshade, B. alba"; reparse+stored collapse the tail into Tamil only (heuristic flags missing Ka./Ma./Te./Tu.) |
| 4441 | genuine_divergence | full_text has no `Ka.` token (Ta./Go./Te./Kol./Nk./Konḍa); Starling assigns `pēl` to Kannada, Burrow does not |
| 4509 (pode) | already matches | `Yes (exact)`; Ka. `pode (podad-, poded-, podd-)` identical |
| 3498 | transcription_divergence | Burrow "(Gowda) `doṇḍE`, (Bark.) doṇḍe"; Starling `dɔṇḍE` uses U+0254 open-o vs Burrow U+006F o |
| 3480 | transcription_divergence | Burrow `tuḍu/ toḍu (toṭṭ-)` vs Starling `tuḍu/toḍu (toṭṭ-)` — differ only by a space after the slash |
| 5330 | other | entire DED 5330 absent from Burrow corpus (cache miss + live fetch failed) |
| 5509 | genuine_divergence | full_text has no `Ka.` token; `bēla, balavala, balōla` that Starling calls Kannada are listed by Burrow under **Ta.** |
| 4509 (bede) | genuine_divergence | Burrow's Ka. form is `pode` (p-/o-); Starling `bede, beda` (b-/e-) not present in Ka. section |

**Proposed fix:** No single systemic fix — mostly divergences. Two cheap safe sub-fixes if pursued: (1) add codepoint folds to `normalize_for_match` in `textnorm.py` (same translate-table mechanism as `BARRED_I_FOLD`): U+01DD turned-e → U+0259 schwa (fixes 190) and U+0254 open-o → o (fixes 3498); optionally strip spaces around `/` (fixes 3480, and helps 3480-type across languages). (2) The lone true parser bug DED 3824 recurs in Malayalam/Tulu too — the Ka./Ma./Te./Tu. tail after a sci-binomial is dropped, only Tamil emitted; single `burrow_entry_parser.py` sci-binomial-boundary fix would clear 3824 across all three groups. 4441/5509/4509-bede/5330 are genuine data gaps.

## Kuwi (Israel) (12 rows)

**Dominant verdict: parser_miss (7/10)** — Burrow consolidates every Kuwi sub-dialect (F./S./Su./Isr./Ḍ./Ṭ.Isr.) into one "Kuwi (Schulze)" attestation, first sub-form as headword and the rest in the gloss. The matcher *does* have machinery to pull the `(Isr.)`-tagged form out (`extract_gloss_forms_for_abbrevs`), but four distinct defects in that extractor make it return nothing / the wrong token / a truncated form — so every row lands "Language only". Remaining 3: 2 spacing-only transcription divergences + 1 genuine absence. All matcher-side / zero-regen.

| ded | class | evidence (quoted token) |
|-----|-------|-------------------------|
| 228 | parser_miss | full_text `(Ṭ. Isr.) rāk- to rub`; stored hw `rāca tuh'nai/mlekh'nai`. Marker group `Ṭ. Isr.` rejected by ASCII-only `_DIALECT_MARKER_GROUP_RE` (`Ṭ.` non-ASCII) → extraction `[]` |
| 333 | parser_miss | full_text `(Su. P. Isr.) ā- (āt-)`; extractor yields form `ā-` (drops `(āt-)` into meaning) → norm `a` ≠ Starling `ā (āt-)` norm `a at` |
| 1043 | genuine_divergence | Burrow DED 1043 has **no Kuwi** (only Ta/Ma/Ko/To/Ka/Koḍ/Tu); Starling `bayalu`→1043 absent |
| 2331 | transcription_divergence | sole Kuwi subform `(Isr.)`; Burrow hw `sap ta` vs Starling `sapta` — internal space only |
| 1438 | transcription_divergence | Burrow hw `kāsa nehˀ uṛi` vs Starling `kāsa nehʔuṛi`; glottal `ˀ→ʔ` already folds, but space in `nehʔ uṛi` remains |
| 4275 | parser_miss | full_text `(Isr. p. 127) puˀ ni`; marker group `Isr. p. 127` rejected by regex (digit token `127`) → extraction `[]` (also `puˀ ni`/`puʔni` space) |
| 3122 | parser_miss | Israel form `ṛev- (-it-)` sits **before** its `(Isr.)` tag; extractor only reads post-marker tokens → returns primary hw `taṛj- (-it-)` |
| 3539 | parser_miss | postposed: `doy- (-it-) (Ḍ.) … (Isr.) to kick`; post-marker token is stopword `to` → falls back to primary hw `toiyali` |
| 5334 | parser_miss | postposed: `vāŋg- (-it-) (Su.) …, (Isr.) leak`; extractor grabs gloss word `leak` as the form (CSV `Matched Burrow form = leak`) |
| 5531 | parser_miss | full_text `(Isr.) vēpa marnu id.`; dropped from stored gloss; even if found, multi-word form truncates to `vēpa` (only `parts[0]` kept) |

**Proposed fix:** systemic — `gloss_extraction.py`, `extract_gloss_forms_for_abbrevs` + helper `_DIALECT_MARKER_GROUP_RE`. Four changes: (1) broaden `_DIALECT_MARKER_GROUP_RE` (`^(?:[A-Za-z]+\.)+…`) to accept diacritic letters and `p.###` page tokens so combined markers `(Ṭ. Isr.)` / `(Isr. p. 127)` are recognized; (2) keep a leading paradigm paren `(…-)` attached to the form token (`ā- (āt-)`) instead of shunting into meaning; (3) handle **postposed markers** — capture the form token immediately preceding a marker group, not only the token after (fixes 3122/3539/5334) [same lever as Muria Gondi fix #1]; (4) capture multi-word form phrases, not just `parts[0]` (fixes `vēpa marnu`). Do **not** touch `burrow_entry_parser.py` — the consolidated attestation is correct. 2331/1438 (and residual 4275 space) are a separate smaller lever: intra-token space inside a single headword, not blanket-strippable in `normalize_for_match` (would fuse real multi-word forms), needs a targeted rule.

## Kuwi (Fitzgerald) (11 rows)

**Dominant pattern: parser_miss (systemic).** Burrow lumps every Kuwi sub-source — `(F.)` Fitzgerald, `(S.)` Schulze, `(Su.)` Sundara, `(Isr.)` Israel, `(Ḍ.)`, `(P.)` — under a single `Kuwi` language span, stores it as ONE attestation canonicalized to `Kuwi (Schulze)`, keeping only the first `<b>` form as headword; the `(F.)` form survives only inside the meaning text (or gets swallowed into a neighbor language). StarlingDB splits Kuwi (Fitzgerald) out, so the fuzzy match connects the language (0.95, "dialect of Schulze") but the `(F.)` headword is never surfaced. Of 8 rows: 4 parser_miss, 2 genuine_divergence, 2 transcription_divergence.

| ded | class | evidence (token quoted) |
|-----|-------|--------------------------|
| 1120 | other | No Kuwi in full_text (Ta./Ma./Ko./Ka./Koḍ./Tu. only) and no validation row — not a Kuwi entry |
| 4527 | parser_miss | `Kuwi (Su.) pom- (-it-) ... (F., p. 139) pompki-ahanaha with arms interlaced` — `(F.)` form = Starling head exactly, lumped under one attestation; matched `pom- (-it-)` |
| 3140 | parser_miss | `Kuwi (Su.) dā'- (dāt-) ... (F) dācali to cut with knife` — `(F) dācali` = Starling head, buried in meaning; matched `dā'- (dāt-)` |
| 706 | parser_miss | `Kui ubga ... Kuwi (Su.) ur- (-h-) ...; (F.) ūrhali to butt` — entire Kuwi block swallowed into the Kui attestation; `(F.) ūrhali` never surfaced |
| 705 | parser_miss | `Manḍ. uli id.; Kuwi (F.) ūlli, (S.) ulli gidda ...` — Kuwi block swallowed into the Manḍ. attestation; `Kuwi (F.) ūlli` never surfaced |
| 5124 | genuine_divergence | Kuwi block `mṇok- (-h-) (Su.) ..., (Isr.) ...; (S.) mrokh'nai` — no `(F.)`; Starling `mrūkhali` absent |
| 4423 | genuine_divergence | `Kuwi (Su. Isr.) per- (-h-)` — only `(Su. Isr.)`, no `(F.)`; Starling `pēṛhali` not in Burrow |
| 410 | transcription_divergence | `Kuwi (F.) īwasi, īwari, īdi, īwati this man...` captured as headword; Starling stores bare base `ī`, Burrow the paradigm — same lexeme, granularity differs |
| 1977 | transcription_divergence | `Kuwi (F.) kiriyū (pl. kīrka) ... ear` present as stored headword; Starling `kiryū (pl. kīrka)` — differ only `kiriyū`~`kiryū`; matcher picked wrong sub-token `kirpejja` |

**Proposed fix (systemic — addresses 4 parser_miss rows; STRUCTURAL/regen, not matcher-side):** `burrow_entry_parser.py`. Two coordinated changes: (1) in `_find_all_lang_spans` (~L650), recognize a plain-text `Kuwi` as a language-span boundary even when not wrapped in its own `<i>Kuwi</i>` and following Kui/Manḍ mid-run (fixes 705/706 swallow). (2) In `parse_language_sections`/`_split_headword_chain` (~L734/48), split intra-span source qualifiers `(F.) (S.) (Su.) (Isr.) (Ḍ.)` inside a Kuwi span into distinct attestations, and add the source→dialect map in `dialect_mapping.py` (`(F.)`→`Kuwi (Fitzgerald)`, `(S.)`→`Kuwi (Schulze)`, `(Su.)`→`Kuwi (Sundara)`, `(Isr.)`→`Kuwi (Israel)`), replacing blanket canonicalization to `Kuwi (Schulze)`. NOTE: this partly conflicts with the Kuwi (Israel) verdict, which recommended matcher-side extraction *without* re-segmenting — a decision point: split Kuwi sub-sources structurally (helps both Kuwi groups, needs regen) vs. matcher-side gloss extraction (zero-regen). 5124/4423 (genuine) and 410/1977 (transcription) not fixable either way.

## Naiki (11 rows)

**Dominant verdict: parser_miss (6/10 resolvable rows).** StarlingDB "Naiki" = Burrow "Nk. (Ch.)" (the Nk.→Naikri / Nk.(Ch.)→Naiki split in `dialect_mapping.py` is *correct*). The failures are the parser dropping the "(Ch.)" dialect qualifier whenever it is not enclosed in the `<i>` language marker, so the Naiki form is either mislabeled Naikri or swallowed into the preceding gloss.

| ded | class | evidence |
|-----|-------|----------|
| 75 | parser_miss | `<i><b>Nk.</b></i> (Ch.) <b>aṛka</b>` — "(Ch.)" outside marker → stored as `Nk. (Naikri): aṛka`; form present, language mislabeled. |
| 1623 | parser_miss | `<i>Nk.</i> (Ch.) <b>khīr</b>` → `Nk. (Naikri): khīr` instead of Naiki. |
| 430 | parser_miss | `<i><b>Nk.</b></i> (Ch.) <b>ḍik-, ḍig-</b>` → `Nk. (Naikri): ḍik-, ḍig-`; Starling head present but wrong lang. |
| 474 | parser_miss | `…two seers. Nk. (Ch.) <b>ernḍi</b>` — entire `Nk. (Ch.)` is **unmarked plain text**, no pattern fires; `ernḍi…` swallowed into preceding `Nk. (Naikri)` gloss. |
| 2781 | parser_miss | `…Ta. <b>cēr</b>). Nk. (Ch.) <b>ser-/sen-/se- (sedd-)</b>` — unmarked `Nk. (Ch.)` swallowed into preceding `Nk. (Naikri)` gloss. |
| 2116 | parser_miss | `<i><b>Nk. (Ch.)</b></i> kombaṛ far; (LSI 4.572; Chanda) khōmbāḍ…` — marker well-formed but whole **leading** attestation dropped (only `Pa. komaḍ` stored); leading-position + `(LSI…)` parenthetical. |
| 1208 | transcription_divergence | Naiki matched (conf 1.00); Starling `katuk-/katk-` vs Burrow `katuk-/ katk-` — space after slash. |
| 1291 | transcription_divergence | Naiki matched (conf 1.00); Starling `karug-/karuk-` vs Burrow `karug-/ karuk-` — space after slash. |
| 1769 | genuine_divergence | `<b><i>Nk.</i> kōti</b>` — Burrow attests only plain `Nk.`(=Naikri) `kōti`; no `Nk. (Ch.)` form. |
| 2773 | genuine_divergence | "DED 2773 not found in Burrow corpus" — entry absent entirely (Starling head `cep, ceppu`). |

**Proposed fix:** systemic, in `burrow_entry_parser.py`. The between-marker qualifier that `_OPT_HEADWORD_QUALIFIER` (L218) currently swallows/discards should be captured and re-attached to the base abbrev when the pair forms a known composite (`_is_known_qualified_abbrev("Nk. (Ch.)")` already returns true, used by `_clean_lang_abbrev` L528). Concretely: extend Pattern C (L286) and Pattern B/D (L340) abbrev handling so a trailing `\s*\([^)<]*\)` sitting *after* `</i>`/`</b></i>` (the "(Ch.)"-outside-marker shape in 75/1623/430) is folded into the `_LANG_ABBREV` group before `_clean_lang_abbrev`, exactly as `_OPT_LANG_QUALIFIER` (L190) already does for the in-marker case. Recovers 75/1623/430. The unmarked-token cases (474/2781 — `Nk. (Ch.)` with no `<i>` anchor) and dropped-leading-attestation (2116) are a separate, harder plain-text language-boundary-scan gap. No change to `dialect_mapping.py` — Nk./Nk.(Ch.) mapping verified correct. (The slash-space rows 1208/1291 recur across Kannada/Kui/Telugu — a shared `/`-spacing lever.)

## Malto (10 rows)

**Dominant-pattern verdict:** The 10 rows split three ways; the only *systemic, fixable* cluster is **parser_miss** (3 rows) — Burrow's `full_text` contains the Malto form but the parser folds it into the preceding Kurukh segment. The other clusters (transcription_divergence 3, genuine_divergence 3, +1 corpus-gap) are data-level.

| ded | class | evidence |
|-----|-------|----------|
| 1 | transcription_divergence | Language matched (`Malt.`); Starling `ā` "that" vs Burrow demonstrative forms `Malt. áh, ár, áth`. Segment parsed fine. |
| 37 | genuine_divergence | full_text ends `Kur. asrnā to tremble.` — no `Malt.` token; Starling maps Malto `asr-` onto a set Burrow lists without a Malto reflex. |
| 409 | genuine_divergence | full_text = Ka/Te/Go/Pe/Kui only, no `Malt.`; Starling Malto `ān_ṛeṭe` absent. |
| 392 | **parser_miss** | `to yawn. Malt. <b>áwole</b> id. Br. <b>āvāning</b> id.` — `Malt.` is **plain text (not `<i>`-wrapped)**, only the form is `<b>`; folded into Kur. gloss. |
| 4473 | **parser_miss** | `<b><i>Malt;</i> boṉg̣e</b>` — abbrev is `<i>`-wrapped but reads **`Malt;`** (semicolon typo); `_LANG_CHAR` excludes `;`, so the `</i>` anchor breaks and it folds into Kur. |
| 410 | transcription_divergence | Starling `ī` "this" vs Burrow `Malt. íh, ír, íth` ("Language only"). Sibling `ine` "today" row matches Yes. |
| 5151 | transcription_divergence | Language matched; Starling `nēreh` vs Burrow `Malt. nére(h), …` — differ only by macron/acute + parenthesis. |
| 2188 | other | `DED 2188 not found in Burrow corpus` — entry absent (numbering/scrape gap). |
| 4879 | genuine_divergence | `miśī (pl. miśā)` **is** in full_text but tagged `Cf. Mar. miśī` — **Marathi** comparison, no `Malt.` reflex. Starling mislabels it Malto. |
| 1040 | **parser_miss** | `a small bird. Malt. <b>óṛe</b> quail.` — `Malt.` plain text (not `<i>`-wrapped); folded into Kur. |

**Proposed fix:** systemic for the parser_miss cluster — `burrow_entry_parser.py`. (1) For 392+1040 (plain-text `Malt.`/`Br.` before a bold form), add a pattern anchoring on a *non-`<i>`* language abbrev immediately preceding `<b>headword</b>` — e.g. `(?<![<>\w])([A-ZĀ-ž][a-zÀ-ÿ.()]*\.)\s*<b>(headword)</b>` — registered like `_STRICT_PATTERNS` and gated through `_is_known_lang_abbrev` in `_find_all_lang_spans()` to keep English gloss words out. This is the SAME plain-text-marker lever as Tulu/Konda/Malayalam. (2) For 4473, tolerate the `Malt;` OCR typo: add `;` to `_LANG_CHAR` or fold trailing `;`→`.` in `_clean_lang_abbrev()`. Rows 1/410/5151 (transcription) and 37/409/4879/2188 (genuine/corpus-gap) have no parser fix.

## Parji (10 rows)

**Dominant-pattern verdict:** `parser_miss` — in 5 of 10 rows the Parji ("Pa.") form is present in Burrow's `full_text` but the parser never splits it into its own attestation because the "Pa." marker is **bare plain text (untagged)** — or (DED 3755) trapped inside a scientific-name `<i>` run — so it gets swallowed into the preceding language's gloss. The rest are genuine language-attribution divergences (3) or entries missing from the corpus (2).

| ded | class | evidence |
|---|---|---|
| 96 | genuine_divergence | No `Pa.` token. Starling `anḍ- (-t-)` = Burrow's **Pe.** (Pengo) `anḍ- (-t-)`; Burrow has no Parji. |
| 2773 | other | Entry absent: "DED 2773 not found in Burrow corpus". |
| 835 | parser_miss | full_text `Pa. iluŋg voice`; raw_html `resound. Pa. <b>iluŋg</b> voice.` — **"Pa." bare, no `<i>`**; glued into the Te. attestation. |
| 796 | parser_miss | full_text `Pa. etip- (etit-) id.`; raw_html `Nk. <b>ett-</b> to lift. Pa. <b>etip- (etit-)</b> id.` — **both Nk. and Pa. bare**; swallowed into the Kol. gloss. |
| 1679 | parser_miss | full_text `Pa. (S.) guḍḍi black`; raw_html `id. Pa. (S.) <b>guḍḍi</b> black.` — **"Pa." bare**; glued into Nk. |
| 1807 | other | Entry absent: "DED 1807 not found in Burrow corpus". |
| 4717 | parser_miss (secondary-in-gloss) | Pa. split *did* work (`Pa. Parji makka`), but Starling headword `mārloŋg / marnoŋg` is a **secondary in-gloss form** inside Burrow's Pa. gloss `? mārloŋg, (S.) marnoŋg rib` — present, not surfaced as a matchable headword. Distinct mechanism (multi-form within one correctly-split gloss). |
| 5114 | genuine_divergence | No `Pa.` token (Ga. moṭo, Konḍa moṭo, Kur. muṭā). Starling Parji `moṭṭi` absent; nearest Ta. `moṭṭai`. |
| 4914 | genuine_divergence | No `Pa.` token. Starling Parji `muñcuḍ dinom/dīna` = Burrow's **Pe.** (Pengo); Burrow has no Parji. |
| 3755 | parser_miss | full_text `Pa. nella Phyllanthus emblica`; raw_html `<i>Premna esculenta. <b>Pa.</b></i> <b>nella</b>` — **`<b>Pa.</b>` trapped inside a scientific-name `<i>` run**; glued into Te. |

**Proposed fix:** `burrow_entry_parser.py` — add a "Pattern G" (bare-plaintext language marker) to `_PATTERNS`/`_find_all_lang_spans` anchoring on a sentence-boundary (`. `) + **untagged** language abbrev + optional `(qualifier)` + `<b>headword</b>` — e.g. `(?<=\. )(<abbrev>)\s+(?:\([^)]*\)\s*)?<b>…</b>` — gating the abbrev through `_is_valid_lang`/whitelist so only real markers split. Closes 835/796/1679 (SAME plain-text-marker lever as Tulu/Konda/Malayalam/Malto). For 3755, extend the sci-name-italic handling so a `<b>Uppercase.</b>` inside a sci-name `<i>` run is treated as a language marker (mirrors the To.-type bold-embedded-marker-behind-sci-names fix, commit 44287fc — SAME lever as Malayalam/Kannada/Tulu 3824). DED 4717 is a *secondary-in-gloss* miss (matcher-side gloss_secondary/headword-alternate scan). No fix for 96/4914/5114 (genuine) or 2773/1807 (not in corpus).

## Kodagu (10 rows)

**Dominant pattern: `parser_miss` (4/10)** — Koḍ./Koḏ. language markers sit in HTML shapes the `<i>`-anchored parser doesn't split, so the Kodagu form is present in `full_text` but absent from stored + reparse attestations. Secondary: `transcription_divergence` (3, comma-vs-space stem lists + ɨ/ï), `genuine_divergence` (2), `mapping_gap` (1).

| ded | class | evidence (token quoted) |
|-----|-------|--------------------------|
| 2690 | parser_miss | HTML `<b><i>L. vulgaris. Koḏ.</i> tore</b>` — abbrev buried at end of an `<i>` behind a sci-name, and macron-below **Koḏ** (U+1E0F) not **Koḍ**; "tore" never carved out |
| 864 | parser_miss | HTML `(Hav.) <b>erugu</b>. Koḍ. <b>urupï.</b>` — "Koḍ." is **plain text** (no `<i>`), glued to Ka. gloss tail |
| 2654 | transcription_divergence | Lang matched; Star `cuḍ- (cuḍuv-, cuṭṭ-)` vs Burrow `cuḍ- (cuḍuv- cuṭṭ-)` — comma vs space inside parens |
| 990 | parser_miss | HTML `<b>Koḍ. orï</b> one <b>(<i>adj.</i>` — section (a) marker **bold-only, no `<i>`**; "orï" not stored, matcher fell back to `Koḍ. okka, okkace` |
| 4027 | transcription_divergence | Star `pari- (parip-, paric-)` vs Burrow `pari- (parip- paric-)` — comma vs space only |
| 3918 | parser_miss | HTML `<i><b>Koḍ</b>.</i> <b>pattï</b>` — italic marker but nested `<b>Koḍ</b>` with the "." **outside the bold**; "pattï" glued into Ka. gloss |
| 4547 | mapping_gap | HTML `<b><i>Kod.</i> pole</b>` — split fine but as language **"Kod"** (plain-d, no dot-below); `pole` matches exactly yet "Kod" ≠ Kodagu |
| 3090 | genuine_divergence | No Koḍ./Kod. in full_text; Burrow has only Ta/Ma/Ko/Ka/Te (Starling headword itself garbled) |
| 587 | transcription_divergence | Star `uḍɨ- (uḍɨp-, uḍɨt-)` vs Burrow `uḍï- (uḍïp- uḍït-)` — ɨ vs ï plus comma vs space |
| 4509 | genuine_divergence | Star Kodagu `bede` absent from Burrow (Burrow Koḍ. = `poda-`, matched Yes separately) |

**Proposed fix:** systemic (`parser_miss`) → `burrow_entry_parser.py`, the `_PATTERNS` language-marker list (+ `_is_known_lang_abbrev`/`_is_valid_lang` gate). Add handlers for the four Koḍ. shapes: (a) **plain-text** running-text marker `</b>. Koḍ. <b>` (864), (b) **bold-only** marker `<b>Koḍ. orï</b>` (990), (c) **nested-`<b>` with period outside bold** `<i><b>Koḍ</b>.</i>` (3918), (d) **abbrev after a sci-name inside one `<i>`** with macron-below **Koḏ.** (2690) — the SAME running-text/To.-type-TEXT + plain-text-marker family (Konda/Malayalam/Tulu/Malto/Parji). Cheaper adjacent wins: add `"Kod": "Koḍ."` to `dialect_mapping.py` `_ABBREV_ALIASES` (closes 4547 mapping_gap — mirrors the Kui./Konḏa. trailing-variant aliases); fold internal comma-vs-space in parenthetical stem lists (+ ɨ↔ï) in `textnorm.normalize_for_match` to flip 2654/4027/587 (comma lever shared with Tamil/Muria; ɨ↔ï is Kodagu-specific). 3090/4509 genuine.

## Kota (10 rows)

**Dominant verdict:** Not systemic — the Kota mapping itself works everywhere (6 rows are explicitly "Ko. = Kota, conf 1.00"). Failures are heterogeneous **headword-level divergences** (transcription + Burrow-internal secondary forms), plus a few isolated parser/mapping bugs — no single fix covers the group.

| ded | class | evidence (token) |
|---|---|---|
| 1109 | other (secondary form) | Starling `gaḍv, gayṛ` present **verbatim** in Burrow's Ko. gloss ("…`gaḍv, gayṛ` fixed or appointed time"), but matcher surfaced only primary headword `kaṛv- (kaṛd-)`. |
| 1850 | other (secondary form) | Starling `gud- (gudy-)` present verbatim in Burrow Ko. sub-entry (b) gloss; matcher returned `kur- (kuṯ-)` from sub-entry (a). |
| 1369 | transcription_divergence | Burrow `kaṛt- (kayt-/ kaṛty-)` vs Starling `kaṛt- (kayt-/kaṛty-)` — stray space after slash. |
| 3655 | parser_miss (headword glue) | `·` already folds (`na·ng`→`nang`=`nāng`), only blocker is parser gluing the pronunciation note: stored headword is `na·ng (n` (from "(n , not ŋ )"). |
| 3700 | mapping_gap | Burrow wrote `Ko..` (double period); parsed as pseudo-language "Ko", never mapped to Kota. Form `nunk- (nunky-)` present under the unmapped label. |
| 4149 | transcription_divergence | Burrow `piṛy ma·y` vs Starling `piṛy māv` — 2nd word `ma·y`(y) vs `māv`(v). |
| 4322 | other (source mislabel) | Burrow labels the Kota form as Kannada: "…`Ka. puḷy` sour… To. püḷy… `Ka. puḷi`…" — doubled/out-of-order `Ka.` is an OCR misread of `Ko.`; no Kota-labeled attestation exists. |
| 2391 | transcription_divergence | Burrow `kac av- (avt-)` vs Starling `kac av- (aft-)` — suffix `avt-`(v) vs `aft-`(f). |
| 760 | parser_miss | full_text has "`Ko. o·ḷ a·ṛ- (a·c-)` (jackal) howls…" (and `Ka. ūḷ…`) but both **dropped** from stored+reparse (attestations jump Ma.→Tu.). Vowel-initial spaced headwords. |
| 5382 | genuine_divergence | DED 5382 absent from Burrow corpus; Starling `vigv- (vigt-)` has nothing to match. |

**Proposed fix:** No systemic mapping/parser fix — mostly divergences and Burrow-internal secondary forms (the secondary-in-gloss cluster 1109/1850 recurs across Parji/Tulu/Muria — a matcher-side gloss_secondary/headword-alternate scan is the shared lever). Three isolated actionable bugs: (1) **760** — `burrow_entry_parser.py`, `_find_all_lang_spans`/`parse_language_sections` drops vowel-initial, space-led `Ko.`/`Ka.` spans (`o·ḷ a·ṛ-`, `ūḷ`); real parser_miss. (2) **3700** — `dialect_mapping.py` add a `Ko..`→`Ko.` alias (or have `_clean_lang_abbrev` collapse a doubled trailing period). (3) **3655** — `burrow_entry_parser.py`, `_split_headword_chain`: stop gluing the `(n , not ŋ )` pronunciation note onto the headword. (1369 slash-space = shared `/`-spacing lever.)

## Irula (10 rows)

**Dominant-pattern verdict:** `genuine_divergence` (10/10) — Burrow's DEDR carries **no Irula (`Ir.`) attestation at all** in any of these entries. Confirmed three ways: published `full_text` contains zero `Ir.`/`Iruḷa`/`Irula` tokens (regex sweep returned `[]` for all 10), stored + reparse panels are identical (parser lost nothing), and each validation note reads "Irula not in DED N; Burrow has: …" with Irula absent. StarlingDB attests Irula reflexes that Burrow declined to include.

| ded | class | evidence |
|-----|-------|----------|
| 1572 | genuine_divergence | Starling `gili`; full_text langs = Ta/Ma/Ko/Ka/Tu/Te only, no `Ir.` |
| 410 | genuine_divergence | Starling `ivä`/`inr_u`; 22 langs cited (Ta…Br), no `Ir.` |
| 524 | genuine_divergence | Starling `irugu`; full_text ends "…Kur. eṭṭnā", no `Ir.` |
| 530 | genuine_divergence | Starling `ini`; langs Ta/Ma/Ka/Tu/Te/Kur/Malt/Br, no `Ir.` |
| 445 | genuine_divergence | Starling `iḍukku`; langs Ta/Ma/Ka/Tu/Te/Ga only, no `Ir.` |
| 555 | genuine_divergence | Starling `īnu`; langs Ta/Ma/Ko/To/Ka/Te/Pa/Konḍa/Br, no `Ir.` |
| 2559 | genuine_divergence | Starling `ille`; full_text has `il`/`illa`/`illai` (Ta/Ma/Ko) but no `Ir.` section |
| 2625 | genuine_divergence | Starling `īrugoli`; full_text `īr` is Ta./Ka./Te., no `Ir.` marker |
| 5259 | genuine_divergence | Starling `vő:r_u`; langs Ta/Ma/Ko/To/Ka/Tu/Konḍa/Pe/Manḍ/Kui/Kuwi, no `Ir.` |

None are `mapping_gap` — a mapping fix only helps when an `Ir.`/`Iruḷa` token is physically present but unresolved; here the token never occurs. The known plain-l vs retroflex Irula orthography split is irrelevant since Burrow prints no Irula base name in any of the 10.

**Proposed fix:** no systemic fix — divergences. Correct "language-only/no-match" outcomes reflecting genuine coverage differences between StarlingDB and Burrow's DEDR; no code change recovers a form Burrow never published.
