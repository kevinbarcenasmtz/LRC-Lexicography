Entry from validation_audit_report.xslx which is a false positive:
Starling record #	Validation DED #	Validation branch label	Validation branch form	Starling language	Starling lexical headword	Starling lexical meaning	Match	Matched Burrow segment scope	Matched Burrow form	Matched Burrow meaning	Validation note
1	47	Proto-South Dravidian	*ac-	Malayalam	accu	mould, type	Yes (exact, 1.00)	Ma.	accu	id.	

The json for it from the C:\Users\Kevin\Github\LRC-Lexicography\data\dravidian\burrow_ded\burrow_corpus.cleaned.json

```json
{
      "page": 6,
      "ded_number": "47",
      "ded_number_raw": "47",
      "edition": "DEDR",
      "raw_html": "<div class='hw_result'><div>  \n\t\t\t\n\t\t\t<number>47</number> <b><i>Ta.</i> accu</b> mould, type. <b><i>Ma.</i> accu</b> id. <b><i>Ko.</i> ac</b> mould for casting iron. <b><i>Ka.</i> accu</b> mould, impression, sign, type, stamp. <b><i>Koḍ.</i> acci</b> cake of jaggery sugar with hollow in middle (formed in a mould). <b><i>Tu.</i> acci</b> form, model. <b><i>Te.</i> accu</b> stamp, impression, print, mould. / ? Cf. Turner, <i><b><xref=\"cdial\">CDIAL</xref=\"cdial\"></b></i>, no. 13096, Skt. <b>sañcaka-</b>, Panj. <b>sañcā, saccā</b> mould; Burrow 1967.41. DED(S, N) 44.\n\t\t\t</div></div>",
      "full_text": "47 Ta. accu mould, type. Ma. accu id. Ko. ac mould for casting iron. Ka. accu mould, impression, sign, type, stamp. Koḍ. acci cake of jaggery sugar with hollow in middle (formed in a mould). Tu. acci form, model. Te. accu stamp, impression, print, mould. / ? Cf. Turner, CDIAL , no. 13096, Skt. sañcaka- , Panj. sañcā, saccā mould; Burrow 1967.41. DED(S, N) 44.",
      "attestations": [
        {
          "language_abbrev": "Ta.",
          "language_name": "Tamil",
          "headwords": [
            "accu"
          ],
          "gloss": "mould, type. Ma. accu id.",
          "source_text": "Ta. accu"
        },
        {
          "language_abbrev": "Ma.",
          "language_name": "Malayalam",
          "headwords": [
            "accu"
          ],
          "gloss": "id.",
          "source_text": "Ma. accu"
        },
        {
          "language_abbrev": "Ko.",
          "language_name": "Kota",
          "headwords": [
            "ac"
          ],
          "gloss": "mould for casting iron.",
          "source_text": "Ko. ac"
        },
        {
          "language_abbrev": "Ka.",
          "language_name": "Kannada",
          "headwords": [
            "accu"
          ],
          "gloss": "mould, impression, sign, type, stamp.",
          "source_text": "Ka. accu"
        },
        {
          "language_abbrev": "Koḍ.",
          "language_name": "Kodagu",
          "headwords": [
            "acci"
          ],
          "gloss": "cake of jaggery sugar with hollow in middle (formed in a mould).",
          "source_text": "Koḍ. acci"
        },
        {
          "language_abbrev": "Tu.",
          "language_name": "Tulu",
          "headwords": [
            "acci"
          ],
          "gloss": "form, model.",
          "source_text": "Tu. acci"
        },
        {
          "language_abbrev": "Te.",
          "language_name": "Telugu",
          "headwords": [
            "accu"
          ],
          "gloss": "stamp, impression, print, mould. / ? Cf. Turner, CDIAL , no. 13096,",
          "source_text": "Te. accu"
        }
      ]
    },
```

The data from the starling json:

```json
"_sub_entries": [
        {
          "_url": "https://starlingdb.org/cgi-bin/response.cgi?single=1&basename=%2fdata%2fdrav%2fsdret&text_number=42&root=config",
          "_depth": 1,
          "Proto-South Dravidian": "*ac-",
          "Meaning": "mould, type",
          "Dravidian etymology": "Dravidian etymology",
          "Tamil": "accu",
          "Tamil meaning": "mould, type",
          "Malayalam": "accu",
          "Malayalam meaning": "mould, type",
          "Kannada": "accu",
          "Kannada meaning": "mould, impression, sign, type, stamp",
          "Kodagu": "acci",
          "Kodagu meaning": "cake of jaggery sugar with hollow in middle (formed in a mould)",
          "Tulu": "acci",
          "Tulu meaning": "form, model",
          "Proto-Nilgiri": "*as (*-c)",
          "Notes": "In Kodagu and Tulu the root is augmented by the nominal suffix-i.",
          "Number in DED": "0047",
          "_content_hash": "cff6734b2c76e788619b6570e6d57aa2",
          "_sub_entries": [
            {
              "_url": "https://starlingdb.org/cgi-bin/response.cgi?single=1&basename=%2fdata%2fdrav%2fktet&text_number=1157&root=config",
              "_depth": 2,
              "Proto-Nilgiri": "*as (*-c)",
              "Meaning": "mould for casting iron",
              "South Dravidian etymology": "South Dravidian etymology",
              "Kota": "ac",
              "Number in DED": "47",
              "_content_hash": "8d8661c428b70e65d64ef685dc4eb440"
            }
          ]
        },
```

As youc can see there's an issue with the burrow_corpus.cleaned.json where we need to have the "gloss" for the "language_name": "Malayalam", entry be the same one for the "Tamil" one there's an issue with the scraper when it sees "id." in the gloss. I think for robustibility we should fix it in the burrow scraper but we should also repair the json to continue with the cross validation.

---

Bug found:

Root Cause: `burrow_entry_parser.py` ΓÇö `_extract_gloss` (line 218)

The parser correctly extracts the text between language spans. For DED #47, the raw HTML is:

```
<b><i>Ta.</i> accu</b> mould, type. <b><i>Ma.</i> accu</b> id. <b><i>Ko.</i> ac</b> ...
```

`_extract_gloss` slices from the end of the `Ma. accu` match to the start of the next `Ko.` span - capturing `"id."` verbatim. **There's no logic to resolve lexicographic `"id."` (idem = "same as previous") back to the preceding gloss.**

---

Existing Repair Script: `repair_burrow_corpus_glosses.py` - `_normalize_spacing` (line 66) and `repair_corpus` (line 73)

This script already exists to fix gloss artifacts, but it only handles spacing/concatenation issues. It has a comment about `id.` (line 68-69) but only normalizes `"id.; "` spacing - **it never substitutes `"id."` with the actual meaning from the prior attestation**.

Two Fixes


**Scraper bug**, `burrow_entry_parser.py:218` (`_extract_gloss`), After extracting a gloss, if it's `"id."` (or starts with `"id."`) look back at the previous attestation's gloss and use it
**JSON repair**, `repair_burrow_corpus_glosses.py:73` (`repair_corpus`), Add a pass that iterates attestations in order and substitutes `"id."` glosses with the prior non-`"id."` gloss within the same entry

The cleaned JSON (`burrow_corpus.cleaned.json`) will also need to be regenerated once the repair script is fixed, or patched directly for DED #47 specifically.
