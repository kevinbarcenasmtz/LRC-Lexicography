# LEX Data Scheme

## 1 Schema for Required Data Columns

* **ID**
	* unique alphanumeric
	* A unique identifier for the data row.  Succinctly: the ID for the word.
* **Entry**
	* string
	* A representation of the word being described in Roman transliteration.  Succinctly: the word in Roman letters.
	* This functions roughly as the headword in a dictionary.
		* Note: when dealing with etymologies, the individual etyma are likewise treated as headwords.  A particular etymon is listed as an Entry, and its Language will be the language or proto-language to which it belongs.  The unique ID for this etymon will be the entry in the ID column of the same row.
* **Meaning**
	* string
	* A meaning or gloss for the Entry, i.e. for the word described.
* **Part of Speech**
	* string
	* The part of speech of the word listed as the Entry.
* **Language**
	* string
	* The name of the language to whose lexicon the Entry belongs.
* **Source**
	* string
	* An abbreviation referring to the source quoted in the accompanying bibliographical data from which the current Entry derives.
* **Page Number**
	* alphanumeric
	* The number of the page (e.g. `'132'`), folio (e.g. `'21r'`), or other serial place indicator in the Source from which the Entry derives and where the Full Original Entry can be found.
* **Full Original Entry**
	* string
	* The full text pertaining to the Entry as found in the Source.
* **Editors**
	* JSON
	* A string listing editor names and dates of edits as a JSON object:
	```javascript
		{
			'editors': [
				{
					'editor1': 'name1', 
					'dates1': [YYYY-MM-DD, YYYY-MM-DD]
				}, 
				{
					'editor2': 'name2', 
					'dates2': [YYYY-MM-DD, YYYY-MM-DD, YYYY-MM-DD]
				} 
			]
		}
	```



## 2 Schema for Optional Data Columns

### 2.1 Orthographic Data

* **Entry in Original Script**
	* string
	* The representation of the Entry, or word, in (one of) its original writing system(s).
* **Alternate Forms**
	* list of strings
	* Comma-separated list of alternate spellings of the Entry.


### 2.2 Etymological Data

* **Etymon**
	* string
	* The immediate **parent** word of the current Entry.
	* For example, in the evolution PIE *ph2ter* > Proto-Italic *pater-* > Latin *pater* > Spanish *padre*, the Entry *padre* would list Latin *pater* as Etymon.
		* Note that PIE *ph2ter*, for example, would itself necessarily be listed as an Entry, with a unique ID, in a separate row.  Its Language would be listed as Proto-Indo-European.
* **Etymon ID**
	* alphanumeric
	* The ID of the word listed as Etymon in the row where it appears as Entry.  Succinctly: the ID of the parent word.
		* Continue the example of PIE *ph2ter* > Proto-Italic *pater-* > Latin *pater* > Spanish *padre*.  Within the data row for *padre*, the Etymon would be listed as Latin *pater*, and the ID from the row defining *pater* would be cross-listed here in the Etymon ID in *padre*'s data.



### 2.3 Semantic Data

* **Semantic Tag**
	* list of strings
	* A comma-separated list of the finest-grained representatives from a hierarchical meaning category scheme.  For example, `soil` for a 2-level scheme with hierarchy `physical world > soil`.


### 2.4 Ancillary Data

* **Notes**
	* string
	* Any additional comments pertinent to the Entry.



## Explanation by Type of Information

* Example sentences
* Morpheme-by-morpheme analysis
* Parts of speech
* Multiple sources for the same headword
* Semantic tagging
* Grammatical information tagging
* Language tree for etymologies… is it different from ours?  Etymologies need to be pegged to our existing tree (list of language names), or we need to discuss special requirements (also provide Glotto-code…)



## Notes

### SemitiLex Columns

#### Verbs

* ID
* meaning
* semantic tag
* Etymon ID
* Etymon
* root
* root in script
* Part of Speech
* language
* prefix Conj 1
* prefix Conj 1 IPA
* Prefix conj 2
* Prefix conj 2 IPA
* Suffix conj
* Suffix conj IPA
* infinitive
* infinitive IPA
* participle
* participle IPA
* PC thematic vowel
* SC thematic vowel
* stem
* complement (separate complements with semi colon if they have a different meaning)
* tag
* Donor language (in case of borrowing)
* Donor word (in donor language)
* Data source
* notes


#### Other

* ID
* meaning
* semantic tag
* Etymon ID
* Etymon ("Proto" word)
* pS root
* Part of Speech
* language
* script
* transliteration
* Sem normalization
* IPA singular
* gender
* f markedness
* pS pattern
* Sem normalization pl
* IPA plural
* pS plural pattern
* PS plural suffix
* deptotic
* Tag (popularity) insert word. rate, archaic, wanderwort, borrowed. ambiguous. no derivation [for primary nouns] primary nouns [divide with commas], deptotic
* Donor language (in cases of borrowing)
* Donor word (in the original language)
* data source
* Notes

### MayaLex Columns

* Meaning
* Semantic Tag
* Etymon
* Language
* Part of Speech
* Root
* Headword (Kaufman spelling)
* Headword (Source spelling)
* Headword (Practical orthography)
* Headword (IPA)
* Definition
* Definition (Practical orthography)
* Meaning (Spanish)
* Meaning Spanish (Unmodernized)
* Meaning (English)
* English Part of Speech
* Spanish Part of Speech
* Full Original Entry
* Alternate Forms/Spellings
* Manuscript Page Number
* Source
* Other
* Editors

## Temporary Hybrid Schema: Etyma vs. Reflexes

* Common
	* Entry
		* Original Script
		* Variants
		* Romanized
		* IPA
	* Meaning (Gloss)
	* Source
	* Page Number
	* Full Original Entry
	* Extra Data
* Reflexes
	* Reflex ID
	* Part of Speech
	* Language
* Etyma
	* Proto-Language
	* Semantic Fields
	* Reflex IDs (list of words descending from this etymon)

