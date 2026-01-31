# Changelog, I'll just update this markdown file with the changes I've done

- Eventually need to create a github repository for MayaLEX
- Ideally, I want to parse all of the IDIEZ file and seperate the content of the entries per they're html tags.

## Structure of the IDIEZ file, Reference Table

| Column                   | Description                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------- |
| **Ref**                  | Acts as the identifier of the entry                                                         |
| **OND_Node_Title**       | Title of this entry's main word (OND possibly refers to the dictionary organization system) |
| **tlahtolli**            | The Nahuatl word for "word"                                                                 |
| **IDIEZ gramática**      | Grammar classification of the word origin from IDIEZ                                        |
| **IDIEZ def. náhuatl**   | Definition of the Nahuatl word in Nahuatl from IDIEZ                                        |
| **IDIEZ def. español**   | Definition of the Nahuatl word in Spanish from IDIEZ                                        |
| **SShort_IDIEZ**         | [Undetermined field]                                                                        |
| **IDIEZ traduc. inglés** | Nahuatl word translated in English from IDIEZ                                               |
| **IDIEZ morfología**     | Morphology of the Nahuatl word from IDIEZ                                                   |
| **Credit**               | Source information                                                                          |

IDIEZ: Instituto de Docencia e Investigación Etnológica de Zacatecas - an institute specializing in Nahuatl language education and research

## Structure of the WHP (Wired Humanities Projects) Early Nahuatl Data

| Column                                   | Description                                                                                      |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------ |
| **Ref**                                  | Acts as the identifier of the entry                                                              |
| **Headword**                             | Word of importance, of the entry                                                                 |
| **Orthographic Variants**                | Orthographic variants of the headword                                                            |
| **Principal English Translation**        | English translation of the Headword                                                              |
| **Attestations from sources in English** | Evidence of the source in English, contains HTML structure/tag                                   |
| **Attestations from sources in Spanish** | Evidence of the source in Spanish,  contains HTML structure/tag                                  |
| **Alonso de Molina**                     | Attestation/source from the Alonso de Molina, contains HTML structure/tag                        |
| **Frances Karttunen**                    | Attestation/source from the Frances Karttunen, contains HTML structure/tag (English)             |
| **Horacio Carochi / English**            | Attestation/source from the Horacio Carochi / English, contains HTML structure/tag (English)     |
| **Andrés de Olmos**                      | Attestation/source from the Andres de Olmos, contains HTML structure/tag (Spanish)               |
| **Lockhart’s Nahuatl as Written**        | Attestation/source from the Lockhart's Nahuatl as Written, contains HTML structure/tag (English) |
| **Themes**                               | These are related to semantic tags of the headword                                               |
| **Spanish Loanword**                     | Yes/No value                                                                                     |
