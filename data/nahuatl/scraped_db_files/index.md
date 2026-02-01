# Context of this directory

In this directory lives the db files used with SQLite and DB Browser that hold the
data we scraped from the <https://nahuatl.wired-humanities.org/> website. This
accompanies the data that was initially given to us from that project but that we
found lacking compared to what they had in their actual website.

nahuatl_scraped.db has the scraped data in relational tables. nahuatl_backup.db is a backup
just in case but might be out of date. look at [schema.sql](schema.sql) for more context of the
schema of the relational tables used to store the data scraped.

---

**Note the following overview is more for the box folder not for the github repo scraped file folders**

## Overview

This directory contains scraped data from nahuatl.wired-humanities.org (Wired Humanities Project). The data represents a complete Nahuatl dictionary with approximately 31,000+ entries, organized according to the normalized database schema defined in `schema.sql`.

Last scrape: October 2025

---

## Core Dictionary Data

### dictionary_entries.csv (9.2 MB)

Main dictionary entries table containing:

- Headwords (WHP and IDIEZ variants)
- Translations (English/Spanish)
- IPA spellings and orthographic variants
- Morphological and grammatical information
- Metadata (node IDs, URLs, timestamps)

**Key fields**: `node_id`, `headword`, `headword_idiez`, `translation_english`, `source_dataset`

### authority_citations.csv (10.2 MB)

Citations from six major Nahuatl sources per entry:

- Molina (1571)
- Karttunen (1983)
- Carochi (1645)
- Olmos (1547)
- Lockhart (Nahuatl as Written)
- Other sources

**Relationship**: Many citations per entry (6 possible sources)
**Key fields**: `node_id`, `authority_name`, `citation_text`

### attestations.csv (9.6 MB)

Usage examples and additional notes in English and Spanish.

**Relationship**: Multiple attestations per entry
**Key fields**: `node_id`, `language`, `attestation_text`, `source_field`

---

## Taxonomy & Classification

### themes.csv (10.4 KB)

Thematic taxonomy terms (e.g., "Water", "Agriculture", "Body Parts").

**Records**: ~150 themes
**Key fields**: `tid`, `name`, `slug`

### entry_themes.csv (1.1 MB)

Many-to-many relationships linking entries to thematic categories.

**Relationship**: Entries can have multiple themes; themes can have multiple entries
**Key fields**: `entry_node_id`, `theme_tid`, `delta`

## theme_slugs_to_scrape.txt

All the different theme slugs (just themes in general present in the WHP website)
that we scraped.

---

## Audio Data

### audio_files.csv (812.7 KB)

Metadata for pronunciation recordings from IDIEZ speakers.

**Records**: ~4,500 audio nodes
**Formats**: WAV, MP3, AIF
**Key fields**: `node_id`, `headword`, `file_mp3`, `speaker`, `date_recorded`

### entry_audio.csv (47.1 KB)

Many-to-many relationships linking dictionary entries to audio files.

**Relationship**: Entries can have multiple audio files (headword pronunciations and example contexts)
**Key fields**: `entry_node_id`, `audio_node_id`, `reference_type`

### audio_files/ (4,542 files)

Downloaded audio files in MP3, WAV, and AIF formats.
Seperated into different folders wav/, mp3/, and /aif. There's also a scraper.log
just showing what the logs when we scraped the audio files.

**Organization**: Files named by node ID (e.g., `12345.mp3`)
**Total size**: ~several GB

---

## Scraping Metadata & Logs

### audio_scraper.log (549 KB)

Detailed log output from the audio scraping process, including:

- Download progress
- HTTP responses
- Error messages
- Timing information

### audio_download_log.csv (974.1 KB)

Tracking file for audio downloads with status information.

**Purpose**: Resume capability, deduplication, error tracking
**Key fields**: `node_id`, `download_status`, `file_path`, `timestamp`

### audio_scrape_errors.csv (141 B)

Records of failed audio downloads.

**Size**: Minimal (141 bytes indicates very few errors)

---

## Scraping Input Files

### theme_slugs_to_scrape.txt (1.9 KB)

List of theme URL slugs used as input for the thematic scraping process.

**Format**: One slug per line (e.g., `agriculture-gardens-stockraising`)
**Purpose**: Systematic scraping of all thematic entry pages

### audio_nodes_to_scrape.txt (14.5 KB)

List of audio node IDs to download.

**Format**: One node ID per line
**Purpose**: Batch download queue for audio files

---

## Data Integrity Notes

- All CSV files use UTF-8 encoding
- HTML markup is preserved in citation_text and attestation_text fields
- Node IDs are consistent across all files as foreign keys
- Timestamps in ISO 8601 format
- Cross-references between entries are not yet extracted (see schema's `entry_cross_references` table)

---

---

## Missing Data / To-Do

### entry_cross_references (NOT YET EXTRACTED)
**Status**: Table exists in schema but contains no data

Cross-references between dictionary entries (e.g., "See also" links) have not been extracted from the source website. These relationships exist in the HTML of individual entry pages but require additional scraping work.

**Next steps**:
1. Identify HTML structure containing cross-references on entry pages
2. Extract source/target node ID pairs
3. Classify reference types if possible (variant_of, see_also, related_term)
4. Create `entry_cross_references.csv` and populate database table

**Expected data volume**: Unknown (depends on how extensively entries cross-reference each other)

## Loading into Database

To populate a SQLite database from these CSVs:

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('nahuatl_dictionary.db')

# Execute schema.sql first
with open('schema.sql', 'r') as f:
    conn.executescript(f.read())

# Load CSVs
pd.read_csv('dictionary_entries.csv').to_sql('dictionary_entries', conn, if_exists='append', index=False)
pd.read_csv('authority_citations.csv').to_sql('authority_citations', conn, if_exists='append', index=False)
pd.read_csv('attestations.csv').to_sql('attestations', conn, if_exists='append', index=False)
pd.read_csv('themes.csv').to_sql('themes', conn, if_exists='append', index=False)
pd.read_csv('entry_themes.csv').to_sql('entry_themes', conn, if_exists='append', index=False)
pd.read_csv('audio_files.csv').to_sql('audio_files', conn, if_exists='append', index=False)
pd.read_csv('entry_audio.csv').to_sql('entry_audio', conn, if_exists='append', index=False)
```
