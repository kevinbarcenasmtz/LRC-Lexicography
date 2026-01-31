-- ============================================================================
-- Nahuatl Dictionary Database Schema (Normalized SQLite)
-- Based on Drupal structure from nahuatl.wired-humanities.org
-- ============================================================================

-- Drop existing tables if they exist (for clean rebuilds)
DROP TABLE IF EXISTS entry_cross_references;
DROP TABLE IF EXISTS entry_audio;
DROP TABLE IF EXISTS audio_files;
DROP TABLE IF EXISTS entry_themes;
DROP TABLE IF EXISTS themes;
DROP TABLE IF EXISTS attestations;
DROP TABLE IF EXISTS authority_citations;
DROP TABLE IF EXISTS dictionary_entries;

-- ============================================================================
-- CORE DICTIONARY ENTRIES
-- ============================================================================

CREATE TABLE dictionary_entries (
    node_id INTEGER PRIMARY KEY,
    
    -- Standard WHP fields
    headword TEXT,                          -- field_wordorparticle
    orthographic_variants TEXT,             -- field_variants
    ipa_spelling TEXT,                      -- field_ipaspelling
    translation_english TEXT,               -- field_translation1
    spanish_loanword TEXT,                  -- field_spanish_loanword (Boolean as TEXT: "Yes"/"No")
    
    -- IDIEZ fields (Huastecan Nahuatl)
    headword_idiez TEXT,                    -- field_head_idiez
    translation_english_idiez TEXT,         -- field_eshort_idiez
    definition_nahuatl_idiez TEXT,          -- field_ndef_idiez
    definition_spanish_idiez TEXT,          -- field_sdef_idiez
    morfologia_idiez TEXT,                  -- field_morf1_idiez
    gramatica_idiez TEXT,                   -- field_gramn_idiez
    
    -- Metadata
    source_dataset TEXT CHECK(source_dataset IN ('WHP', 'IDIEZ', 'HYBRID')),
    url_alias TEXT,                         -- e.g., "/content/ne-0"
    created_timestamp TEXT,                 -- ISO 8601 format
    scrape_timestamp TEXT,                  -- When we scraped this data
    
    UNIQUE(node_id)
);

-- ============================================================================
-- AUTHORITY CITATIONS (6 possible sources per entry)
-- ============================================================================

CREATE TABLE authority_citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    
    -- Authority source (which field it came from)
    authority_name TEXT NOT NULL CHECK(authority_name IN (
        'Molina',           -- field_authority1
        'Karttunen',        -- field_authority2
        'Carochi',          -- field_authority3
        'Olmos',            -- field_authority4
        'Lockhart',         -- field_authority6 (Nahuatl as Written)
        'Other'             -- field_authority5 or unlabeled
    )),
    
    -- Full citation text (includes HTML markup from website)
    citation_text TEXT NOT NULL,
    
    -- Order within entry (if multiple citations from same source)
    citation_order INTEGER DEFAULT 0,
    
    FOREIGN KEY (node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE
);

-- ============================================================================
-- ATTESTATIONS (Examples/usage from sources)
-- ============================================================================

CREATE TABLE attestations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    
    -- Language of attestation
    language TEXT CHECK(language IN ('English', 'Spanish')),
    
    -- Full attestation text
    attestation_text TEXT NOT NULL,
    
    -- Source field in Drupal
    source_field TEXT CHECK(source_field IN (
        'field_additionalnotes_lang1',  -- English attestations
        'field_additionalnotes_lang2'   -- Spanish attestations
    )),
    
    FOREIGN KEY (node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE
);

-- ============================================================================
-- THEMES (Taxonomy Terms)
-- ============================================================================

CREATE TABLE themes (
    tid INTEGER PRIMARY KEY,                -- Taxonomy term ID
    name TEXT NOT NULL,                     -- e.g., "Water", "Agriculture"
    slug TEXT UNIQUE NOT NULL,              -- e.g., "water", "agriculture-gardens-stockraising"
    description TEXT,
    vocabulary_id INTEGER DEFAULT 1,        -- All themes belong to same vocabulary
    
    UNIQUE(tid),
    UNIQUE(slug)
);

-- ============================================================================
-- ENTRY-THEME RELATIONSHIPS (Many-to-Many)
-- ============================================================================

CREATE TABLE entry_themes (
    entry_node_id INTEGER NOT NULL,
    theme_tid INTEGER NOT NULL,
    delta INTEGER DEFAULT 0,                -- Order of themes for this entry
    
    PRIMARY KEY (entry_node_id, theme_tid),
    FOREIGN KEY (entry_node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE,
    FOREIGN KEY (theme_tid) REFERENCES themes(tid) ON DELETE CASCADE
);

-- ============================================================================
-- AUDIO FILES (Pronunciation recordings)
-- ============================================================================

CREATE TABLE audio_files (
    node_id INTEGER PRIMARY KEY,            -- Audio node ID
    headword TEXT NOT NULL,                 -- field_head_idiez (tlahtolli)
    
    -- File paths (relative URLs)
    file_wav TEXT,                          -- field_audio_file_wav
    file_mp3 TEXT,                          -- field_audio_file_mp3
    file_aif TEXT,                          -- field_audio_file_aif
    
    -- Metadata
    speaker TEXT,                           -- field_speaker
    date_recorded TEXT,                     -- field_data_set_date
    
    -- Scraping metadata
    url_alias TEXT,
    scrape_timestamp TEXT,
    
    UNIQUE(node_id)
);

-- ============================================================================
-- ENTRY-AUDIO RELATIONSHIPS (Many-to-Many)
-- ============================================================================

CREATE TABLE entry_audio (
    entry_node_id INTEGER NOT NULL,
    audio_node_id INTEGER NOT NULL,
   
    -- Type of reference
    reference_type TEXT CHECK(reference_type IN (
        'headword',     -- field_audio_headword
        'example'       -- field_audio_examples_in_context
    )),
   
    delta INTEGER DEFAULT 0,                -- Order if multiple audio files
   
    PRIMARY KEY (entry_node_id, audio_node_id, delta),  -- Add delta here!
    FOREIGN KEY (entry_node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE,
    FOREIGN KEY (audio_node_id) REFERENCES audio_files(node_id) ON DELETE CASCADE
);

-- ============================================================================
-- ENTRY CROSS-REFERENCES (See Also links)
-- ============================================================================

CREATE TABLE entry_cross_references (
    source_node_id INTEGER NOT NULL,
    target_node_id INTEGER NOT NULL,
    
    -- Optional: type of relationship if we can determine it
    reference_type TEXT,                    -- e.g., "see_also", "variant_of"
    
    PRIMARY KEY (source_node_id, target_node_id),
    FOREIGN KEY (source_node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE,
    FOREIGN KEY (target_node_id) REFERENCES dictionary_entries(node_id) ON DELETE CASCADE
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Dictionary entry lookups
CREATE INDEX idx_entries_headword ON dictionary_entries(headword);
CREATE INDEX idx_entries_headword_idiez ON dictionary_entries(headword_idiez);
CREATE INDEX idx_entries_source ON dictionary_entries(source_dataset);
CREATE INDEX idx_entries_url ON dictionary_entries(url_alias);

-- Authority citations
CREATE INDEX idx_citations_node ON authority_citations(node_id);
CREATE INDEX idx_citations_authority ON authority_citations(authority_name);

-- Attestations
CREATE INDEX idx_attestations_node ON attestations(node_id);

-- Themes
CREATE INDEX idx_themes_slug ON themes(slug);
CREATE INDEX idx_entry_themes_entry ON entry_themes(entry_node_id);
CREATE INDEX idx_entry_themes_theme ON entry_themes(theme_tid);

-- Audio
CREATE INDEX idx_audio_headword ON audio_files(headword);
CREATE INDEX idx_entry_audio_entry ON entry_audio(entry_node_id);
CREATE INDEX idx_entry_audio_audio ON entry_audio(audio_node_id);

-- Cross references
CREATE INDEX idx_xref_source ON entry_cross_references(source_node_id);
CREATE INDEX idx_xref_target ON entry_cross_references(target_node_id);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Complete entry with all themes
CREATE VIEW v_entries_with_themes AS
SELECT 
    de.node_id,
    de.headword,
    de.headword_idiez,
    de.translation_english,
    de.source_dataset,
    GROUP_CONCAT(t.name, '; ') AS themes
FROM dictionary_entries de
LEFT JOIN entry_themes et ON de.node_id = et.entry_node_id
LEFT JOIN themes t ON et.theme_tid = t.tid
GROUP BY de.node_id;

-- Entries with audio files
CREATE VIEW v_entries_with_audio AS
SELECT 
    de.node_id,
    de.headword,
    ea.audio_node_id,
    af.file_mp3,
    af.speaker
FROM dictionary_entries de
INNER JOIN entry_audio ea ON de.node_id = ea.entry_node_id
INNER JOIN audio_files af ON ea.audio_node_id = af.node_id;

-- ============================================================================
-- METADATA TABLE (Track scraping progress)
-- ============================================================================

CREATE TABLE scrape_metadata (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_timestamp TEXT
);

-- Initialize with default values
INSERT INTO scrape_metadata (key, value, updated_timestamp) VALUES
    ('last_scrape_date', NULL, NULL),
    ('total_entries_scraped', '0', NULL),
    ('total_themes_scraped', '0', NULL),
    ('total_audio_scraped', '0', NULL),
    ('scraper_version', '1.0.0', datetime('now'));