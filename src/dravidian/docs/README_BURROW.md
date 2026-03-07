# Burrow DED Cross-Validation System

Robust scraper and validation workflow for cross-validating StarlingDB Dravidian data against the authoritative Burrow & Emeneau Dravidian Etymological Dictionary (DDSA).

## Features

- **Two-tier query strategy**: DED number (primary) → headword fallback
- **Production-grade robustness**: Retry logic, exponential backoff, comprehensive error tracking
- **Checkpoint system**: Resume interrupted validations
- **Rate limiting**: Respectful scraping with configurable delays
- **Comprehensive reporting**: JSON + CSV outputs with detailed match statistics

## Files

- `burrow_scraper.py` - Core scraper with retry logic and error handling
- `burrow_validation.py` - Validation workflow for StarlingDB data
- `burrow_test_queries.py` - Simple test script for query validation

## Quick Start

### 1. Test Basic Queries

```bash
python burrow_test_queries.py
```

This runs a quick test of the query strategies against Burrow's website.

### 2. Test Validation Workflow (10 entries)

```bash
python burrow_validation.py /path/to/your/starling_export.csv --test
```

### 3. Run Full Validation

```bash
python burrow_validation.py /path/to/your/starling_export.csv
```

## Input Requirements

Your StarlingDB CSV must have these columns:
- `ID` - Entry identifier
- `Headword` - Language form
- `Language` - Language name
- `Number in DED` (optional) - DED etymon number
- `Meaning` (optional) - Gloss

## Output Files

### validation_output/
- `validation_results.csv` - Main validation results
- `validation_results.json` - Full results with HTML
- `validation_summary.json` - Summary statistics
- `burrow_cache/burrow_results.json` - Raw Burrow query results
- `burrow_cache/failed_queries.csv` - Failed queries log

## Validation Match Statuses

- **ded_match** - Found via DED number, primary entry confirmed
- **ded_cross_ref_only** - DED number found but only as cross-reference
- **headword_confirms_ded** - Headword search confirms DED number
- **headword_different_ded** - Headword found but different DED number
- **headword_match_new_ded** - Found via headword, discovered new DED number
- **not_found** - Not found in Burrow by either method

## Configuration

### Batch Size
Control memory usage and checkpointing frequency:
```bash
python burrow_validation.py data.csv --batch-size 100
```

### Output Directory
Specify custom output location:
```bash
python burrow_validation.py data.csv --output-dir results_2024
```

## Query Strategy Details

### Tier 1: DED Number Query
- **Mode**: Full-text search (unchecked)
- **URL**: `?qs={number}`
- **Primary detection**: Entry starts with number and has content
- **Filters**: Excludes cross-references (`...`) and page artifacts

### Tier 2: Headword Fallback
- **Mode**: Exact headword match (searchhws=yes)
- **URL**: `?qs={headword}&searchhws=yes&matchtype=exact`
- **Preprocessing**: Strips asterisks and hyphens from proto-forms
- **Use case**: When DED number missing or primary query fails

## Error Handling

- **Retry logic**: 3 attempts with exponential backoff
- **Timeout**: 45 seconds per request
- **Failed queries**: Logged to CSV for manual review
- **Checkpoints**: Save progress after each batch

## Rate Limiting

- **Between queries**: 0.5 seconds
- **Between batches**: Automatic via checkpoint system
- **Respectful**: Follows robots.txt, reasonable delays

## Example Workflow

```bash
# 1. Quick test with sample queries
python burrow_test_queries.py

# 2. Validate small test set (10 entries)
python burrow_validation.py Proto_Dravidian.csv --test

# 3. Review test results
cat validation_output/validation_summary.json

# 4. Run full validation
python burrow_validation.py Proto_Dravidian.csv --batch-size 50

# 5. Check for failures
cat validation_output/burrow_cache/failed_queries.csv
```

## Troubleshooting

### "No DED number" entries
- System automatically tries headword fallback
- Check `query_method` column in results

### High failure rate
- Check network connection
- Review `failed_queries.csv` for error patterns
- Consider increasing `--batch-size` to reduce request frequency

### Resume interrupted validation
- Delete `validation_checkpoint.json` to restart
- Keep checkpoint to resume from last batch

## Dependencies

```bash
pip install requests beautifulsoup4 pandas
```

## Next Steps

After validation:
1. Review `validation_summary.json` for coverage statistics
2. Examine entries with `match_status='headword_different_ded'` for potential data issues
3. Manual review of `not_found` entries
4. Cross-reference with StarlingDB for discrepancies
