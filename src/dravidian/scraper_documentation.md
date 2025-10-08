# Dravidian StarLing Database Scraper - Documentation

## Project Overview
A Python web scraper designed to extract linguistic data from the StarLing Dravidian etymological database (starlingdb.org). The scraper recursively follows hierarchical linguistic relationships while avoiding circular references through content-based deduplication.

## Database Information
- **Target URL**: https://starlingdb.org/cgi-bin/response.cgi?root=config&basename=\data\drav\dravet
- **Total Records**: 2,211 records across 111 pages
- **Records per page**: 20
- **Database Structure**: Hierarchical proto-language reconstructions with cross-references

## Core Requirements

### 1. Scope and Input
- Manual configuration via two variables: START_URL and TOTAL_PAGES
- Generalizable to any StarLing database (not Dravidian-specific)
- Support for resuming interrupted scrapes via checkpoint system

### 2. Recursion Logic
- Follow ALL response.cgi links with plus-icon indicators (subquery_link class)
- Continue recursively until circular reference detected
- No URL-based deduplication - only content-based
- **Critical exclusion**: Skip "etymology" fields (they are back-references to parent entries)

### 3. Content Deduplication Strategy
**Evolution of approach:**
- **Initial attempt**: Hash only the "Notes" field
  - **Problem**: Many entries lack Notes fields, causing infinite recursion
- **Final solution**: Hash first proto-language field + its word form
  - Creates unique identifier like "Proto-Dravidian:*ac-"
  - Works for all entries regardless of Notes presence
  - Properly captures linguistic uniqueness

### 4. Data Extraction
- Fully generic field extraction (no hardcoded field names)
- Extracts ALL fields from results_record divs
- Field identification via span.fld class
- Value extraction from span.unicode class or fallback to div text

### 5. Output Format
- Single JSON file: starling_complete_data.json
- Hierarchical structure preserved via _sub_entries arrays
- Metadata includes: pages scraped, total records, unique entries count
- Failed URLs logged separately to failed_urls.csv

### 6. Reliability Features
- Checkpoint system (scraper_checkpoint.json) for resume capability
- Exponential backoff retry logic (3 attempts, 45-second timeout)
- Progress saved after each page
- Failed request tracking with timestamps

## Key Technical Decisions

### HTML Structure Parsing
**Target HTML pattern:**
```html
<div class="results_record">
  <div>
    <span class="fld">Proto-Dravidian:</span>
    <span class="unicode">*ac-</span>
    <div class="subquery_link">
      <img src="/icons/plus-8.png" onclick="...">
    </div>
  </div>
</div>
```

### Field Name Processing
**Bug discovered**: Field names had trailing spaces and colons
- Original: "Notes :" and "Proto-Dravidian :"
- Solution: `.rstrip(':').strip()` to normalize field names

### Link Following Logic
Only follow links with plus-icon indicators:
```python
subquery_link = div.find('div', class_='subquery_link')
if subquery_link and isinstance(subquery_link, Tag):
    plus_img = subquery_link.find('img', onclick=True)
```

**Critical filter**: Skip etymology fields
```python
if 'etymology' in field_name.lower():
    continue
```

### Circular Reference Detection
```python
def create_content_hash(self, entry_data: Dict) -> Optional[str]:
    for key, value in entry_data.items():
        if key.startswith('_') or key in ['Meaning', 'Notes', 'Number in DED']:
            continue
        if value and value.strip():
            content = f"{key.strip()}:{value.strip()}"
            return hashlib.md5(content.encode('utf-8')).hexdigest()
    return None
```

### Type Safety Fixes
BeautifulSoup returns Union types that cause Pylance errors:
```python
# Always check isinstance(element, Tag) before accessing Tag-specific methods
if subquery_link and isinstance(subquery_link, Tag):
    plus_img = subquery_link.find('img', onclick=True)
    if plus_img and isinstance(plus_img, Tag):
        onclick = plus_img.get('onclick', '')
        if onclick:
            match = re.search(r"'([^']+)'", str(onclick))
```

## Configuration Parameters

### Session Headers
```python
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
'Referer': 'https://starlingdb.org/cgi-bin/main.cgi'
```

### Timing Parameters
- **Page delay**: 3 seconds between pages
- **Sub-entry delay**: 0.7 + (depth * 0.1) seconds
- **Retry backoff**: 2^attempt * 2 seconds
- **Request timeout**: 45 seconds

### Retry Logic
- Maximum retries: 3
- Handles: Timeout exceptions, RequestException
- All failures logged to failed_urls list

## Page Numbering Logic
```python
if page_num == 1:
    url = self.start_url
else:
    first = 1 + (page_num - 1) * 20
    url = f"{self.start_url}&first={first}"
```

## Known Issues and Solutions

### Windows Encoding Issue
Unicode symbols in print statements cause cp1252 encoding errors when redirecting to file.

**Solution**:
```python
import sys
import io

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
```

### Checkpoint Resume Logic
The checkpoint system tracks:
- start_url (verification that checkpoint matches current job)
- current_page (resume point)
- seen_content_hashes (deduplication state)
- records_scraped count

Load existing JSON data to append rather than overwrite.

## Usage Examples

### Full scrape (pages 1-111):
```python
START_URL = r"https://starlingdb.org/cgi-bin/response.cgi?root=config&basename=\data\drav\dravet"
TOTAL_PAGES = 111
START_PAGE = 1

scraper = StarLingGeneralScraper(START_URL, TOTAL_PAGES, START_PAGE)
scraper.scrape_all()
```

### Resume from page 21:
```python
START_URL = r"https://starlingdb.org/cgi-bin/response.cgi?root=config&basename=\data\drav\dravet&first=401"
TOTAL_PAGES = 111
START_PAGE = 21

scraper = StarLingGeneralScraper(START_URL, TOTAL_PAGES, START_PAGE)
scraper.scrape_all()
```

### With logging to file:
```bash
python dravidian_scraper.py > scrape.log 2>&1
```

## Output Files

1. **starling_complete_data.json**: Main output with all scraped data
2. **failed_urls.csv**: List of URLs that failed after max retries
3. **scraper_checkpoint.json**: Resume state (deleted on completion)
4. **scrape.log**: Console output (if redirected)

## Testing Results
- First run: Pages 1-20, 400 records, 1,975 unique entries
- Target: Pages 1-111, 2,211 records
- Circular reference detection: Working correctly
- Etymology exclusion: Prevents infinite back-reference loops

## Dependencies
```python
requests
beautifulsoup4
json (stdlib)
time (stdlib)
urllib.parse (stdlib)
typing (stdlib)
re (stdlib)
hashlib (stdlib)
os (stdlib)
csv (stdlib)
```