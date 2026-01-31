import requests
from bs4 import BeautifulSoup, Tag
import json
import sys
import io
from typing import List, Dict, Optional

# Force UTF-8 encoding for stdout (Windows compatibility)
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class IranianScraper:
    def __init__(self, url: str):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
    
    def extract_language_entry(self, div) -> Optional[Dict]:
        """Extract a language-specific entry with all its data"""
        if not isinstance(div, Tag):
            return None
            
        entry = {}
        
        # Get language name and metadata from data-notes attribute
        lang_span = div.find('span', class_='lang-name')
        if lang_span and isinstance(lang_span, Tag):
            entry['language'] = lang_span.get_text(strip=True)
            lang_metadata = lang_span.get('data-notes', '')
            if lang_metadata:
                entry['language_metadata'] = lang_metadata
        
        # Get the word/form (the unicode span content)
        unicode_span = div.find('span', class_='unicode')
        if unicode_span and isinstance(unicode_span, Tag):
            entry['form'] = unicode_span.get_text(strip=True)
        
        # Get superscript number if present
        sup = div.find('sup')
        if sup and isinstance(sup, Tag):
            entry['reference_number'] = sup.get_text(strip=True)
        
        # Get notes from expandable div
        expandable = div.find('div', class_='expandable')
        if expandable and isinstance(expandable, Tag):
            notes_div = expandable.find('div', class_='notes')
            if notes_div and isinstance(notes_div, Tag):
                entry['notes'] = notes_div.get_text(strip=True)
        
        return entry if entry else None
    
    def scrape_record(self, record_div) -> Dict:
        """Extract all data from a single record"""
        record = {
            'fields': {},
            'language_entries': []
        }
        
        # Process all divs in the record
        for div in record_div.find_all('div', recursive=False):
            if not isinstance(div, Tag):
                continue
            
            field_span = div.find('span', class_='fld')
            if field_span and isinstance(field_span, Tag):
                field_name = field_span.get_text(strip=True).rstrip(':')
                
                # Check if this is a language entry (has lang-name span)
                if div.find('span', class_='lang-name'):
                    lang_entry = self.extract_language_entry(div)
                    if lang_entry:
                        record['language_entries'].append(lang_entry)
                else:
                    # Regular field (Number, Word, etc.)
                    unicode_span = div.find('span', class_='unicode')
                    if unicode_span and isinstance(unicode_span, Tag):
                        record['fields'][field_name] = unicode_span.get_text(strip=True)
        
        return record
    
    def scrape(self) -> List[Dict]:
        """Scrape all records from the page"""
        print(f"Fetching: {self.url}")
        
        try:
            response = self.session.get(self.url, timeout=60)
            response.raise_for_status()
            print(f"[OK] Response received ({len(response.content)} bytes)")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Failed to fetch URL: {e}")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all records
        record_divs = soup.find_all('div', class_='results_record')
        print(f"[OK] Found {len(record_divs)} records")
        
        records = []
        for i, record_div in enumerate(record_divs, 1):
            try:
                record_data = self.scrape_record(record_div)
                records.append(record_data)
                
                # Progress update every 100 records
                if i % 100 == 0:
                    print(f"  ... processed {i} records")
                    
            except Exception as e:
                print(f"[WARN] Error on record {i}: {e}")
        
        print(f"[OK] Successfully scraped {len(records)} records")
        return records
    
    def save_to_json(self, records: List[Dict], filename: str = 'iranian_data.json'):
        """Save records to JSON file"""
        output = {
            'metadata': {
                'source_url': self.url,
                'total_records': len(records),
                'total_language_entries': sum(len(r['language_entries']) for r in records)
            },
            'records': records
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        print(f"[OK] Saved to {filename}")
        print(f"  - Records: {output['metadata']['total_records']}")
        print(f"  - Language entries: {output['metadata']['total_language_entries']}")


if __name__ == "__main__":
    URL = "https://starlingdb.org/cgi-bin/response.cgi?root=new100&basename=new100%2fier%2firn&limit=-1"
    
    scraper = IranianScraper(URL)
    records = scraper.scrape()
    
    if records:
        scraper.save_to_json(records, 'iranian_data.json')
        print("\n[COMPLETE]")
    else:
        print("\n[FAILED] No records scraped")