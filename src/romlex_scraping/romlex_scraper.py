import requests
import xml.etree.ElementTree as ET
from time import sleep
import random
from pathlib import Path
import json
import csv
from datetime import datetime
import string
import pandas as pd
import sys

from romlex_config import (
    DIALECT_NAMES,
    PATTERN_MATCH_MODES,
    SEARCH_PARAMS,
    API_CONFIG,
    SCRAPER_CONFIG,
    HEADERS,
    USER_AGENTS
)

class RomLexScraper:
    def __init__(self, dialect_code, output_dir=None):
        self.dialect_code = dialect_code
        self.output_dir = Path(output_dir or SCRAPER_CONFIG['default_output_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = API_CONFIG['base_url']
        self.all_entries = []
        self.stats = {
            'dialect': dialect_code,
            'dialect_name': DIALECT_NAMES.get(dialect_code, 'Unknown'),
            'start_time': datetime.now().isoformat(),
            'total_queries': 0,
            'total_entries': 0,
            'failed_queries': 0,
            'letters_processed': []
        }
        
        self.session = requests.Session() if SCRAPER_CONFIG.get('use_session') else None
        self.headers = HEADERS.copy()
    
    def get_random_user_agent(self):
        """Get a random user agent from the pool"""
        if SCRAPER_CONFIG.get('rotate_user_agent'):
            return random.choice(USER_AGENTS)
        return self.headers.get('User-Agent')
    
    def query_romlex(self, search_term, translation=None, pattern_match=None, retry_count=0):
        """Query the RomLex database with retry logic"""
        
        translation = translation or API_CONFIG['default_translation']
        pattern_match = pattern_match or PATTERN_MATCH_MODES['prefix']
        
        params = {
            'st': search_term,
            'rev': SEARCH_PARAMS['reverse'],
            'cl1': self.dialect_code,
            'cl2': translation,
            'fi': SEARCH_PARAMS['file'],
            'pm': pattern_match,
            'ic': SEARCH_PARAMS['ignore_case'],
            'im': SEARCH_PARAMS['ignore_marks'],
            'wc': SEARCH_PARAMS['word_class']
        }
        
        headers = self.headers.copy()
        headers['User-Agent'] = self.get_random_user_agent() # type: ignore
        
        try:
            if self.session:
                response = self.session.get(
                    self.base_url, 
                    params=params, 
                    headers=headers, 
                    timeout=30
                )
            else:
                response = requests.get(
                    self.base_url, 
                    params=params, 
                    headers=headers, 
                    timeout=30
                )
            
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            self.stats['total_queries'] += 1
            
            root = ET.fromstring(response.content)
            entries = root.findall('.//entry')
            
            return entries, root
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and retry_count < SCRAPER_CONFIG['max_retries']:
                wait_time = SCRAPER_CONFIG['error_retry_delay'] * (SCRAPER_CONFIG['backoff_multiplier'] ** retry_count)
                jitter = random.uniform(0, 2)
                total_wait = wait_time + jitter
                
                print(f"    403 error, retrying in {total_wait:.1f}s (attempt {retry_count + 1}/{SCRAPER_CONFIG['max_retries']})")
                sleep(total_wait)
                
                return self.query_romlex(search_term, translation, pattern_match, retry_count + 1)
            else:
                raise
    
    def extract_entry_data(self, entry):
        """Extract all data from an entry element"""
        
        entry_data = {
            'id': entry.get('id'),
            'dialect': entry.get('dia'),
            'orthographic_form': '',
            'pos': '',
            'glosses': []
        }
        
        ortho_elem = entry.find('o')
        if ortho_elem is not None and ortho_elem.text:
            entry_data['orthographic_form'] = ortho_elem.text.strip()
        
        pos_elem = entry.find('pos')
        if pos_elem is not None and pos_elem.text:
            entry_data['pos'] = pos_elem.text.strip()
        
        for gloss_group in entry.findall('.//g'):
            gloss_senses = []
            for sense in gloss_group.findall('.//s'):
                translations = []
                for trans_elem in sense.findall('.//t'):
                    trans_data = {}
                    e_elem = trans_elem.find('e')
                    if e_elem is not None and e_elem.text:
                        trans_data['translation'] = e_elem.text.strip()
                    
                    h_elem = trans_elem.find('h')
                    if h_elem is not None and h_elem.text:
                        trans_data['hint'] = h_elem.text.strip()
                    
                    if trans_data:
                        translations.append(trans_data)
                
                if translations:
                    gloss_senses.append(translations)
            
            if gloss_senses:
                entry_data['glosses'].append(gloss_senses)
        
        return entry_data
    
    def flatten_entry_for_csv(self, entry):
        """Flatten entry structure for CSV/Excel export"""
        
        glosses_formatted = []
        
        for i, gloss_group in enumerate(entry['glosses'], 1):
            sense_parts = []
            for sense in gloss_group:
                translations = []
                for trans in sense:
                    # Handle missing 'translation' key
                    if 'translation' not in trans:
                        continue
                    
                    if 'hint' in trans:
                        translations.append(f"{trans['translation']} ({trans['hint']})")
                    else:
                        translations.append(trans['translation'])
                if translations:
                    sense_parts.append(', '.join(translations))
            if sense_parts:
                glosses_formatted.append('; '.join(sense_parts))
        
        return {
            'entry_id': entry['id'],
            'dialect_code': entry['dialect'],
            'headword': entry['orthographic_form'],
            'part_of_speech': entry['pos'],
            'gloss': ' | '.join(glosses_formatted)
        }
    
    def get_entries_recursive(self, prefix, depth=0):
        """Recursively fetch all entries, subdividing when hitting 200-entry limit"""
        
        indent = "  " * depth
        
        try:
            entries, root = self.query_romlex(prefix)
            count = len(entries)
            
            print(f"{indent}'{prefix}': {count} entries")
            
            if count < API_CONFIG['result_limit']:
                delay = SCRAPER_CONFIG['request_delay'] + random.uniform(0, 1.0)
                sleep(delay)
                return [self.extract_entry_data(e) for e in entries]
            
            print(f"{indent}  Limit hit, subdividing...")
            all_entries = []
            
            alphabet = string.ascii_lowercase
            for letter in alphabet:
                sub_prefix = prefix + letter
                sub_entries = self.get_entries_recursive(sub_prefix, depth + 1)
                all_entries.extend(sub_entries)
                delay = SCRAPER_CONFIG['request_delay'] + random.uniform(0, 1.0)
                sleep(delay)
            
            return all_entries
            
        except Exception as e:
            print(f"{indent}Error querying '{prefix}': {e}")
            self.stats['failed_queries'] += 1
            sleep(SCRAPER_CONFIG['error_retry_delay'])
            return []
    
    def scrape_full_dialect(self):
        """Scrape entire dialect dictionary"""
        
        print(f"Starting full scrape for dialect: {self.dialect_code}")
        print(f"Dialect name: {self.stats['dialect_name']}")
        print(f"Output directory: {self.output_dir}")
        print(f"Request delay: {SCRAPER_CONFIG['request_delay']}s (+ jitter)")
        print(f"User-Agent rotation: {SCRAPER_CONFIG.get('rotate_user_agent', False)}")
        print(f"Session management: {SCRAPER_CONFIG.get('use_session', False)}")
        
        alphabet = string.ascii_lowercase
        
        for i, letter in enumerate(alphabet, 1):
            print(f"\n[{i}/{len(alphabet)}] Processing letter '{letter}':")
            entries = self.get_entries_recursive(letter)
            self.all_entries.extend(entries)
            self.stats['letters_processed'].append({
                'letter': letter,
                'count': len(entries)
            })
            print(f"  Total for '{letter}': {len(entries)} entries")
            print(f"  Running total: {len(self.all_entries)} entries")
            
            if i < len(alphabet):
                inter_delay = SCRAPER_CONFIG.get('inter_letter_delay', 0)
                if inter_delay > 0:
                    print(f"  Waiting {inter_delay}s before next letter...")
                    sleep(inter_delay)
        
        self.stats['total_entries'] = len(self.all_entries)
        self.stats['end_time'] = datetime.now().isoformat()
        
        if self.session:
            self.session.close()
        
        self.save_results()
    
    def save_results(self):
        """Save scraped data in JSON, CSV, and XLSX formats"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        json_file = self.output_dir / f"{self.dialect_code}_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_entries, f, ensure_ascii=False, indent=2)
        
        flattened_entries = [self.flatten_entry_for_csv(e) for e in self.all_entries]
        
        csv_file = self.output_dir / f"{self.dialect_code}_{timestamp}.csv"
        if flattened_entries:
            with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=flattened_entries[0].keys())
                writer.writeheader()
                writer.writerows(flattened_entries)
        
        xlsx_file = self.output_dir / f"{self.dialect_code}_{timestamp}.xlsx"
        df = pd.DataFrame(flattened_entries)
        df.to_excel(xlsx_file, index=False, engine='openpyxl')
        
        stats_file = self.output_dir / f"{self.dialect_code}_{timestamp}_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2)
        
        print("Scraping complete!")
        print(f"Dialect: {self.stats['dialect_name']} ({self.dialect_code})")
        print(f"Total entries: {self.stats['total_entries']}")
        print(f"Total queries: {self.stats['total_queries']}")
        print(f"Failed queries: {self.stats['failed_queries']}")
        
        start = datetime.fromisoformat(self.stats['start_time'])
        end = datetime.fromisoformat(self.stats['end_time'])
        duration = end - start
        print(f"Duration: {duration}")
        
        print(f"\nFiles saved:")
        print(f"  JSON: {json_file}")
        print(f"  CSV:  {csv_file}")
        print(f"  XLSX: {xlsx_file}")
        print(f"  Stats: {stats_file}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("RomLex Dictionary Scraper")
        print("\nUsage: python romlex_scraper.py <dialect_code>")
        print("\nAvailable dialects:")
        for code, name in sorted(DIALECT_NAMES.items()):
            print(f"  {code} - {name}")
        print("\nExample: python romlex_scraper.py rmyb")
        sys.exit(1)
    
    dialect = sys.argv[1].lower()
    
    if dialect not in DIALECT_NAMES:
        print(f"Error: Unknown dialect code '{dialect}'")
        print(f"Run without arguments to see available dialects.")
        sys.exit(1)
    
    print(f"Dialect: {DIALECT_NAMES[dialect]} ({dialect})")
    confirm = input("Start scraping? [y/N]: ")
    
    if confirm.lower() != 'y':
        print("Cancelled.")
        sys.exit(0)
    
    scraper = RomLexScraper(dialect)
    scraper.scrape_full_dialect()