import requests
from bs4 import BeautifulSoup, Tag
import json
import time
from urllib.parse import urljoin
from typing import Set, Dict, List, Optional
import re
import hashlib
import os

class StarLingGeneralScraper:
    def __init__(self, start_url: str, total_pages: int, start_page: int, checkpoint_file: str = 'scraper_checkpoint.json'):
        self.start_url = start_url
        self.total_pages = total_pages
        self.checkpoint_file = checkpoint_file
        
        self.base_url = "https://starlingdb.org"
        self.seen_content: Set[str] = set()  # Track content hashes (Notes field)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': 'https://starlingdb.org/cgi-bin/main.cgi'
        })
        
        self.all_records: List[Dict] = []
        self.current_page: int = start_page
        self.failed_urls: List[Dict] = []
        
        # Load checkpoint if exists
        self.load_checkpoint()
    
    def create_content_hash(self, entry_data: Dict) -> Optional[str]:
        """Create hash from the first proto-language field + its value"""
        # Look for the primary proto-language field (usually the first non-metadata field)
        for key, value in entry_data.items():
            # Skip metadata fields and secondary fields
            if key.startswith('_') or key in ['Meaning', 'Notes', 'Number in DED']:
                continue
            
            # First substantial field is typically the proto-language + word
            if value and value.strip():
                content = f"{key.strip()}:{value.strip()}"
                return hashlib.md5(content.encode('utf-8')).hexdigest()
        
        return None
    
    def extract_field_data(self, div) -> tuple:
        """Extract field name and value from a div - completely generic"""
        field_span = div.find('span', class_='fld')
        if field_span:
            field_name = field_span.get_text(strip=True).rstrip(':').strip()
            value_span = div.find('span', class_='unicode')
            if value_span:
                value = value_span.get_text(strip=True)
            else:
                # Get all text after field name
                value = div.get_text(strip=True).replace(field_name, '', 1).strip()
            return field_name, value
        return None, None
    
    def fetch_with_retry(self, url: str, max_retries: int = 3, timeout: int = 45) -> Optional[requests.Response]:
        """Fetch URL with exponential backoff retry"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                wait_time = (2 ** attempt) * 2
                print(f"      Timeout (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s...")
                time.sleep(wait_time)
            except requests.exceptions.RequestException as e:
                wait_time = (2 ** attempt) * 2
                print(f"      Error (attempt {attempt + 1}/{max_retries}): {str(e)[:50]}, waiting {wait_time}s...")
                time.sleep(wait_time)
        
        print(f"Failed after {max_retries} attempts")
        self.failed_urls.append({
        'url': url,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'error': 'Failed after max retries'
        })
        return None
    
    def save_failed_urls(self):
        """Save failed URLs to CSV"""
        if self.failed_urls:
            import csv
            with open('failed_urls.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['timestamp', 'url', 'error'])
                writer.writeheader()
                writer.writerows(self.failed_urls)
            print(f"\n {len(self.failed_urls)} failed URLs saved to failed_urls.csv")
            
    def scrape_sub_entry(self, url: str, depth: int = 0) -> Dict:
        """Recursively scrape a sub-entry following ALL links until content is seen again"""
        print(f"{'  ' * depth}Depth {depth}")
        
        try:
            response = self.fetch_with_retry(url)
            if not response:
                return {"_error": "fetch_failed", "_url": url, "_depth": depth}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            entry_data = {"_url": url, "_depth": depth}
            
            record = soup.find('div', class_='results_record')
            
                
            if not record:
                return entry_data
            
            # Extract ALL field-value pairs generically
            if isinstance(record, Tag):
                for div in record.find_all('div', recursive=False):
                    if not isinstance(div, Tag):
                        continue
                    field_name, value = self.extract_field_data(div)
                    if field_name:
                        entry_data[field_name] = value
                        
                     
            
            # Check if we've seen this content before (based on Notes)
            content_hash = self.create_content_hash(entry_data)
            

            if content_hash and content_hash in self.seen_content:
                print(f"{'  ' * depth}Seen before (circular reference)")
                return {"_circular_reference": True, "_url": url, "_content_hash": content_hash}

            # Mark as seen if it has the same headword
            if content_hash:
                self.seen_content.add(content_hash)
                entry_data["_content_hash"] = content_hash
            
            if isinstance(record, Tag):
                for div in record.find_all('div', recursive=False):
                    if not isinstance(div, Tag):
                        continue
                    field_name, _ = self.extract_field_data(div)
                    if field_name:
                        # ONLY follow links that have a subquery_link (+ icon)
                        if 'etymology' in field_name.lower():
                            continue
                        subquery_link = div.find('div', class_='subquery_link')
                        if subquery_link and isinstance(subquery_link, Tag):
                            if subquery_link:
                                plus_img = subquery_link.find('img', onclick=True)
                                if plus_img and isinstance(plus_img, Tag):
                                    onclick = plus_img.get('onclick', '')
                                    match = re.search(r"'([^']+)'", str(onclick))
                                    if match:
                                        sub_url = urljoin(self.base_url + '/cgi-bin/', match.group(1))
                                        sub_data = self.scrape_sub_entry(sub_url, depth + 1)
                                    
                                    # Only add if not circular
                                    if not sub_data.get("_circular_reference"):
                                        if "_sub_entries" not in entry_data:
                                            entry_data["_sub_entries"] = []
                                        entry_data["_sub_entries"].append(sub_data)
                
            # Polite delay
            time.sleep(0.8 + (depth * 0.1))
            return entry_data
            
        except Exception as e:
            print(f"{'  ' * depth}Error: {str(e)[:60]}")
            return {"_error": str(e), "_url": url, "_depth": depth}
    
    def scrape_record(self, record_div, page_num: int, record_num: int) -> Dict:
        """Scrape a single results_record from the main page"""
        print(f"\n[Page {page_num}, Record {record_num}]")
        
        record_data = {
            "_page": page_num,
            "_record_num": record_num,
            "_sub_entries": []
        }
        
        # Extract ALL main fields generically
        for div in record_div.find_all('div', recursive=False):
            field_name, value = self.extract_field_data(div)
            if field_name:
                record_data[field_name] = value
        
        # Create content hash for main record
        content_hash = self.create_content_hash(record_data)
        if content_hash:
            record_data["_content_hash"] = content_hash
            self.seen_content.add(content_hash)
        
        # Now scrape ALL sub-entries (+ icons)
        for div in record_div.find_all('div', recursive=False):
            field_name, _ = self.extract_field_data(div)
            if field_name:
                if 'etymology' in field_name.lower():
                    continue
                subquery_link = div.find('div', class_='subquery_link')
                if subquery_link:
                    plus_img = subquery_link.find('img', onclick=True)
                    if plus_img:
                        onclick = plus_img.get('onclick', '')
                        match = re.search(r"'([^']+)'", onclick)
                        if match:
                            sub_url = urljoin(self.base_url + '/cgi-bin/', match.group(1))
                            sub_data = self.scrape_sub_entry(sub_url, depth=1)
                            if not sub_data.get("_circular_reference"):
                                record_data["_sub_entries"].append(sub_data)
        
        return record_data
    
    def scrape_page(self, page_num: int) -> List[Dict]:
        """Scrape a single page of results"""
        # Generate URL based on page number
        if page_num == 1:
            url = self.start_url
        else:
            first = 1 + (page_num - 1) * 20
            # Add first parameter to base URL
            if '?' in self.start_url:
                url = f"{self.start_url}&first={first}"
            else:
                url = f"{self.start_url}?first={first}"
        
        print(f"\n{'='*70}")
        print(f"PAGE {page_num}/{self.total_pages}")
        print(f"{'='*70}")
        
        try:
            response = self.fetch_with_retry(url, timeout=45)
            if not response:
                print(f"Failed to fetch page {page_num}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            records = soup.find_all('div', class_='results_record')
            print(f"Found {len(records)} records")
            
            page_data = []
            
            for i, record in enumerate(records, 1):
                try:
                    record_data = self.scrape_record(record, page_num, i)
                    page_data.append(record_data)
                except Exception as e:
                    print(f"Error on record {i}: {e}")
            
            time.sleep(4)  # Longer delay between pages
            return page_data
            
        except Exception as e:
            print(f"Error scraping page {page_num}: {e}")
            return []
    
    def save_checkpoint(self):
        """Save checkpoint for resume capability"""
        checkpoint = {
            'start_url': self.start_url,
            'total_pages': self.total_pages,
            'current_page': self.current_page,
            'seen_content_hashes': list(self.seen_content),
            'records_scraped': len(self.all_records)
        }
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    
    def load_checkpoint(self):
        """Load checkpoint if exists"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                
                # Verify checkpoint matches current job
                if checkpoint.get('start_url') == self.start_url:
                    self.current_page = checkpoint.get('current_page', 1)
                    self.seen_content = set(checkpoint.get('seen_content_hashes', []))
                    print(f"Resuming from page {self.current_page}")
                    print(f"Already seen: {len(self.seen_content)} unique entries")
                    
                    # Load existing data
                    if os.path.exists('starling_complete_data.json'):
                        with open('starling_complete_data.json', 'r', encoding='utf-8') as f:
                            existing = json.load(f)
                            self.all_records = existing.get('records', [])
            except Exception as e:
                print(f"Could not load checkpoint: {e}")
    
    def save_results(self):
        """Save all results to JSON"""
        output = {
            'metadata': {
                'start_url': self.start_url,
                'total_pages_scraped': len([r for r in self.all_records if r]), 
                'total_records': len(self.all_records),
                'unique_entries': len(self.seen_content)
            },
            'records': self.all_records
        }
        
        with open('starling_complete_data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        print(f"\n Saved to starling_complete_data.json")
    
    def scrape_all(self):
        """Scrape all pages with checkpoint support"""
        print(f"Starting scrape from: {self.start_url}")
        print(f"Total pages: {self.total_pages}")
        print(f"Records per page: 20 (assumed)")
        print(f"Deduplication: Notes field content\n")
        
        for page in range(self.current_page, self.total_pages + 1):
            self.current_page = page
            page_records = self.scrape_page(page)
            self.all_records.extend(page_records)
            
            # Save after each page
            self.save_results()
            self.save_checkpoint()
            
            print(f"\n Progress: {len(self.all_records)} total records, {len(self.seen_content)} unique entries")
        
        self.save_failed_urls()
        
        # Clean up checkpoint on completion
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
        
        print(f"\n{'='*70}")
        print(f"COMPLETE")
        print(f"{'='*70}")


if __name__ == "__main__":
    # Just change these two variables for any StarLing database
    START_URL = r"https://starlingdb.org/cgi-bin/response.cgi?root=config&basename=\data\drav\dravet"
    TOTAL_PAGES = 111
    START_PAGE = 21
    scraper = StarLingGeneralScraper(START_URL, TOTAL_PAGES, START_PAGE)
    scraper.scrape_all()