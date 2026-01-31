"""
Dictionary Enumerator for Nahuatl WHP Website
Scrapes /dictionary pagination to build master node inventory
"""

import requests
import time
import logging
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import csv


@dataclass
class NodeInventoryEntry:
    """Single node from dictionary table"""
    node_id: Optional[int]
    url_alias: str
    title: str
    wordorparticle: str
    head_idiez: str
    has_translation1: bool
    has_ndef_idiez: bool
    has_eshort_idiez: bool
    source_dataset: str  # 'WHP', 'IDIEZ', or 'HYBRID'
    page_number: int
    scrape_timestamp: str


class DictionaryEnumerator:
    """Enumerate all dictionary entries from paginated table view"""
    
    def __init__(
        self,
        base_url: str = "https://nahuatl.wired-humanities.org/dictionary",
        delay_seconds: float = 0.5,
        output_dir: str = "data/interim"
    ):
        self.base_url = base_url
        self.delay_seconds = delay_seconds
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = self._setup_session()
        self.logger = self._setup_logger()
        
        # Progress tracking
        self.checkpoint_file = self.output_dir / "enumeration_checkpoint.txt"
        
    def _setup_session(self) -> requests.Session:
        """Configure requests session"""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; NahuatLEX-Enumerator/1.0)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.5",
        })
        return session
    
    def _setup_logger(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _parse_node_id_from_url(self, url: str) -> Optional[int]:
        """Extract node ID from /node/XXXXX URL"""
        if not url:
            return None
        if url.startswith('/node/'):
            try:
                return int(url.replace('/node/', ''))
            except ValueError:
                return None
        return None
    
    def _classify_source_dataset(
        self,
        has_wordorparticle: bool,
        has_head_idiez: bool
    ) -> str:
        """Determine if entry is WHP, IDIEZ, or HYBRID"""
        if has_wordorparticle and has_head_idiez:
            return 'HYBRID'
        elif has_head_idiez:
            return 'IDIEZ'
        elif has_wordorparticle:
            return 'WHP'
        else:
            return 'UNKNOWN'
    
    def parse_dictionary_page(self, page_num: int) -> List[NodeInventoryEntry]:
        """Parse a single page of the dictionary table"""
        url = f"{self.base_url}?page={page_num}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            entries = []
            
            # Find all table rows (skip header)
            rows = soup.select('table.views-table tbody tr')
            
            if not rows:
                self.logger.warning(f"No rows found on page {page_num}")
                return []
            
            for row in rows:
                # Extract title cell (contains node link)
                title_cell = row.select_one('td.views-field-title a')
                if not title_cell:
                    continue
                
                node_url = str(title_cell.get("href") or "")
                node_id = self._parse_node_id_from_url(node_url)
                title = title_cell.text.strip()
                
                # Extract URL alias (might be /content/... or /node/...)
                url_alias = node_url if node_url.startswith('/content/') else ''
                
                # Extract field values from other columns
                wordorparticle_cell = row.select_one('td.views-field-field-wordorparticle')
                head_idiez_cell = row.select_one('td.views-field-field-head-idiez')
                translation1_cell = row.select_one('td.views-field-field-translation1')
                ndef_idiez_cell = row.select_one('td.views-field-field-ndef-idiez')
                eshort_idiez_cell = row.select_one('td.views-field-field-eshort-idiez')
                
                # Get text values
                wordorparticle = wordorparticle_cell.text.strip() if wordorparticle_cell else ''
                head_idiez = head_idiez_cell.text.strip() if head_idiez_cell else ''
                
                # Check if fields have content (not just whitespace)
                has_translation1 = bool(translation1_cell and translation1_cell.text.strip())
                has_ndef_idiez = bool(ndef_idiez_cell and ndef_idiez_cell.text.strip())
                has_eshort_idiez = bool(eshort_idiez_cell and eshort_idiez_cell.text.strip())
                
                # Classify source dataset
                source_dataset = self._classify_source_dataset(
                    bool(wordorparticle),
                    bool(head_idiez)
                )
                
                entry = NodeInventoryEntry(
                    node_id=node_id,
                    url_alias=url_alias,
                    title=title,
                    wordorparticle=wordorparticle,
                    head_idiez=head_idiez,
                    has_translation1=has_translation1,
                    has_ndef_idiez=has_ndef_idiez,
                    has_eshort_idiez=has_eshort_idiez,
                    source_dataset=source_dataset,
                    page_number=page_num,
                    scrape_timestamp=datetime.now().isoformat()
                )
                
                entries.append(entry)
            
            self.logger.info(f"Page {page_num}: Extracted {len(entries)} entries")
            return entries
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch page {page_num}: {e}")
            return []
    
    def save_checkpoint(self, page_num: int):
        """Save progress checkpoint"""
        with open(self.checkpoint_file, 'w') as f:
            f.write(str(page_num))
    
    def load_checkpoint(self) -> int:
        """Load last completed page number"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return -1
    
    def enumerate_all_entries(
        self,
        start_page: int = 0,
        end_page: int = 771,
        resume: bool = True
    ) -> List[NodeInventoryEntry]:
        """
        Enumerate all dictionary entries across all pages
        
        Args:
            start_page: First page to scrape (0-indexed)
            end_page: Last page to scrape (inclusive)
            resume: If True, resume from checkpoint
        """
        all_entries = []
        
        # Resume from checkpoint if requested
        if resume:
            checkpoint_page = self.load_checkpoint()
            if checkpoint_page >= start_page:
                start_page = checkpoint_page + 1
                self.logger.info(f"Resuming from page {start_page}")
        
        self.logger.info(f"Starting enumeration: pages {start_page}-{end_page}")
        
        for page_num in range(start_page, end_page + 1):
            entries = self.parse_dictionary_page(page_num)
            all_entries.extend(entries)
            
            # Save checkpoint every 10 pages
            if (page_num + 1) % 10 == 0:
                self.save_checkpoint(page_num)
                self.logger.info(f"Checkpoint saved at page {page_num}")
            
            # Progress update every 50 pages
            if (page_num + 1) % 50 == 0:
                self.logger.info(
                    f"Progress: {page_num + 1}/{end_page + 1} pages "
                    f"({len(all_entries)} entries total)"
                )
            
            # Rate limiting
            if page_num < end_page:
                time.sleep(self.delay_seconds)
        
        # Final checkpoint
        self.save_checkpoint(end_page)
        
        return all_entries
    
    def save_inventory(
        self,
        entries: List[NodeInventoryEntry],
        filename: str = "node_inventory.csv"
    ) -> str:
        """Save node inventory to CSV"""
        filepath = self.output_dir / filename
        
        # Convert to DataFrame for easy CSV writing
        df = pd.DataFrame([asdict(entry) for entry in entries])
        
        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        self.logger.info(f"Saved {len(entries)} entries to {filepath}")
        
        # Print summary statistics
        self.logger.info("\n" + "=" * 70)
        self.logger.info("ENUMERATION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Total entries: {len(entries)}")
        self.logger.info(f"WHP entries: {len(df[df['source_dataset'] == 'WHP'])}")
        self.logger.info(f"IDIEZ entries: {len(df[df['source_dataset'] == 'IDIEZ'])}")
        self.logger.info(f"Hybrid entries: {len(df[df['source_dataset'] == 'HYBRID'])}")
        self.logger.info(f"Entries with URL aliases: {len(df[df['url_alias'] != ''])}")
        self.logger.info(f"Unique node IDs: {df['node_id'].nunique()}")
        
        return str(filepath)
    
    def generate_summary_report(self, entries: List[NodeInventoryEntry]) -> Dict:
        """Generate detailed summary statistics"""
        df = pd.DataFrame([asdict(entry) for entry in entries])
        
        return {
            'total_entries': len(entries),
            'unique_node_ids': df['node_id'].nunique(),
            'by_source': df['source_dataset'].value_counts().to_dict(),
            'with_url_alias': len(df[df['url_alias'] != '']),
            'with_translation1': df['has_translation1'].sum(),
            'with_ndef_idiez': df['has_ndef_idiez'].sum(),
            'with_eshort_idiez': df['has_eshort_idiez'].sum(),
            'pages_scraped': df['page_number'].nunique(),
            'duplicate_node_ids': len(df) - df['node_id'].nunique()
        }


def main():
    """Run the dictionary enumeration"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enumerate all dictionary entries from WHP website'
    )
    parser.add_argument(
        '--start-page',
        type=int,
        default=0,
        help='Starting page number (default: 0)'
    )
    parser.add_argument(
        '--end-page',
        type=int,
        default=771,
        help='Ending page number (default: 771)'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start from beginning, ignore checkpoint'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between requests in seconds (default: 0.5)'
    )
    parser.add_argument(
        '--output-dir',
        default='data/interim',
        help='Output directory for CSV files'
    )
    
    args = parser.parse_args()
    
    # Create enumerator
    enumerator = DictionaryEnumerator(
        delay_seconds=args.delay,
        output_dir=args.output_dir
    )
    
    # Run enumeration
    entries = enumerator.enumerate_all_entries(
        start_page=args.start_page,
        end_page=args.end_page,
        resume=not args.no_resume
    )
    
    # Save results
    filepath = enumerator.save_inventory(entries)
    
    # Generate summary
    summary = enumerator.generate_summary_report(entries)
    
    print("\n" + "=" * 70)
    print("DETAILED SUMMARY")
    print("=" * 70)
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    print(f"\nInventory saved to: {filepath}")


if __name__ == "__main__":
    main()