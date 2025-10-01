"""
CSV Exporter for Scraped Nahuatl Dictionary Data
Converts ScrapedNodeData objects to CSV files for SQLite import
"""

import csv
import logging
from typing import List, Set, Dict, Optional
from pathlib import Path
from dataclasses import asdict
from datetime import datetime

from node_entry_scraper import (
    ScrapedNodeData,
    DictionaryEntryData,
    AuthorityCitation,
    Attestation,
    ThemeReference,
    AudioReference
)


class CSVExporter:
    """Export scraped data to CSV files"""
    
    def __init__(self, output_dir: str = "data/interim/scraped"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = self._setup_logger()
        
        # Track unique values for later scraping
        self.unique_audio_nodes: Set[int] = set()
        self.unique_theme_slugs: Set[str] = set()
        
        # CSV file paths
        self.files = {
            'entries': self.output_dir / 'dictionary_entries.csv',
            'citations': self.output_dir / 'authority_citations.csv',
            'attestations': self.output_dir / 'attestations.csv',
            'themes': self.output_dir / 'entry_themes.csv',
            'audio': self.output_dir / 'entry_audio.csv',
        }
        
        # Statistics
        self.stats = {
            'total_entries': 0,
            'successful': 0,
            'not_found': 0,
            'errors': 0,
            'total_citations': 0,
            'total_attestations': 0,
            'total_themes': 0,
            'total_audio_refs': 0,
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _initialize_csv_files(self):
        """Create CSV files with headers"""
        
        # Dictionary entries
        with open(self.files['entries'], 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'headword', 'orthographic_variants', 'ipa_spelling',
                'translation_english', 'spanish_loanword', 'headword_idiez',
                'translation_english_idiez', 'definition_nahuatl_idiez',
                'definition_spanish_idiez', 'morfologia_idiez', 'gramatica_idiez',
                'source_dataset', 'url_alias', 'created_timestamp', 'scrape_timestamp'
            ])
            writer.writeheader()
        
        # Authority citations
        with open(self.files['citations'], 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'authority_name', 'citation_text', 'citation_order'
            ])
            writer.writeheader()
        
        # Attestations
        with open(self.files['attestations'], 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'language', 'attestation_text', 'source_field'
            ])
            writer.writeheader()
        
        # Entry themes
        with open(self.files['themes'], 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'entry_node_id', 'theme_slug', 'theme_name', 'delta'
            ])
            writer.writeheader()
        
        # Entry audio
        with open(self.files['audio'], 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'entry_node_id', 'audio_node_id', 'reference_type', 'delta'
            ])
            writer.writeheader()
        
        self.logger.info(f"Initialized CSV files in {self.output_dir}")
    
    def _append_entry(self, entry_data: DictionaryEntryData):
        """Append dictionary entry to CSV"""
        with open(self.files['entries'], 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'headword', 'orthographic_variants', 'ipa_spelling',
                'translation_english', 'spanish_loanword', 'headword_idiez',
                'translation_english_idiez', 'definition_nahuatl_idiez',
                'definition_spanish_idiez', 'morfologia_idiez', 'gramatica_idiez',
                'source_dataset', 'url_alias', 'created_timestamp', 'scrape_timestamp'
            ])
            writer.writerow(asdict(entry_data))
    
    def _append_citations(self, citations: List[AuthorityCitation]):
        """Append authority citations to CSV"""
        if not citations:
            return
        
        with open(self.files['citations'], 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'authority_name', 'citation_text', 'citation_order'
            ])
            for citation in citations:
                writer.writerow(asdict(citation))
    
    def _append_attestations(self, attestations: List[Attestation]):
        """Append attestations to CSV"""
        if not attestations:
            return
        
        with open(self.files['attestations'], 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'node_id', 'language', 'attestation_text', 'source_field'
            ])
            for attestation in attestations:
                writer.writerow(asdict(attestation))
    
    def _append_themes(self, themes: List[ThemeReference]):
        """Append theme references to CSV"""
        if not themes:
            return
        
        with open(self.files['themes'], 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'entry_node_id', 'theme_slug', 'theme_name', 'delta'
            ])
            for theme in themes:
                writer.writerow(asdict(theme))
                self.unique_theme_slugs.add(theme.theme_slug)
    
    def _append_audio_refs(self, audio_refs: List[AudioReference]):
        """Append audio references to CSV"""
        if not audio_refs:
            return
        
        with open(self.files['audio'], 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'entry_node_id', 'audio_node_id', 'reference_type', 'delta'
            ])
            for audio_ref in audio_refs:
                writer.writerow(asdict(audio_ref))
                self.unique_audio_nodes.add(audio_ref.audio_node_id)
    
    def export_batch(
        self,
        scraped_data: List[ScrapedNodeData],
        initialize: bool = True
    ):
        """
        Export a batch of scraped data to CSV files
        
        Args:
            scraped_data: List of ScrapedNodeData objects
            initialize: If True, create new CSV files with headers
        """
        if initialize:
            self._initialize_csv_files()
        
        for data in scraped_data:
            self.stats['total_entries'] += 1
            
            # Update status statistics
            if data.scrape_status == 'success':
                self.stats['successful'] += 1
            elif data.scrape_status == 'not_found':
                self.stats['not_found'] += 1
            else:
                self.stats['errors'] += 1
            
            # Only export successful scrapes
            if data.scrape_status != 'success':
                continue
            
            # Export main entry
            self._append_entry(data.entry)
            
            # Export citations
            self._append_citations(data.citations)
            self.stats['total_citations'] += len(data.citations)
            
            # Export attestations
            self._append_attestations(data.attestations)
            self.stats['total_attestations'] += len(data.attestations)
            
            # Export themes
            self._append_themes(data.themes)
            self.stats['total_themes'] += len(data.themes)
            
            # Export audio references
            self._append_audio_refs(data.audio_refs)
            self.stats['total_audio_refs'] += len(data.audio_refs)
    
    def save_tracking_files(self):
        """Save lists of unique audio nodes and themes for later scraping"""
        
        # Save unique audio node IDs
        audio_file = self.output_dir / 'audio_nodes_to_scrape.txt'
        with open(audio_file, 'w', encoding='utf-8-sig') as f:
            f.write(f"# Audio node IDs to scrape - {datetime.now()}\n")
            f.write(f"# Total: {len(self.unique_audio_nodes)}\n\n")
            for node_id in sorted(self.unique_audio_nodes):
                f.write(f"{node_id}\n")
        
        self.logger.info(f"Saved {len(self.unique_audio_nodes)} audio node IDs to {audio_file}")
        
        # Save unique theme slugs
        theme_file = self.output_dir / 'theme_slugs_to_scrape.txt'
        with open(theme_file, 'w', encoding='utf-8-sig') as f:
            f.write(f"# Theme slugs to scrape - {datetime.now()}\n")
            f.write(f"# Total: {len(self.unique_theme_slugs)}\n\n")
            for slug in sorted(self.unique_theme_slugs):
                f.write(f"{slug}\n")
        
        self.logger.info(f"Saved {len(self.unique_theme_slugs)} theme slugs to {theme_file}")
    
    def print_statistics(self):
        """Print export statistics"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("EXPORT STATISTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Total entries processed: {self.stats['total_entries']}")
        self.logger.info(f"Successful: {self.stats['successful']}")
        self.logger.info(f"Not found: {self.stats['not_found']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info(f"\nData exported:")
        self.logger.info(f"  Citations: {self.stats['total_citations']}")
        self.logger.info(f"  Attestations: {self.stats['total_attestations']}")
        self.logger.info(f"  Theme references: {self.stats['total_themes']}")
        self.logger.info(f"  Audio references: {self.stats['total_audio_refs']}")
        self.logger.info(f"\nUnique values for later scraping:")
        self.logger.info(f"  Audio nodes: {len(self.unique_audio_nodes)}")
        self.logger.info(f"  Theme slugs: {len(self.unique_theme_slugs)}")
        self.logger.info(f"\nFiles saved to: {self.output_dir}")


def main():
    """Test the exporter"""
    import argparse
    from node_entry_scraper import NodeEntryScraper
    
    parser = argparse.ArgumentParser(description='Export scraped data to CSV')
    parser.add_argument(
        '--inventory',
        default='data/interim/node_inventory.csv',
        help='Path to node inventory'
    )
    parser.add_argument(
        '--end',
        type=int,
        default=10,
        help='Number of entries to scrape and export (default: 10)'
    )
    parser.add_argument(
        '--output-dir',
        default='data/interim/scraped',
        help='Output directory for CSV files'
    )
    
    args = parser.parse_args()
    
    # Scrape some entries
    print(f"Scraping first {args.end} entries...")
    scraper = NodeEntryScraper()
    scraped_data = scraper.scrape_nodes_from_inventory(
        inventory_path=args.inventory,
        start_index=0,
        end_index=args.end
    )
    
    # Export to CSV
    print(f"\nExporting to CSV files...")
    exporter = CSVExporter(output_dir=args.output_dir)
    exporter.export_batch(scraped_data, initialize=True)
    exporter.save_tracking_files()
    exporter.print_statistics()


if __name__ == "__main__":
    main()