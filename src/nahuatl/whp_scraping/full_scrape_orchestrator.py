"""
Full Dictionary Scrape Orchestrator
Handles large-scale scraping with checkpoints and recovery
"""

import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

from node_entry_scraper import NodeEntryScraper
from csv_exporter import CSVExporter


class FullScrapeOrchestrator:
    """Orchestrate full dictionary scrape with checkpointing"""
    
    def __init__(
        self,
        inventory_path: str = "data/interim/node_inventory.csv",
        output_dir: str = "data/interim/scraped",
        checkpoint_interval: int = 500
    ):
        self.inventory_path = inventory_path
        self.output_dir = Path(output_dir)
        self.checkpoint_interval = checkpoint_interval
        
        self.checkpoint_file = self.output_dir / "scrape_checkpoint.txt"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.scraper = NodeEntryScraper(delay_seconds=0.5)
        self.exporter = CSVExporter(output_dir=str(self.output_dir))
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'scrape.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def load_checkpoint(self) -> int:
        """Load last completed index"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return 0
    
    def save_checkpoint(self, index: int):
        """Save progress checkpoint"""
        with open(self.checkpoint_file, 'w') as f:
            f.write(str(index))
    
    def run_full_scrape(self, resume: bool = True):
        """
        Execute full dictionary scrape with checkpointing
        
        Args:
            resume: If True, resume from last checkpoint
        """
        start_index = self.load_checkpoint() if resume else 0
        
        # Count total entries
        import pandas as pd
        df = pd.read_csv(self.inventory_path)
        total_entries = len(df)
        
        self.logger.info("=" * 70)
        self.logger.info("FULL DICTIONARY SCRAPE")
        self.logger.info("=" * 70)
        self.logger.info(f"Total entries: {total_entries}")
        self.logger.info(f"Starting from index: {start_index}")
        self.logger.info(f"Remaining: {total_entries - start_index}")
        self.logger.info(f"Checkpoint interval: {self.checkpoint_interval}")
        self.logger.info(f"Estimated time: {(total_entries - start_index) * 0.5 / 3600:.1f} hours")
        self.logger.info("=" * 70)
        
        # Initialize CSV files on first run
        initialize = (start_index == 0)
        if initialize:
            self.exporter._initialize_csv_files()
        
        # Process in batches
        current_index = start_index
        
        while current_index < total_entries:
            batch_end = min(current_index + self.checkpoint_interval, total_entries)
            
            self.logger.info(f"\nProcessing batch: {current_index}-{batch_end}")
            
            # Scrape batch
            scraped_data = self.scraper.scrape_nodes_from_inventory(
                inventory_path=self.inventory_path,
                start_index=current_index,
                end_index=batch_end
            )
            
            # Export batch
            self.exporter.export_batch(scraped_data, initialize=False)
            
            # Save checkpoint
            self.save_checkpoint(batch_end)
            
            # Progress update
            progress = (batch_end / total_entries) * 100
            self.logger.info(f"Progress: {batch_end}/{total_entries} ({progress:.1f}%)")
            self.logger.info(f"Checkpoint saved at index {batch_end}")
            
            current_index = batch_end
        
        # Final steps
        self.exporter.save_tracking_files()
        self.exporter.print_statistics()
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("SCRAPE COMPLETE")
        self.logger.info("=" * 70)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Full dictionary scrape')
    parser.add_argument(
        '--inventory',
        default='data/interim/node_inventory.csv',
        help='Path to node inventory'
    )
    parser.add_argument(
        '--output-dir',
        default='data/interim/scraped',
        help='Output directory'
    )
    parser.add_argument(
        '--checkpoint-interval',
        type=int,
        default=500,
        help='Save checkpoint every N entries (default: 500)'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start from beginning (ignore checkpoint)'
    )
    
    args = parser.parse_args()
    
    orchestrator = FullScrapeOrchestrator(
        inventory_path=args.inventory,
        output_dir=args.output_dir,
        checkpoint_interval=args.checkpoint_interval
    )
    
    orchestrator.run_full_scrape(resume=not args.no_resume)


if __name__ == "__main__":
    main()