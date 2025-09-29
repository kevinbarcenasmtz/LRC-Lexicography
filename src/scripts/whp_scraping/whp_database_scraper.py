"""
Integration script to scrape WHP entries from database headwords
Connects SQLite database with WHPScraper
"""

import sqlite3
import sys
from pathlib import Path
import pandas as pd
from typing import List, Optional, Dict
import logging

# import the scraper from the same directory
from whp_scraper import WHPScraper, ScrapingConfig, WHPEntry


class DatabaseScrapeOrchestrator:
    """Orchestrates scraping from database headwords"""
    
    def __init__(self, db_path: str, scraper_config: Optional[ScrapingConfig] = None):
        """
        Initialize orchestrator
        
        Args:
            db_path: Path to SQLite database
            scraper_config: Optional scraper configuration
        """
        self.db_path = db_path
        self.scraper = WHPScraper(config=scraper_config)
        self.logger = logging.getLogger(__name__)
        
    def load_headwords_from_db(
        self, 
        table_name: str = 'checkpoint_removed_empty_p_tags_20250929',
        limit: Optional[int] = None,
        where_clause: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load headwords from database
        
        Args:
            table_name: Database table name
            limit: Optional limit on number of rows
            where_clause: Optional SQL WHERE clause for filtering
            
        Returns:
            DataFrame with Ref and Headword columns
        """
        query = f"SELECT Ref, Headword FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        if limit:
            query += f" LIMIT {limit}"
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn)
            
        self.logger.info(f"Loaded {len(df)} headwords from database")
        return df
    
    def find_missing_entries(
        self,
        scraped_entries: List[WHPEntry]
    ) -> Dict[str, List[str]]:
        """
        Analyze scraping results to find gaps
        
        Returns:
            Dictionary with 'not_found', 'errors', and 'successful' headwords
        """
        results = {
            'not_found': [],
            'errors': [],
            'successful': []
        }
        
        for entry in scraped_entries:
            if entry.scrape_status == 'not_found':
                results['not_found'].append(entry.headword)
            elif entry.scrape_status == 'error':
                results['errors'].append(entry.headword)
            else:
                results['successful'].append(entry.headword)
        
        return results
    
    def scrape_from_database(
        self,
        table_name: str = 'checkpoint_removed_empty_p_tags_20250929',
        limit: Optional[int] = None,
        sample_size: Optional[int] = None,
        where_clause: Optional[str] = None
    ) -> List[WHPEntry]:
        """
        Main workflow: Load headwords from DB and scrape them
        
        Args:
            table_name: Database table to load from
            limit: Maximum number of entries to process
            sample_size: If set, randomly sample this many entries
            where_clause: SQL WHERE clause for filtering
            
        Returns:
            List of scraped entries
        """
        # Load headwords from database
        df = self.load_headwords_from_db(table_name, limit, where_clause)
        
        # Optional: Random sample for testing
        if sample_size and sample_size < len(df):
            df = df.sample(n=sample_size, random_state=42)
            self.logger.info(f"Sampled {sample_size} random entries")
        
        # Extract headwords (remove periods and clean)
        headwords = df['Headword'].str.rstrip('.').tolist()
        
        # Scrape entries
        entries = self.scraper.scrape_entries(headwords)
        
        return entries
    
    def run_gap_analysis(
        self,
        table_name: str = 'checkpoint_removed_empty_p_tags_20250929',
        limit: Optional[int] = None,
        sample_size: Optional[int] = None,
        save_results: bool = True
    ) -> Dict:
        """
        Full workflow: Scrape and analyze gaps
        
        Returns:
            Dictionary with results and statistics
        """
        self.logger.info("=" * 70)
        self.logger.info("WHP DATABASE SCRAPING - GAP ANALYSIS")
        self.logger.info("=" * 70)
        
        # Scrape entries
        entries = self.scrape_from_database(
            table_name=table_name,
            limit=limit,
            sample_size=sample_size
        )
        
        # Analyze results
        analysis = self.find_missing_entries(entries)
        
        # Print summary
        self.logger.info("\n" + "=" * 70)
        self.logger.info("SCRAPING RESULTS SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Total entries processed: {len(entries)}")
        self.logger.info(f"Successful: {len(analysis['successful'])} ({len(analysis['successful'])/len(entries)*100:.1f}%)")
        self.logger.info(f"Not found on website: {len(analysis['not_found'])} ({len(analysis['not_found'])/len(entries)*100:.1f}%)")
        self.logger.info(f"Errors: {len(analysis['errors'])} ({len(analysis['errors'])/len(entries)*100:.1f}%)")
        
        # Save results
        if save_results:
            results_file = self.scraper.save_results(entries)
            not_found_file = self.scraper.save_not_found_entries(entries)
            self.logger.info(f"\nResults saved to: {results_file}")
            if not_found_file:
                self.logger.info(f"Not-found list saved to: {not_found_file}")
        
        return {
            'entries': entries,
            'analysis': analysis,
            'statistics': {
                'total': len(entries),
                'successful': len(analysis['successful']),
                'not_found': len(analysis['not_found']),
                'errors': len(analysis['errors'])
            }
        }


def main():
    """Example usage with command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape WHP entries from database')
    parser.add_argument(
        '--db-path', 
        default='../../../data/sqLiteDb/nahuatl_processing.db',
        help='Path to SQLite database'
    )
    parser.add_argument(
        '--table', 
        default='checkpoint_removed_empty_p_tags_20250929',
        help='Database table name'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Limit number of entries to scrape'
    )
    parser.add_argument(
        '--sample', 
        type=int, 
        help='Random sample size for testing'
    )
    parser.add_argument(
        '--test', 
        action='store_true', 
        help='Test mode: scrape only 10 entries'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.5,
        help='Delay between requests in seconds (default: 1.5)'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test mode: just 10 entries
    if args.test:
        args.sample = 10
        logging.info("TEST MODE: Scraping only 10 random entries")
    
    # Run orchestrator
    orchestrator = DatabaseScrapeOrchestrator(
        db_path=args.db_path,
        scraper_config=ScrapingConfig(delay_seconds=args.delay)
    )
    
    results = orchestrator.run_gap_analysis(
        table_name=args.table,
        limit=args.limit,
        sample_size=args.sample,
        save_results=True
    )
    
    # Print some not-found entries as examples
    if results['analysis']['not_found']:
        print("\n" + "=" * 70)
        print("SAMPLE OF NOT-FOUND ENTRIES:")
        print("=" * 70)
        for headword in results['analysis']['not_found'][:10]:
            print(f"  - {headword}")
        if len(results['analysis']['not_found']) > 10:
            print(f"  ... and {len(results['analysis']['not_found']) - 10} more")


if __name__ == "__main__":
    main()