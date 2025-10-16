"""
Database Import Pipeline for Nahuatl Dictionary
Imports scraped CSV data into nahuatl.db following schema.sql
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import sys


class DatabaseImporter:
    """Import CSV data into SQLite database"""
    
    def __init__(
        self,
        db_path: str = "data/scraped_data/nahuatl.db",
        csv_dir: str = "data/interim/scraped",
        schema_path: str = "config/schema.sql",
        log_dir: str = "logs"
    ):
        self.db_path = Path(db_path)
        self.csv_dir = Path(csv_dir)
        self.schema_path = Path(schema_path)
        self.log_dir = Path(log_dir)
        
        # Create directories
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.logger = self._setup_logger()
        
        # Statistics tracking
        self.stats = {
            'themes': {'imported': 0, 'skipped': 0, 'errors': 0},
            'audio_files': {'imported': 0, 'skipped': 0, 'errors': 0},
            'dictionary_entries': {'imported': 0, 'skipped': 0, 'errors': 0},
            'attestations': {'imported': 0, 'skipped': 0, 'errors': 0},
            'authority_citations': {'imported': 0, 'skipped': 0, 'errors': 0},
            'entry_themes': {'imported': 0, 'skipped': 0, 'errors': 0},
            'entry_audio': {'imported': 0, 'skipped': 0, 'errors': 0}
        }
        
        # Connection (will be set during import)
        self.conn: Optional[sqlite3.Connection] = None
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging to file and console"""
        log_file = self.log_dir / f"db_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"Log file: {log_file}")
        
        return logger
    
    def create_database(self, fresh: bool = True) -> None:
        """
        Create database from schema.sql
        
        Args:
            fresh: If True, drop existing database and recreate
        """
        if fresh and self.db_path.exists():
            self.logger.info(f"Removing existing database: {self.db_path}")
            self.db_path.unlink()
        
        self.logger.info(f"Creating database: {self.db_path}")
        
        # Read schema
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        
        with open(self.schema_path, 'r', encoding='utf-8-sig') as f:
            schema_sql = f.read()
        
        # Create database and execute schema
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Execute schema (split by semicolon to handle multiple statements)
            conn.executescript(schema_sql)
            conn.commit()
            
            self.logger.info("Database schema created successfully")
            
            # Log table creation
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]
            self.logger.info(f"Tables created: {', '.join(tables)}")
            
        except sqlite3.Error as e:
            self.logger.error(f"Error creating database: {e}")
            raise
        finally:
            conn.close()
    
    def validate_csvs(self) -> bool:
        """Verify all required CSV files exist"""
        required_csvs = [
            'dictionary_entries.csv',
            'attestations.csv',
            'authority_citations.csv',
            'entry_themes.csv',
            'entry_audio.csv',
            'themes.csv',
            'audio_files.csv'
        ]
        
        self.logger.info("Validating CSV files...")
        
        missing = []
        for csv_file in required_csvs:
            csv_path = self.csv_dir / csv_file
            if not csv_path.exists():
                missing.append(csv_file)
                self.logger.error(f"Missing CSV: {csv_file}")
            else:
                # Check if file is empty
                df = pd.read_csv(csv_path, nrows=0)
                self.logger.info(f"{csv_file} - Columns: {list(df.columns)}")
        
        if missing:
            self.logger.error(f"Missing {len(missing)} CSV file(s)")
            return False
        
        self.logger.info("All CSV files validated successfully")
        return True
    
    def _validate_foreign_keys_themes(self) -> Tuple[bool, List[str]]:
        """Validate that all theme_slug values in entry_themes exist in themes"""
        entry_themes_df = pd.read_csv(self.csv_dir / 'entry_themes.csv')
        themes_df = pd.read_csv(self.csv_dir / 'themes.csv')
        
        # Get unique slugs from entry_themes
        entry_slugs = set(entry_themes_df['theme_slug'].unique())
        theme_slugs = set(themes_df['slug'].unique())
        
        # Find missing slugs
        missing_slugs = entry_slugs - theme_slugs
        
        if missing_slugs:
            return False, list(missing_slugs)
        
        return True, []
    
    def _validate_foreign_keys_entry_themes(self) -> Tuple[bool, List[int]]:
        """Validate that all entry_node_id in entry_themes exist in dictionary_entries"""
        entry_themes_df = pd.read_csv(self.csv_dir / 'entry_themes.csv')
        entries_df = pd.read_csv(self.csv_dir / 'dictionary_entries.csv')
        
        entry_ids = set(entry_themes_df['entry_node_id'].unique())
        valid_ids = set(entries_df['node_id'].unique())
        
        missing_ids = entry_ids - valid_ids
        
        if missing_ids:
            return False, list(missing_ids)
        
        return True, []
    
    def _validate_foreign_keys_entry_audio(self) -> Tuple[bool, Dict[str, List[int]]]:
        """Validate foreign keys for entry_audio table"""
        entry_audio_df = pd.read_csv(self.csv_dir / 'entry_audio.csv')
        entries_df = pd.read_csv(self.csv_dir / 'dictionary_entries.csv')
        audio_df = pd.read_csv(self.csv_dir / 'audio_files.csv')
        
        errors = {}
        
        # Check entry_node_id
        entry_ids = set(entry_audio_df['entry_node_id'].unique())
        valid_entry_ids = set(entries_df['node_id'].unique())
        missing_entry_ids = entry_ids - valid_entry_ids
        
        if missing_entry_ids:
            errors['missing_entry_ids'] = list(missing_entry_ids)
        
        # Check audio_node_id
        audio_ids = set(entry_audio_df['audio_node_id'].unique())
        valid_audio_ids = set(audio_df['node_id'].unique())
        missing_audio_ids = audio_ids - valid_audio_ids
        
        if missing_audio_ids:
            errors['missing_audio_ids'] = list(missing_audio_ids)
        
        if errors:
            return False, errors
        
        return True, {}
    
    def _validate_foreign_keys_attestations(self) -> Tuple[bool, List[int]]:
        """Validate that all node_id in attestations exist in dictionary_entries"""
        attestations_df = pd.read_csv(self.csv_dir / 'attestations.csv')
        entries_df = pd.read_csv(self.csv_dir / 'dictionary_entries.csv')
        
        attest_ids = set(attestations_df['node_id'].unique())
        valid_ids = set(entries_df['node_id'].unique())
        
        missing_ids = attest_ids - valid_ids
        
        if missing_ids:
            return False, list(missing_ids)
        
        return True, []
    
    def _validate_foreign_keys_citations(self) -> Tuple[bool, List[int]]:
        """Validate that all node_id in authority_citations exist in dictionary_entries"""
        citations_df = pd.read_csv(self.csv_dir / 'authority_citations.csv')
        entries_df = pd.read_csv(self.csv_dir / 'dictionary_entries.csv')
        
        citation_ids = set(citations_df['node_id'].unique())
        valid_ids = set(entries_df['node_id'].unique())
        
        missing_ids = citation_ids - valid_ids
        
        if missing_ids:
            return False, list(missing_ids)
        
        return True, []
    
    def validate_foreign_keys(self) -> bool:
        """Validate all foreign key relationships before import"""
        self.logger.info("=" * 70)
        self.logger.info("VALIDATING FOREIGN KEY RELATIONSHIPS")
        self.logger.info("=" * 70)
        
        all_valid = True
        
        # 1. Validate theme_slug  themes.slug
        self.logger.info("Checking entry_themes.theme_slug  themes.slug...")
        valid, missing = self._validate_foreign_keys_themes()
        if not valid:
            self.logger.error(f"Found {len(missing)} missing theme slugs: {missing[:10]}")
            all_valid = False
        else:
            self.logger.info(" All theme slugs valid")
        
        # 2. Validate entry_themes.entry_node_id  dictionary_entries.node_id
        self.logger.info("Checking entry_themes.entry_node_id  dictionary_entries.node_id...")
        valid, missing = self._validate_foreign_keys_entry_themes()
        if not valid:
            self.logger.error(f"Found {len(missing)} missing entry IDs in entry_themes: {missing[:10]}")
            all_valid = False
        else:
            self.logger.info(" All entry_node_ids valid in entry_themes")
        
        # 3. Validate entry_audio foreign keys
        self.logger.info("Checking entry_audio foreign keys...")
        valid, errors = self._validate_foreign_keys_entry_audio()
        if not valid:
            for key, values in errors.items():
                self.logger.error(f"{key}: {len(values)} missing - {values[:10]}")
            all_valid = False
        else:
            self.logger.info(" All foreign keys valid in entry_audio")
        
        # 4. Validate attestations.node_id  dictionary_entries.node_id
        self.logger.info("Checking attestations.node_id  dictionary_entries.node_id...")
        valid, missing = self._validate_foreign_keys_attestations()
        if not valid:
            self.logger.error(f"Found {len(missing)} missing entry IDs in attestations: {missing[:10]}")
            all_valid = False
        else:
            self.logger.info(" All node_ids valid in attestations")
        
        # 5. Validate authority_citations.node_id  dictionary_entries.node_id
        self.logger.info("Checking authority_citations.node_id  dictionary_entries.node_id...")
        valid, missing = self._validate_foreign_keys_citations()
        if not valid:
            self.logger.error(f"Found {len(missing)} missing entry IDs in citations: {missing[:10]}")
            all_valid = False
        else:
            self.logger.info(" All node_ids valid in authority_citations")
        
        self.logger.info("=" * 70)
        
        if not all_valid:
            self.logger.error("Foreign key validation FAILED")
            return False
        
        self.logger.info("Foreign key validation PASSED")
        return True
    
    def import_themes(self) -> None:
        """Import themes table"""
        self.logger.info("Importing themes...")
        
        df = pd.read_csv(self.csv_dir / 'themes.csv')
        
        # Drop extra columns not in schema
        columns_to_keep = ['tid', 'name', 'slug', 'description', 'vocabulary_id']
        df = df[columns_to_keep]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('themes', self.conn, if_exists='append', index=False)
        
        self.stats['themes']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} themes")
    
    def import_audio_files(self) -> None:
        """Import audio_files table"""
        self.logger.info("Importing audio_files...")
        
        df = pd.read_csv(self.csv_dir / 'audio_files.csv')
        
        # Drop extra columns not in schema
        columns_to_keep = [
            'node_id', 'headword', 'file_wav', 'file_mp3', 'file_aif',
            'speaker', 'date_recorded', 'url_alias', 'scrape_timestamp'
        ]
        df = df[columns_to_keep]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('audio_files', self.conn, if_exists='append', index=False)
        
        self.stats['audio_files']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} audio files")
    
    def import_dictionary_entries(self) -> None:
        """Import dictionary_entries table"""
        self.logger.info("Importing dictionary_entries...")
        
        df = pd.read_csv(self.csv_dir / 'dictionary_entries.csv')
        
        # Drop any empty trailing columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('dictionary_entries', self.conn, if_exists='append', index=False)
        
        self.stats['dictionary_entries']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} dictionary entries")
    
    def import_attestations(self) -> None:
        """Import attestations table"""
        self.logger.info("Importing attestations...")
        
        df = pd.read_csv(self.csv_dir / 'attestations.csv')
        
        # Don't include 'id' column (AUTOINCREMENT)
        columns_to_keep = ['node_id', 'language', 'attestation_text', 'source_field']
        df = df[columns_to_keep]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('attestations', self.conn, if_exists='append', index=False)
        
        self.stats['attestations']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} attestations")
    
    def import_authority_citations(self) -> None:
        """Import authority_citations table"""
        self.logger.info("Importing authority_citations...")
        
        df = pd.read_csv(self.csv_dir / 'authority_citations.csv')
        
        # Don't include 'id' column (AUTOINCREMENT)
        columns_to_keep = ['node_id', 'authority_name', 'citation_text', 'citation_order']
        df = df[columns_to_keep]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('authority_citations', self.conn, if_exists='append', index=False)
        
        self.stats['authority_citations']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} authority citations")
    
    def import_entry_themes(self) -> None:
        """
        Import entry_themes table
        Requires mapping theme_slug  tid from themes table
        """
        self.logger.info("Importing entry_themes...")
        
        df = pd.read_csv(self.csv_dir / 'entry_themes.csv')
        
        # Get theme slug  tid mapping from database
        if self.conn is None:
            return
        cursor = self.conn.execute("SELECT tid, slug FROM themes")
        slug_to_tid = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Map slug to tid
        df['theme_tid'] = df['theme_slug'].map(slug_to_tid)
        
        # Check for any unmapped slugs (should not happen if validation passed)
        unmapped = df[df['theme_tid'].isna()]
        if not unmapped.empty:
            self.logger.error(f"Found {len(unmapped)} unmapped theme slugs")
            raise ValueError("Unmapped theme slugs found after validation")
        
        # Select only columns for database
        columns_to_keep = ['entry_node_id', 'theme_tid', 'delta']
        df = df[columns_to_keep]
        
        # Convert theme_tid to int
        df['theme_tid'] = df['theme_tid'].astype(int)
        
        # Import to database
        df.to_sql('entry_themes', self.conn, if_exists='append', index=False)
        
        self.stats['entry_themes']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} entry-theme relationships")
    
    def import_entry_audio(self) -> None:
        """Import entry_audio table"""
        self.logger.info("Importing entry_audio...")
        
        df = pd.read_csv(self.csv_dir / 'entry_audio.csv')
        
        # Columns match schema exactly
        columns_to_keep = ['entry_node_id', 'audio_node_id', 'reference_type', 'delta']
        df = df[columns_to_keep]
        
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Import to database
        df.to_sql('entry_audio', self.conn, if_exists='append', index=False)
        
        self.stats['entry_audio']['imported'] = len(df)
        self.logger.info(f" Imported {len(df)} entry-audio relationships")
    
    def verify_data_integrity(self) -> bool:
        """Verify data integrity after import"""
        self.logger.info("=" * 70)
        self.logger.info("VERIFYING DATA INTEGRITY")
        self.logger.info("=" * 70)
        
        all_valid = True
        
        # Check row counts
        tables = [
            'themes', 'audio_files', 'dictionary_entries',
            'attestations', 'authority_citations', 'entry_themes', 'entry_audio'
        ]
        
        for table in tables:
            if self.conn is not None:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                expected = self.stats[table]['imported']
                
                if count != expected:
                    self.logger.error(f"{table}: Expected {expected}, found {count}")
                    all_valid = False
                else:
                    self.logger.info(f" {table}: {count} rows")
        
        # Check foreign key constraints
        self.logger.info("\nChecking foreign key constraints...")
        
        # Test foreign key enforcement
        if self.conn is None:
            return False
        cursor = self.conn.execute("PRAGMA foreign_key_check")
        fk_errors = cursor.fetchall()
        
        if fk_errors:
            self.logger.error(f"Foreign key constraint violations: {len(fk_errors)}")
            for error in fk_errors[:10]:  # Show first 10
                self.logger.error(f"  {error}")
            all_valid = False
        else:
            self.logger.info(" No foreign key violations")
        
        # Check for NULL in required fields
        self.logger.info("\nChecking for NULL values in required fields...")
        
        null_checks = [
            ("dictionary_entries", "node_id"),
            ("dictionary_entries", "headword"),
            ("themes", "tid"),
            ("themes", "slug"),
            ("audio_files", "node_id"),
        ]
        
        for table, column in null_checks:
            cursor = self.conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column} = ''"
            )
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                self.logger.warning(f"{table}.{column}: {null_count} NULL/empty values")
            else:
                self.logger.info(f" {table}.{column}: No NULL values")
        
        self.logger.info("=" * 70)
        
        if not all_valid:
            self.logger.error("Data integrity check FAILED")
            return False
        
        self.logger.info("Data integrity check PASSED")
        return True
    
    def print_statistics(self) -> None:
        """Print import statistics"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("IMPORT STATISTICS")
        self.logger.info("=" * 70)
        
        total_imported = sum(table['imported'] for table in self.stats.values())
        
        for table_name, stats in self.stats.items():
            self.logger.info(f"{table_name:25} {stats['imported']:>8,} rows")
        
        self.logger.info("-" * 70)
        self.logger.info(f"{'TOTAL':25} {total_imported:>8,} rows")
        self.logger.info("=" * 70)
        
        # Database file size
        if self.db_path.exists():
            size_mb = self.db_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"Database size: {size_mb:.2f} MB")
            self.logger.info(f"Database location: {self.db_path}")
    
    def run_import_pipeline(self, skip_validation: bool = False) -> bool:
        """
        Run complete import pipeline
        
        Args:
            skip_validation: Skip foreign key validation (not recommended)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 70)
            self.logger.info("NAHUATL DATABASE IMPORT PIPELINE")
            self.logger.info("=" * 70)
            self.logger.info(f"Database: {self.db_path}")
            self.logger.info(f"CSV Directory: {self.csv_dir}")
            self.logger.info(f"Schema: {self.schema_path}")
            self.logger.info("=" * 70)
            
            # Step 1: Validate CSV files exist
            if not self.validate_csvs():
                self.logger.error("CSV validation failed")
                return False
            
            # Step 2: Validate foreign keys (before creating database)
            if not skip_validation:
                if not self.validate_foreign_keys():
                    self.logger.error("Foreign key validation failed - aborting import")
                    return False
            else:
                self.logger.warning("Skipping foreign key validation (not recommended)")
            
            # Step 3: Create fresh database
            self.create_database(fresh=True)
            
            # Step 4: Connect to database
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            try:
                # Step 5: Import data in order (respecting foreign keys)
                self.logger.info("\n" + "=" * 70)
                self.logger.info("IMPORTING DATA")
                self.logger.info("=" * 70)
                
                self.import_themes()
                self.import_audio_files()
                self.import_dictionary_entries()
                self.import_attestations()
                self.import_authority_citations()
                self.import_entry_themes()
                self.import_entry_audio()
                
                # Commit transaction
                self.conn.commit()
                self.logger.info("\nAll data committed to database")
                
                # Step 6: Verify data integrity
                if not self.verify_data_integrity():
                    self.logger.error("Data integrity check failed")
                    return False
                
                # Step 7: Print statistics
                self.print_statistics()
                
                self.logger.info("\n" + "=" * 70)
                self.logger.info("IMPORT COMPLETED SUCCESSFULLY")
                self.logger.info("=" * 70)
                
                return True
                
            except Exception as e:
                self.logger.error(f"Error during import: {e}")
                self.conn.rollback()
                raise
            
            finally:
                self.conn.close()
        
        except Exception as e:
            self.logger.error(f"Import pipeline failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False


def main():
    """Run database import"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Import scraped CSV data into nahuatl.db'
    )
    parser.add_argument(
        '--db-path',
        default='data/scraped_data/nahuatl.db',
        help='Path to SQLite database'
    )
    parser.add_argument(
        '--csv-dir',
        default='data/interim/scraped',
        help='Directory containing CSV files'
    )
    parser.add_argument(
        '--schema',
        default='config/schema.sql',
        help='Path to schema.sql file'
    )
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip foreign key validation (not recommended)'
    )
    
    args = parser.parse_args()
    
    # Create importer
    importer = DatabaseImporter(
        db_path=args.db_path,
        csv_dir=args.csv_dir,
        schema_path=args.schema
    )
    
    # Run import
    success = importer.run_import_pipeline(skip_validation=args.skip_validation)
    
    if success:
        print("\n Database import completed successfully")
        sys.exit(0)
    else:
        print("\n✗ Database import failed - check logs for details")
        sys.exit(1)


if __name__ == "__main__":
    main()