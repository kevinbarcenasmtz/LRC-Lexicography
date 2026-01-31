"""
Apply manual review decisions to scraped and local databases.
Processes translation, authority citation, and attestation mismatch reports
and updates the appropriate database(s) based on the Decision column.

Usage:
    python apply_review_decisions.py --dry-run  # Preview changes
    python apply_review_decisions.py            # Apply changes
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re
import argparse

class ReviewDecisionApplicator:
    def __init__(
        self,
        scraped_db_path: str,
        local_db_path: str,
        local_table: str,
        dry_run: bool = True
    ):
        self.scraped_db_path = Path(scraped_db_path)
        self.local_db_path = Path(local_db_path)
        self.local_table = local_table
        self.dry_run = dry_run
        
        self.scraped_conn = None
        self.local_conn = None
        
        self.stats = {
            'translations': {'scraped': 0, 'local': 0, 'both': 0, 'deleted': 0, 'skipped': 0},
            'authorities': {'scraped': 0, 'local': 0, 'both': 0, 'deleted': 0, 'skipped': 0},
            'attestations': {'scraped': 0, 'local': 0, 'both': 0, 'deleted': 0, 'skipped': 0}
        }
        
        self.authority_mapping = {
            'Alonso de Molina': 'Molina',
            'Frances Karttunen': 'Karttunen',
            'Horacio Carochi / English': 'Carochi',
            'Andrés de Olmos': 'Olmos',
            "Lockhart’s Nahuatl as Written": 'Lockhart'
        }
        
        self.changes_log = []
        self.spanish_attestation_notes = []
        
    def connect(self):
        """Establish database connections"""
        print("Connecting to databases...")
        self.scraped_conn = sqlite3.connect(self.scraped_db_path)
        self.local_conn = sqlite3.connect(self.local_db_path)
        
        if not self.dry_run:
            self.scraped_conn.execute("PRAGMA foreign_keys = ON")
            self.local_conn.execute("PRAGMA foreign_keys = ON")
        
        print(f" Scraped DB: {self.scraped_db_path}")
        print(f" Local DB: {self.local_db_path}")
        print(f" Local table: {self.local_table}")
        
    def close(self):
        """Close database connections"""
        if self.scraped_conn:
            self.scraped_conn.close()
        if self.local_conn:
            self.local_conn.close()
            
    def normalize_decision(self, decision: str) -> Tuple[str, bool]:
        """
        Normalize decision string to standard action.
        Returns: (action, check_spanish_attestation)
        
        Possible actions:
        - 'use_scraped_update_local'
        - 'use_local_update_scraped'
        - 'update_both_from_scraped'
        - 'update_both_from_local'
        - 'delete_local'
        - 'skip'
        """
        if pd.isna(decision) or decision.strip() == "":
            return 'skip', False
            
        decision_lower = decision.lower().strip()
        check_spanish = 'spanish attestation' in decision_lower or 'spanish one' in decision_lower
        
        if 'use scraped' in decision_lower and 'update local' in decision_lower:
            return 'use_scraped_update_local', check_spanish
        elif 'scraped value is the correct' in decision_lower and 'update local' in decision_lower:
            return 'use_scraped_update_local', check_spanish
        elif 'use scraped to update both' in decision_lower:
            return 'update_both_from_scraped', check_spanish
        elif 'update both' in decision_lower and 'based off scraped' in decision_lower:
            return 'update_both_from_scraped', check_spanish
            
        elif 'use local' in decision_lower and 'update scraped' in decision_lower:
            return 'use_local_update_scraped', check_spanish
        elif 'local value is the correct' in decision_lower and 'update scraped' in decision_lower:
            return 'use_local_update_scraped', check_spanish
        elif 'local value is incorrect' in decision_lower:
            return 'use_scraped_update_local', check_spanish
        elif 'update both' in decision_lower and 'based off local' in decision_lower:
            return 'update_both_from_local', check_spanish
        elif 'update local dataset value with current local' in decision_lower:
            return 'use_local_update_scraped', check_spanish
            
        elif 'local will have' in decision_lower and 'value deleted' in decision_lower:
            return 'delete_local', check_spanish
        elif 'actual value in both is none' in decision_lower:
            return 'delete_local', check_spanish
            
        else:
            print(f"  WARNING: Unrecognized decision: '{decision}'")
            return 'skip', check_spanish
    
    def update_scraped_translation(self, node_id: str, new_value: str) -> bool:
        """Update translation in scraped database"""
        try:
            cursor = self.scraped_conn.cursor() # type: ignore
            
            old_value = cursor.execute(
                "SELECT translation_english FROM dictionary_entries WHERE node_id = ?",
                (node_id,)
            ).fetchone()
            
            if old_value:
                old_value = old_value[0]
            
            if not self.dry_run:
                cursor.execute(
                    "UPDATE dictionary_entries SET translation_english = ? WHERE node_id = ?",
                    (new_value, node_id)
                )
                self.scraped_conn.commit() # pyright: ignore[reportOptionalMemberAccess]
            
            self.changes_log.append({
                'database': 'scraped',
                'table': 'dictionary_entries',
                'node_id': node_id,
                'field': 'translation_english',
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating scraped translation for node {node_id}: {e}")
            return False
    
    def update_local_translation(self, node_id: str, new_value: str) -> bool:
        """Update translation in local database checkpoint table"""
        try:
            cursor = self.local_conn.cursor() # type: ignore
            
            ref_value = f"WHP-{node_id}"
            
            old_value = cursor.execute(
                f'SELECT "Principal English Translation" FROM [{self.local_table}] WHERE Ref = ?',
                (ref_value,)
            ).fetchone()
            
            if old_value:
                old_value = old_value[0]
            
            if not self.dry_run:
                cursor.execute(
                    f'UPDATE [{self.local_table}] SET "Principal English Translation" = ? WHERE Ref = ?',
                    (new_value, ref_value)
                )
                self.local_conn.commit()
            
            self.changes_log.append({
                'database': 'local',
                'table': self.local_table,
                'node_id': node_id,
                'field': 'Principal English Translation',
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating local translation for node {node_id}: {e}")
            return False
    
    def update_scraped_authority(self, node_id: str, authority: str, new_value: str) -> bool:
        """Update authority citation in scraped database"""
        try:
            cursor = self.scraped_conn.cursor() # type: ignore
            
            existing = cursor.execute(
                "SELECT id, citation_text FROM authority_citations WHERE node_id = ? AND authority_name = ?",
                (node_id, authority)
            ).fetchone()
            
            if existing:
                citation_id, old_value = existing
                if not self.dry_run:
                    if new_value and new_value.strip():
                        cursor.execute(
                            "UPDATE authority_citations SET citation_text = ? WHERE id = ?",
                            (new_value, citation_id)
                        )
                    else:
                        cursor.execute(
                            "DELETE FROM authority_citations WHERE id = ?",
                            (citation_id,)
                        )
                    self.scraped_conn.commit()
            else:
                old_value = None
                if new_value and new_value.strip() and not self.dry_run:
                    cursor.execute(
                        "INSERT INTO authority_citations (node_id, authority_name, citation_text, citation_order) VALUES (?, ?, ?, 0)",
                        (node_id, authority, new_value)
                    )
                    self.scraped_conn.commit()
            
            self.changes_log.append({
                'database': 'scraped',
                'table': 'authority_citations',
                'node_id': node_id,
                'field': f'authority_{authority}',
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating scraped authority {authority} for node {node_id}: {e}")
            return False
    
    def update_local_authority(self, node_id: str, authority_local_name: str, new_value: str) -> bool:
        """Update authority citation in local database checkpoint table"""
        try:
            cursor = self.local_conn.cursor()
            
            ref_value = f"WHP-{node_id}"
            
            old_value = cursor.execute(
                f'SELECT "{authority_local_name}" FROM [{self.local_table}] WHERE Ref = ?',
                (ref_value,)
            ).fetchone()
            
            if old_value:
                old_value = old_value[0]
            
            if not self.dry_run:
                cursor.execute(
                    f'UPDATE [{self.local_table}] SET "{authority_local_name}" = ? WHERE Ref = ?',
                    (new_value, ref_value)
                )
                self.local_conn.commit()
            
            self.changes_log.append({
                'database': 'local',
                'table': self.local_table,
                'node_id': node_id,
                'field': authority_local_name,
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating local authority {authority_local_name} for node {node_id}: {e}")
            return False
    
    def update_scraped_attestation(self, node_id: str, language: str, new_value: str) -> bool:
        """Update attestation in scraped database"""
        try:
            cursor = self.scraped_conn.cursor()
            
            existing = cursor.execute(
                "SELECT id, attestation_text FROM attestations WHERE node_id = ? AND language = ?",
                (node_id, language)
            ).fetchone()
            
            if existing:
                attestation_id, old_value = existing
                if not self.dry_run:
                    if new_value and new_value.strip():
                        cursor.execute(
                            "UPDATE attestations SET attestation_text = ? WHERE id = ?",
                            (new_value, attestation_id)
                        )
                    else:
                        cursor.execute(
                            "DELETE FROM attestations WHERE id = ?",
                            (attestation_id,)
                        )
                    self.scraped_conn.commit()
            else:
                old_value = None
                if new_value and new_value.strip() and not self.dry_run:
                    cursor.execute(
                        "INSERT INTO attestations (node_id, language, attestation_text, source_field) VALUES (?, ?, ?, ?)",
                        (node_id, language, new_value, f'field_attestation_{language.lower()}')
                    )
                    self.scraped_conn.commit()
            
            self.changes_log.append({
                'database': 'scraped',
                'table': 'attestations',
                'node_id': node_id,
                'field': f'attestation_{language}',
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating scraped attestation {language} for node {node_id}: {e}")
            return False
    
    def update_local_attestation(self, node_id: str, language: str, new_value: str) -> bool:
        """Update attestation in local database checkpoint table"""
        try:
            cursor = self.local_conn.cursor()
            
            ref_value = f"WHP-{node_id}"
            column_name = f"Attestations from sources in {language}"
            
            old_value = cursor.execute(
                f'SELECT "{column_name}" FROM [{self.local_table}] WHERE Ref = ?',
                (ref_value,)
            ).fetchone()
            
            if old_value:
                old_value = old_value[0]
            
            if not self.dry_run:
                cursor.execute(
                    f'UPDATE [{self.local_table}] SET "{column_name}" = ? WHERE Ref = ?',
                    (new_value, ref_value)
                )
                self.local_conn.commit()
            
            self.changes_log.append({
                'database': 'local',
                'table': self.local_table,
                'node_id': node_id,
                'field': column_name,
                'old_value': old_value,
                'new_value': new_value
            })
            
            return True
        except Exception as e:
            print(f"  ERROR updating local attestation {language} for node {node_id}: {e}")
            return False
    
    def process_translations(self, report_path: str):
        """Process translation mismatch report"""
        print("\n" + "="*80)
        print("PROCESSING TRANSLATION MISMATCHES")
        print("="*80)
        
        if not Path(report_path).exists():
            print(f"  WARNING: Report not found: {report_path}")
            return
        
        df = pd.read_csv(report_path, encoding='utf-8-sig')
        
        if 'Decision' not in df.columns:
            print("  ERROR: 'Decision' column not found in report")
            return
        
        print(f"  Loaded {len(df)} translation mismatches")
        
        for idx, row in df.iterrows():
            node_id = str(row['node_id'])
            scraped_val = row['scraped_translation_clean']
            local_val = row['local_translation_clean']
            decision = row['Decision']
            
            action, check_spanish = self.normalize_decision(decision)
            
            if check_spanish:
                self.spanish_attestation_notes.append({
                    'node_id': node_id,
                    'headword': row.get('headword', ''),
                    'field_type': 'translation',
                    'note': decision
                })
            
            if action == 'skip':
                self.stats['translations']['skipped'] += 1
                continue
            
            if action == 'use_scraped_update_local':
                if self.update_local_translation(node_id, scraped_val):
                    self.stats['translations']['local'] += 1
                    
            elif action == 'use_local_update_scraped':
                if self.update_scraped_translation(node_id, local_val):
                    self.stats['translations']['scraped'] += 1
                    
            elif action == 'update_both_from_scraped':
                success = True
                if not self.update_scraped_translation(node_id, scraped_val):
                    success = False
                if not self.update_local_translation(node_id, scraped_val):
                    success = False
                if success:
                    self.stats['translations']['both'] += 1
                    
            elif action == 'update_both_from_local':
                success = True
                if not self.update_scraped_translation(node_id, local_val):
                    success = False
                if not self.update_local_translation(node_id, local_val):
                    success = False
                if success:
                    self.stats['translations']['both'] += 1
                    
            elif action == 'delete_local':
                if self.update_local_translation(node_id, ''):
                    self.stats['translations']['deleted'] += 1
        
        print(f"\n  Translation updates:")
        print(f"    Scraped DB updated: {self.stats['translations']['scraped']}")
        print(f"    Local DB updated: {self.stats['translations']['local']}")
        print(f"    Both updated: {self.stats['translations']['both']}")
        print(f"    Local deleted: {self.stats['translations']['deleted']}")
        print(f"    Skipped: {self.stats['translations']['skipped']}")
    
    def process_authorities(self, report_path: str):
        """Process authority citation mismatch report"""
        print("\n" + "="*80)
        print("PROCESSING AUTHORITY CITATION MISMATCHES")
        print("="*80)
        
        if not Path(report_path).exists():
            print(f"  WARNING: Report not found: {report_path}")
            return
        
        df = pd.read_csv(report_path, encoding='utf-8-sig')
        
        if 'Decision' not in df.columns:
            print("  ERROR: 'Decision' column not found in report")
            return
        
        print(f"  Loaded {len(df)} authority citation mismatches")
        
        for idx, row in df.iterrows():
            node_id = str(row['node_id'])
            authority_scraped = row['authority']
            scraped_val = row['scraped_value']
            local_val = row['local_value']
            decision = row['Decision']
            
            authority_local_name = [k for k, v in self.authority_mapping.items() if v == authority_scraped]
            if not authority_local_name:
                print(f"  WARNING: Unknown authority {authority_scraped} for node {node_id}")
                continue
            authority_local_name = authority_local_name[0]
            
            action, check_spanish = self.normalize_decision(decision)
            
            if check_spanish:
                self.spanish_attestation_notes.append({
                    'node_id': node_id,
                    'headword': row.get('headword', ''),
                    'field_type': f'authority_{authority_scraped}',
                    'note': decision
                })
            
            if action == 'skip':
                self.stats['authorities']['skipped'] += 1
                continue
            
            if action == 'use_scraped_update_local':
                if self.update_local_authority(node_id, authority_local_name, scraped_val):
                    self.stats['authorities']['local'] += 1
                    
            elif action == 'use_local_update_scraped':
                if self.update_scraped_authority(node_id, authority_scraped, local_val):
                    self.stats['authorities']['scraped'] += 1
                    
            elif action == 'update_both_from_scraped':
                success = True
                if not self.update_scraped_authority(node_id, authority_scraped, scraped_val):
                    success = False
                if not self.update_local_authority(node_id, authority_local_name, scraped_val):
                    success = False
                if success:
                    self.stats['authorities']['both'] += 1
                    
            elif action == 'update_both_from_local':
                success = True
                if not self.update_scraped_authority(node_id, authority_scraped, local_val):
                    success = False
                if not self.update_local_authority(node_id, authority_local_name, local_val):
                    success = False
                if success:
                    self.stats['authorities']['both'] += 1
                    
            elif action == 'delete_local':
                if self.update_local_authority(node_id, authority_local_name, ''):
                    self.stats['authorities']['deleted'] += 1
        
        print(f"\n  Authority citation updates:")
        print(f"    Scraped DB updated: {self.stats['authorities']['scraped']}")
        print(f"    Local DB updated: {self.stats['authorities']['local']}")
        print(f"    Both updated: {self.stats['authorities']['both']}")
        print(f"    Local deleted: {self.stats['authorities']['deleted']}")
        print(f"    Skipped: {self.stats['authorities']['skipped']}")
    
    def process_attestations(self, report_path: str):
        """Process attestation mismatch report"""
        print("\n" + "="*80)
        print("PROCESSING ATTESTATION MISMATCHES")
        print("="*80)
        
        if not Path(report_path).exists():
            print(f"  WARNING: Report not found: {report_path}")
            return
        
        df = pd.read_csv(report_path, encoding='utf-8-sig')
        
        if 'Decision' not in df.columns:
            print("  ERROR: 'Decision' column not found in report")
            return
        
        print(f"  Loaded {len(df)} attestation mismatches")
        
        for idx, row in df.iterrows():
            node_id = str(row['node_id'])
            language = row['language']
            scraped_val = row['scraped_value']
            local_val = row['local_value']
            decision = row['Decision']
            
            action, check_spanish = self.normalize_decision(decision)
            
            if check_spanish:
                self.spanish_attestation_notes.append({
                    'node_id': node_id,
                    'headword': row.get('headword', ''),
                    'field_type': f'attestation_{language}',
                    'note': decision
                })
            
            if action == 'skip':
                self.stats['attestations']['skipped'] += 1
                continue
            
            if action == 'use_scraped_update_local':
                if self.update_local_attestation(node_id, language, scraped_val):
                    self.stats['attestations']['local'] += 1
                    
            elif action == 'use_local_update_scraped':
                if self.update_scraped_attestation(node_id, language, local_val):
                    self.stats['attestations']['scraped'] += 1
                    
            elif action == 'update_both_from_scraped':
                success = True
                if not self.update_scraped_attestation(node_id, language, scraped_val):
                    success = False
                if not self.update_local_attestation(node_id, language, scraped_val):
                    success = False
                if success:
                    self.stats['attestations']['both'] += 1
                    
            elif action == 'update_both_from_local':
                success = True
                if not self.update_scraped_attestation(node_id, language, local_val):
                    success = False
                if not self.update_local_attestation(node_id, language, local_val):
                    success = False
                if success:
                    self.stats['attestations']['both'] += 1
                    
            elif action == 'delete_local':
                if self.update_local_attestation(node_id, language, ''):
                    self.stats['attestations']['deleted'] += 1
        
        print(f"\n  Attestation updates:")
        print(f"    Scraped DB updated: {self.stats['attestations']['scraped']}")
        print(f"    Local DB updated: {self.stats['attestations']['local']}")
        print(f"    Both updated: {self.stats['attestations']['both']}")
        print(f"    Local deleted: {self.stats['attestations']['deleted']}")
        print(f"    Skipped: {self.stats['attestations']['skipped']}")
    
    def create_checkpoints(self):
        """Create new checkpoint tables in both databases"""
        if self.dry_run:
            print("\n" + "="*80)
            print("CHECKPOINT CREATION (DRY RUN - SKIPPED)")
            print("="*80)
            return
        
        print("\n" + "="*80)
        print("CREATING CHECKPOINTS")
        print("="*80)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        local_checkpoint_name = f"checkpoint_after_manual_review_{timestamp}"
        print(f"\n  Creating local checkpoint: {local_checkpoint_name}")
        
        try:
            self.local_conn.execute(f"""
                CREATE TABLE [{local_checkpoint_name}] AS 
                SELECT * FROM [{self.local_table}]
            """)
            self.local_conn.commit()
            
            row_count = self.local_conn.execute(
                f"SELECT COUNT(*) FROM [{local_checkpoint_name}]"
            ).fetchone()[0]
            
            print(f"   Created with {row_count:,} rows")
        except Exception as e:
            print(f"    ERROR creating local checkpoint: {e}")
        
        scraped_checkpoint_name = f"checkpoint_after_manual_review_{timestamp}"
        print(f"\n  Creating scraped checkpoint: {scraped_checkpoint_name}")
        
        try:
            self.scraped_conn.execute(f"""
                CREATE TABLE [{scraped_checkpoint_name}] AS 
                SELECT * FROM dictionary_entries
            """)
            self.scraped_conn.commit()
            
            row_count = self.scraped_conn.execute(
                f"SELECT COUNT(*) FROM [{scraped_checkpoint_name}]"
            ).fetchone()[0]
            
            print(f"   Created with {row_count:,} rows")
        except Exception as e:
            print(f"    ERROR creating scraped checkpoint: {e}")
    
    def save_changes_log(self):
        """Save detailed changes log to CSV"""
        if not self.changes_log:
            print("\n  No changes to log")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"changes_log_{timestamp}.csv"
        
        df = pd.DataFrame(self.changes_log)
        df.to_csv(log_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n  Changes log saved: {log_filename}")
        print(f"    Total changes: {len(self.changes_log)}")
    
    def save_spanish_attestation_notes(self):
        """Save Spanish attestation follow-up notes"""
        if not self.spanish_attestation_notes:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        notes_filename = f"spanish_attestation_followup_{timestamp}.csv"
        
        df = pd.DataFrame(self.spanish_attestation_notes)
        df.to_csv(notes_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n  Spanish attestation follow-up notes: {notes_filename}")
        print(f"    Items requiring manual check: {len(self.spanish_attestation_notes)}")
    
    def print_summary(self):
        """Print final summary statistics"""
        print("\n" + "="*80)
        print("FINAL SUMMARY")
        print("="*80)
        
        if self.dry_run:
            print("\n  ⚠ DRY RUN MODE - No actual changes made to databases")
        else:
            print("\n Changes applied to databases")
        
        total_updates = sum(
            self.stats[category][action] 
            for category in self.stats 
            for action in ['scraped', 'local', 'both', 'deleted']
        )
        
        print(f"\n  Total updates: {total_updates}")
        print(f"\n  By category:")
        print(f"    Translations: {sum(self.stats['translations'].values())}")
        print(f"    Authority citations: {sum(self.stats['authorities'].values())}")
        print(f"    Attestations: {sum(self.stats['attestations'].values())}")
        
        print(f"\n  By target:")
        scraped_total = sum(self.stats[cat]['scraped'] + self.stats[cat]['both'] for cat in self.stats)
        local_total = sum(self.stats[cat]['local'] + self.stats[cat]['both'] for cat in self.stats)
        print(f"    Scraped DB updates: {scraped_total}")
        print(f"    Local DB updates: {local_total}")
        print(f"    Deletions: {sum(self.stats[cat]['deleted'] for cat in self.stats)}")
        
        if self.spanish_attestation_notes:
            print(f"\n  ⚠ Spanish attestation checks needed: {len(self.spanish_attestation_notes)}")
            print(f"    See: spanish_attestation_followup_*.csv")


def main():
    parser = argparse.ArgumentParser(
        description='Apply manual review decisions to scraped and local databases'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Preview changes without applying them (default: True)'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Actually apply changes to databases (overrides --dry-run)'
    )
    parser.add_argument(
        '--scraped-db',
        default='../../../data/scrapedDataDb/nahuatl.db',
        help='Path to scraped database'
    )
    parser.add_argument(
        '--local-db',
        default='../../../data/sqLiteDb/nahuatl_processing.db',
        help='Path to local processing database'
    )
    parser.add_argument(
        '--local-table',
        default='checkpoint_llm_validated_20251030',
        help='Local checkpoint table name'
    )
    
    args = parser.parse_args()
    
    dry_run = not args.apply
    
    print("="*80)
    print("MANUAL REVIEW DECISION APPLICATOR")
    print("="*80)
    print(f"\nMode: {'DRY RUN' if dry_run else 'APPLY CHANGES'}")
    
    applicator = ReviewDecisionApplicator(
        scraped_db_path=args.scraped_db,
        local_db_path=args.local_db,
        local_table=args.local_table,
        dry_run=dry_run
    )
    
    try:
        applicator.connect()
        
        applicator.process_translations('./src/notebooks/cross_validation/report_translation_mismatches.csv')
        applicator.process_authorities('./src/notebooks/cross_validation/report_authority_mismatches.csv')
        applicator.process_attestations('./src/notebooks/cross_validation/report_attestation_mismatches.csv')
        
        applicator.create_checkpoints()
        applicator.save_changes_log()
        applicator.save_spanish_attestation_notes()
        
        applicator.print_summary()
        
    finally:
        applicator.close()
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()