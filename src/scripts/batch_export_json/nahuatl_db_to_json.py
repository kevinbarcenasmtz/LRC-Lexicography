# from initial sqLite table get it already in json format skip all csv shenanigans as formatting becomes an issue

import sqlite3
import pandas as pd
import json
import re
from typing import List, Dict, Optional

class NahuatlJsonExporter:
    """Transformed db to batch import JSON format"""
    
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        
        # reference tbl abbreviations
        self.authority_mapping = {
            'Molina': 'VLM',
            'Karttunen': 'AND',
            'Carochi': 'GML',
            'Olmos': 'ALM',
            'Lockhart': 'NWN'
        }
    
    def extract_page_number(self, citation: str) -> str:
        """Extract page number from citation using regex"""
        if not citation:
            return ""
        
        patterns = [
            r'f\.\s*(\d+[rv]?)',           # f. 5r
            r'p\.\s*(\d+)',                # p. 123
            r',\s*(\d+[rv]?)\.',           # , 6.
            r',\s*(\d+–\d+)',              # , 214–215
            r'(\d+–\d+)\.',                # 242–243.
            r'vol\.\s*\d+,\s*(\d+–\d+)',   # vol. 2, 214–215
        ]
        
        for pattern in patterns:
            match = re.search(pattern, citation)
            if match:
                return match.group(1)
            
        return ""
    
    def match_citation_to_authority(self, citation: str) -> Optional[str]:
        """Match citation to authority abbreviation by author name"""
        citation_lower = citation.lower()
        
        for author_name, abbrev in self.authority_mapping.items():
            if author_name.lower() in citation_lower:
                return abbrev
        
        return None
    
    def parse_citations(self, citations_text: str) -> List[str]:
        """Parse pipe-separated citation tags"""
        if not citations_text or pd.isna(citations_text):
            return []
        
        # Split by pipe and clean
        citations = [c.strip() for c in citations_text.split('|')]
        return citations
    
    def build_whp_sources(self, row: pd.Series) -> List[Dict]:
        """Build Sources JSON for WHP/Classical Nahuatl entry"""
        sources = []
        citations = self.parse_citations(row['Citations'])
        
        # Authority columns to check
        authority_columns = {
            'Alonso de Molina': 'VLM',
            'Frances Karttunen': 'AND',
            'Horacio Carochi / English': 'GML',
            'Andrés de Olmos': 'ALM',
            "Lockhart's Nahuatl as Written": 'NWN'
        }
        
        # Process each authority column
        for col_name, abbrev in authority_columns.items():
            authority_content = row.get(col_name)
            
            # Skip if None or empty
            if not authority_content or pd.isna(authority_content) or authority_content == 'None':
                continue
            
            # Find matching citations
            matching_citations = [
                cit for cit in citations 
                if self.match_citation_to_authority(cit) == abbrev
            ]
            
            # Create source entry for each matching citation
            for citation in matching_citations:
                page_number = self.extract_page_number(citation)
                
                sources.append({
                    "source": abbrev,
                    "page_number": page_number,
                    "original_entry": authority_content + " " + citation
                })
            
            # If no matching citation found but column has content, add without bibliography
            if not matching_citations:
                sources.append({
                    "source": abbrev,
                    "page_number": "",
                    "original_entry": authority_content,
                })
        
        return sources
        
    def build_idiez_sources(self, row: pd.Series) -> List[Dict]:
        """Build Sources JSON for IDIEZ/Huasteca Nahuatl entry"""
        # Compile all IDIEZ fields
        fields = []
        
        if row.get('tlahtolli') and not pd.isna(row.get('tlahtolli')):
            fields.append(f"IDIEZ morfema: {row['tlahtolli']}")
        if row.get('IDIEZ traduc. inglés') and not pd.isna(row.get('IDIEZ traduc. inglés')):
            fields.append(f"IDIEZ traduc. inglés: {row['IDIEZ traduc. inglés']}")
        if row.get('IDIEZ def. náhuatl') and not pd.isna(row.get('IDIEZ def. náhuatl')):
            fields.append(f"IDIEZ def. náhuatl: {row['IDIEZ def. náhuatl']}")
        if row.get('IDIEZ def. español') and not pd.isna(row.get('IDIEZ def. español')):
            fields.append(f"IDIEZ def. español: {row['IDIEZ def. español']}")
        if row.get('IDIEZ morfología') and not pd.isna(row.get('IDIEZ morfología')):
            fields.append(f"IDIEZ morfología: {row['IDIEZ morfología']}")
        if row.get('IDIEZ gramática') and not pd.isna(row.get('IDIEZ gramática')):
            fields.append(f"IDIEZ gramática: {row['IDIEZ gramática']}")
        
        original_entry = ". ".join(fields) + "." if fields else ""
        
        return [{
            "source": "IDIEZ",
            "page_number": "",
            "original_entry": original_entry
        }]
        
    def process_whp_data(self, limit: Optional[int] = None) -> List[Dict]:
        """Process WHP data to JSON format"""
        print("Loading WHP data...")
        
        query = """
        SELECT 
            Ref,
            Headword,
            "Orthographic Variants",
            "Principal English Translation",
            "Attestations from sources in English",
            "Attestations from sources in Spanish",
            "Alonso de Molina",
            "Frances Karttunen",
            "Horacio Carochi / English",
            "Andrés de Olmos",
            "Lockhart's Nahuatl as Written",
            themes,
            "Spanish Loanword",
            Citations,
            Number_of_Citations
        FROM "checkpoint_after_citation_crossref_reinsertion_20251030"
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql(query, self.conn)
        
        print(f"Processing {len(df)} WHP entries...")
        
        export_rows = []
        
        for idx, row in df.iterrows():
            if idx % 1000 == 0: # type: ignore
                print(f"  Processing WHP entry {idx}...")
            
            entry = {
                'Headwords': row['Headword'],
                'Gloss': row['Principal English Translation'] or "",
                'Language': 'Classical Nahuatl',
                'Sources': self.build_whp_sources(row)
            }
            
            # Add themes if present
            if row.get('themes') and not pd.isna(row['themes']):
                entry['Themes'] = row['themes']
            
            export_rows.append(entry)
        
        return export_rows
    
    def process_idiez_data(self, limit: Optional[int] = None) -> List[Dict]:
        """Process IDIEZ data to JSON format"""
        print("\nLoading IDIEZ data...")
        
        query = """
        SELECT 
            Ref,
            OND_Node_Title,
            tlahtolli,
            "IDIEZ gramática",
            "IDIEZ def. náhuatl",
            "IDIEZ def. español",
            SShort_IDIEZ,
            "IDIEZ traduc. inglés",
            "IDIEZ morfología",
            Credit
        FROM "IDIEZ_modern_nahuatl-all-2024-03-27T09-45-31"
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql(query, self.conn)
        
        print(f"Processing {len(df)} IDIEZ entries...")
        
        export_rows = []
        
        for idx, row in df.iterrows():
            if idx % 1000 == 0: # type: ignore
                print(f"  Processing IDIEZ entry {idx}...")
            
            entry = {
                'Headwords': row['OND_Node_Title'],
                'Gloss': row['IDIEZ traduc. inglés'] or "",
                'Language': 'Huasteca Nahuatl',
                'Sources': self.build_idiez_sources(row)
            }
            
            # Add themes if present
            if row.get('themes') and not pd.isna(row['themes']):
                entry['Themes'] = row['themes']
                
            export_rows.append(entry)
        
        return export_rows
    
    def export_combined_json(self, output_path: str = "data/nahuatl_batch_import.json", limit: Optional[int] = None):
        """Export both WHP and IDIEZ to single combined JSON"""
        
        print("="*70)
        print("EXPORTING COMBINED NAHUATL LEXICON TO JSON")
        if limit:
            print(f"TEST MODE: Limited to {limit} records per dataset")
        print("="*70)
        print()
        
        # Process WHP
        whp_entries = self.process_whp_data(limit=limit)
        
        # Process IDIEZ
        idiez_entries = self.process_idiez_data(limit=limit)
        
        # Combine
        print("\nCombining datasets...")
        combined_entries = whp_entries + idiez_entries
        
        # Export to JSON
        print(f"Writing JSON to {output_path}...")
        with open(output_path, 'w', encoding='utf-8-sig') as f:
            json.dump(combined_entries, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*70}")
        print(f"✓ EXPORT COMPLETE")
        print(f"{'='*70}")
        print(f"Total entries: {len(combined_entries)}")
        print(f"  - Classical Nahuatl: {len(whp_entries)}")
        print(f"  - Huasteca Nahuatl: {len(idiez_entries)}")
        print(f"\nOutput file: {output_path}")
        
        # Print sample output for verification
        if limit and limit <= 5:
            print(f"\n{'='*70}")
            print("SAMPLE OUTPUT:")
            print(f"{'='*70}")
            print(json.dumps(combined_entries, ensure_ascii=False, indent=2))
        
        return combined_entries


# Usage
if __name__ == "__main__":
    # Update with your actual database path
    exporter = NahuatlJsonExporter("../../../data/sqLiteDb/nahuatl_processing.db")
    
    # TEST: Export only 2 records from each dataset
    # print("Running TEST with 2 records per dataset...\n")
    # test_data = exporter.export_combined_json("data/nahuatl_test_2records.json", limit=2)
    
    # Uncomment below to run full export
    combined_data = exporter.export_combined_json("data/nahuatl_batch_import.json")