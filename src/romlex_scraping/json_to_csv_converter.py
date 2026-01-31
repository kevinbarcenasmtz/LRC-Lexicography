import json
import csv
from pathlib import Path
import pandas as pd
import sys

def flatten_entry_for_csv(entry):
    """Flatten entry structure for CSV/Excel export"""
    
    glosses_formatted = []
    
    for i, gloss_group in enumerate(entry.get('glosses', []), 1):
        sense_parts = []
        for sense in gloss_group:
            translations = []
            for trans in sense:
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
        'entry_id': entry.get('id', ''),
        'dialect_code': entry.get('dialect', ''),
        'headword': entry.get('orthographic_form', ''),
        'part_of_speech': entry.get('pos', ''),
        'gloss': ' | '.join(glosses_formatted) if glosses_formatted else ''
    }

def convert_json_to_csv(json_file):
    """Convert RomLex JSON to CSV and XLSX"""
    
    json_path = Path(json_file)
    
    if not json_path.exists():
        print(f"Error: File not found: {json_file}")
        sys.exit(1)
    
    print(f"Reading: {json_path}")
    with open(json_path, 'r', encoding='utf-8') as f:
        entries = json.load(f)
    
    print(f"Found {len(entries)} entries")
    
    flattened_entries = [flatten_entry_for_csv(e) for e in entries]
    
    csv_file = json_path.with_suffix('.csv')
    print(f"Writing CSV: {csv_file}")
    if flattened_entries:
        with open(csv_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=flattened_entries[0].keys())
            writer.writeheader()
            writer.writerows(flattened_entries)
    
    xlsx_file = json_path.with_suffix('.xlsx')
    print(f"Writing XLSX: {xlsx_file}")
    df = pd.DataFrame(flattened_entries)
    df.to_excel(xlsx_file, index=False, engine='openpyxl')
    
    print("\n✓ Conversion complete!")
    print(f"  CSV:  {csv_file}")
    print(f"  XLSX: {xlsx_file}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python json_to_csv_converter.py <json_file>")
        print("Example: python json_to_csv_converter.py data/romlex/rmyb_20251115_164520.json")
        sys.exit(1)
    
    convert_json_to_csv(sys.argv[1])