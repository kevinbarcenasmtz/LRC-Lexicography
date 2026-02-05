"""
Burrow & Emeneau language abbreviation mappings.
Based on frontmatter §52 from the printed dictionary.
"""

BURROW_TO_STARLING = {
    'ĀlKu.': 'Ālu Kuṟumba',
    'Bel.': 'Belari',
    'Br.': 'Brahui',
    'Dr.': 'Proto-Dravidian',
    'PDr.': 'Proto-Dravidian',
    'Ga.': 'Gadba',
    'Go.': 'Gondi',
    'Ir.': 'Iruḷa',
    'Ka.': 'Kannada',
    'Ko.': 'Kota',
    'Koḍ.': 'Kodagu',
    'Kol.': 'Kolami',
    'Kor.': 'Koraga',
    'Kur.': 'Kurukh',
    'Kurub.': 'Beṭṭa Kuruba',
    'Ma.': 'Malayalam',
    'Malt.': 'Malto',
    'Manḍ.': 'Manda',
    'Nk.': 'Naikri',
    'Nk. (Ch.)': 'Naiki',
    'Pa.': 'Parji',
    'PālKu.': 'Pālu Kuṟumba',
    'Pe.': 'Pengo',
    'Ta.': 'Tamil',
    'Te.': 'Telugu',
    'To.': 'Toda',
    'Tu.': 'Tulu',
    'Konḍa': 'Konda',
    'Kui': 'Kui',
    'Kuwi': 'Kuwi (Schulze)',
}

STARLING_VARIANTS = {
    'Gondi': [
        'Koya Gondi', 'Muria Gondi', 'Maria Gondi', 'Betul Gondi',
        'Adilabad Gondi', 'Mandla Gondi (Phailbus)', 'Maria Gondi (Mitchell)',
        'Mandla Gondi (Williamson)', 'Seoni Gondi', 'Gommu Gondi',
        'Yeotmal Gondi', 'Chindwara Gondi', 'Durg Gondi', 'Chanda Gondi',
        'Mandla Gondi', 'Maria Gondi (Lind)', 'Maria Gondi (Smith)'
    ],
    'Gadba': [
        'Salur Gadba', 'Ollari Gadba', 'Kondekor Gadba', 'Poya Gadba'
    ],
    'Kuwi': [
        'Kuwi (Schulze)', 'Kuwi (Fitzgerald)', 'Kuwi (Israel)',
        'Sunkarametta Kuwi', 'Kuwi (Mahanti)', 'Tekriya Kuwi', 'Dongriya Kuwi',
        'Parja Kuwi'
    ],
    'Kolami': [
        'Kinwat Kolami', 'Kolami (Setumadhava Rao)'
    ],
    'Konda': [
        'Konda (Burrow/Bhattacharya)'
    ],
    'Kui': [
        'Khuttia Kui'
    ],
    'Telugu': [
        'Telugu (Krishnamurti)', 'Inscriptional Telugu', 'Merolu Telugu',
        'Proto-Telugu'
    ]
}

def normalize_language(burrow_abbrev: str) -> str:
    """
    Convert Burrow abbreviation to standardized StarlingDB language name.
    Returns the base language name (variants must be matched separately).
    """
    return BURROW_TO_STARLING.get(burrow_abbrev, burrow_abbrev)


def match_language_variant(burrow_lang: str, starling_lang: str) -> bool:
    """
    Check if StarlingDB language matches Burrow language (including variants).
    
    Examples:
        match_language_variant('Gondi', 'Maria Gondi') → True
        match_language_variant('Kuwi', 'Kuwi (Schulze)') → True
        match_language_variant('Tamil', 'Telugu') → False
    """
    normalized_burrow = normalize_language(burrow_lang)
    
    if normalized_burrow == starling_lang:
        return True
    
    for base_lang, variants in STARLING_VARIANTS.items():
        if normalized_burrow == base_lang and starling_lang in variants:
            return True
        if normalized_burrow in variants and starling_lang == base_lang:
            return True
        if normalized_burrow in variants and starling_lang in variants:
            return True
    
    return False


if __name__ == "__main__":
    print("Burrow → StarlingDB Language Mappings")
    print("="*70)
    
    for burrow, starling in sorted(BURROW_TO_STARLING.items()):
        print(f"{burrow:<15} → {starling}")
    
    print(f"\nTotal mappings: {len(BURROW_TO_STARLING)}")
    
    print("\n\nVariant Groups:")
    print("="*70)
    for base, variants in STARLING_VARIANTS.items():
        print(f"\n{base}:")
        for v in variants:
            print(f"  - {v}")
    
    print("\n\nTest Cases:")
    print("="*70)
    test_cases = [
        ('Go.', 'Maria Gondi'),
        ('Kuwi', 'Kuwi (Schulze)'),
        ('Ta.', 'Tamil'),
        ('Te.', 'Telugu (Krishnamurti)'),
        ('Ka.', 'Telugu'),
    ]
    
    for burrow, starling in test_cases:
        result = match_language_variant(burrow, starling)
        print(f"{burrow:<10} vs {starling:<30} → {result}")