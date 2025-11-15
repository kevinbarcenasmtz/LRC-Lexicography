"""
Configuration for RomLex scraper
"""

DIALECT_NAMES = {
    'rmcb': 'Banatiski Gurbet Romani',
    'rmcd': 'Dolenjski Romani', 
    'rmce': 'East Slovak Romani',
    'rmcp': 'Prekmurski Romani',
    'rmcr': 'Romungro Romani',
    'rmcs': 'Sremski Gurbet Romani',
    'rmcv': 'Veršend Romani',
    'rmff': 'Finnish Romani',
    'rmna': 'Macedonian Arli Romani',
    'rmnb': 'Macedonian Džambazi Romani',
    'rmnc': 'North Russian Romani',
    'rmne': 'Bugurdži Romani',
    'rmnk': 'Kosovo Arli Romani',
    'rmns': 'Sinte Romani',
    'rmnu': 'Kalderas Romani',
    'rmoo': 'Lovara Romani',
    'rmww': 'Welsh Romani',
    'rmyb': 'Burgenland Romani',
    'rmyd': 'Sofia Erli Romani',
    'rmyg': 'Gurbet Romani',
    'rmyh': 'Hungarian Vend Romani',
    'rmyk': 'Gurvari Romani',
    'rmyl': 'Lithuanian Romani',
    'rmys': 'Ursari Romani',
    'roml': 'Latvian Romani',
    'romr': 'Sepečides Romani',
    'romt': 'Crimean Romani'
}

PATTERN_MATCH_MODES = {
    'prefix': 'pr',
    'infix': 'in',
    'suffix': 'su',
    'fuzzy': 'fu'
}

SEARCH_PARAMS = {
    'reverse': 'n',
    'ignore_case': 'y',
    'ignore_marks': 'y',
    'word_class': '',
    'file': ''
}

API_CONFIG = {
    'base_url': 'http://romani.uni-graz.at/romlex/lex.cgi',
    'referer': 'http://romani.uni-graz.at/romlex/',
    'result_limit': 200,
    'default_translation': 'en'
}

SCRAPER_CONFIG = {
    'request_delay': 3.0, 
    'error_retry_delay': 10.0,
    'max_retries': 3,
    'backoff_multiplier': 2.0,
    'default_output_dir': 'data/',
    'inter_letter_delay': 5.0  
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Referer': 'http://romani.uni-graz.at/romlex/'
}