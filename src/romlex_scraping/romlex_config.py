"""
Configuration for RomLex scraper
"""

DIALECT_NAMES = {
    'rmcb': 'Burgenland Romani',
    'rmcd': 'Dolenjski Romani', 
    'rmce': 'East Slovak Romani',
    'rmcp': 'Prekmurski Romani',
    'rmcr': 'Romungro Romani',
    'rmcs': 'Veršend Romani',
    'rmcv': 'Hungarian Vend Romani',
    'rmff': 'Finnish Romani',
    'rmna': 'Macedonian Arli Romani',
    'rmnb': 'Bugurdži Romani',
    'rmnc': 'Crimean Romani',
    'rmne': 'Sofia Erli Romani',
    'rmnk': 'Kosovo Arli Romani',
    'rmns': 'Sepečides Romani',
    'rmnu': 'Ursari Romani',
    'rmoo': 'Sinte Romani',
    'rmww': 'Welsh Romani',
    'rmyb': 'Banatiski Gurbet Romani',
    'rmyd': 'Macedonian Džambazi Romani',
    'rmyg': 'Gurbet Romani',
    'rmyh': 'Gurvari Romani',
    'rmyk': 'Kalderaš Romani',
    'rmyl': 'Lovara Romani',
    'rmys': 'Sremski Gurbet Romani',
    'roml': 'Latvian Romani',
    'romr': 'North Russian Romani',
    'romt': 'Lithuanian Romani'
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
    'inter_letter_delay': 5.0,
    'rotate_user_agent': True,
    'use_session': True
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Referer': 'http://romani.uni-graz.at/romlex/'
}