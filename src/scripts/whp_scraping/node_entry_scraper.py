"""
Node Entry Scraper for Nahuatl Dictionary
Scrapes individual dictionary entries by node ID
Outputs data in normalized format for SQLite import
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Tuple, cast
from dataclasses import dataclass, field
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Tag
from datetime import datetime
import csv


@dataclass
class DictionaryEntryData:
    """Main dictionary entry data"""
    node_id: int
    headword: str = ""
    orthographic_variants: str = ""
    ipa_spelling: str = ""
    translation_english: str = ""
    spanish_loanword: str = ""
    headword_idiez: str = ""
    translation_english_idiez: str = ""
    definition_nahuatl_idiez: str = ""
    definition_spanish_idiez: str = ""
    morfologia_idiez: str = ""
    gramatica_idiez: str = ""
    source_dataset: str = ""
    url_alias: str = ""
    created_timestamp: str = ""
    scrape_timestamp: str = ""


@dataclass
class AuthorityCitation:
    """Authority citation data"""
    node_id: int
    authority_name: str
    citation_text: str
    citation_order: int = 0


@dataclass
class Attestation:
    """Attestation/example data"""
    node_id: int
    language: str
    attestation_text: str
    source_field: str


@dataclass
class ThemeReference:
    """Theme relationship"""
    entry_node_id: int
    theme_slug: str
    theme_name: str
    delta: int = 0


@dataclass
class AudioReference:
    """Audio file relationship"""
    entry_node_id: int
    audio_node_id: int
    reference_type: str
    delta: int = 0


@dataclass
class ScrapedNodeData:
    """Complete scraped data for a node"""
    entry: DictionaryEntryData
    citations: List[AuthorityCitation] = field(default_factory=list)
    attestations: List[Attestation] = field(default_factory=list)
    themes: List[ThemeReference] = field(default_factory=list)
    audio_refs: List[AudioReference] = field(default_factory=list)
    scrape_status: str = "success"
    error_message: str = ""


class NodeEntryScraper:
    """Scrape dictionary entries by node ID"""
    
    # Field name mappings
    AUTHORITY_FIELDS = {
        'field-authority1': 'Molina',
        'field-authority2': 'Karttunen',
        'field-authority3': 'Carochi',
        'field-authority4': 'Olmos',
        'field-authority6': 'Lockhart'
    }
    
    def __init__(
        self,
        delay_seconds: float = 0.5,
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = self._setup_session()
        self.logger = self._setup_logger()
    
    def _setup_session(self) -> requests.Session:
        """Configure requests session"""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; NahuatLEX-NodeScraper/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        })
        return session
    
    def _setup_logger(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _extract_field_text(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract text from a field div"""
        # Ensure lambda returns strictly bool
        field_div = soup.find("div", class_=lambda x: bool(x and field_name in x))
        if not field_div:
            return ""

        # Cast to Tag so Pylance knows .find() exists
        field_div = cast(Tag, field_div)
        field_item = field_div.find("div", class_="field-item")
        if not field_item:
            return ""

        return field_item.get_text(strip=True)
    
    def _extract_field_html(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract HTML content from a field div"""
        field_div = soup.find("div", class_=lambda x: bool(x and field_name in x))
        if not field_div:
            return ""
        
        field_div = cast(Tag, field_div)
        field_item = field_div.find('div', class_='field-item')
        if not field_item:
            return ""
        
        return str(field_item)
    
    def _parse_main_entry_fields(self, soup: BeautifulSoup, node_id: int) -> DictionaryEntryData:
        """Parse main dictionary entry fields"""
        entry = DictionaryEntryData(
            node_id=node_id,
            scrape_timestamp=datetime.now().isoformat()
        )
        
        # Standard WHP fields
        entry.headword = self._extract_field_text(soup, 'field-wordorparticle')
        entry.orthographic_variants = self._extract_field_text(soup, 'field-variants')
        entry.ipa_spelling = self._extract_field_text(soup, 'field-ipaspelling')
        entry.translation_english = self._extract_field_html(soup, 'field-translation1')
        entry.spanish_loanword = self._extract_field_text(soup, 'field-spanish-loanword')
        
        # IDIEZ fields
        entry.headword_idiez = self._extract_field_text(soup, 'field-head-idiez')
        entry.translation_english_idiez = self._extract_field_text(soup, 'field-eshort-idiez')
        entry.definition_nahuatl_idiez = self._extract_field_text(soup, 'field-ndef-idiez')
        entry.definition_spanish_idiez = self._extract_field_text(soup, 'field-sdef-idiez')
        entry.morfologia_idiez = self._extract_field_text(soup, 'field-morf1-idiez')
        entry.gramatica_idiez = self._extract_field_text(soup, 'field-gramn-idiez')
        
        # Determine source dataset
        has_whp = bool(entry.headword)
        has_idiez = bool(entry.headword_idiez)
        
        if has_whp and has_idiez:
            entry.source_dataset = 'HYBRID'
        elif has_idiez:
            entry.source_dataset = 'IDIEZ'
        elif has_whp:
            entry.source_dataset = 'WHP'
        else:
            entry.source_dataset = 'UNKNOWN'
        
        # Extract URL alias from about attribute
        main_div = soup.find('div', class_='node-dictionary-entry')
        if main_div:
            main_div = cast(Tag, main_div)
            entry.url_alias = str(main_div.get("about") or "")
        
        return entry
    
    def _parse_authority_citations(self, soup: BeautifulSoup, node_id: int) -> List[AuthorityCitation]:
        """Parse all authority citation fields"""
        citations = []
        
        for field_class, authority_name in self.AUTHORITY_FIELDS.items():
            field_div = soup.find('div', class_=lambda x: bool(x and field_class in x))
            if not field_div:
                continue
            field_div = cast(Tag, field_div)
            field_item = field_div.find('div', class_='field-item')
            if not field_item:
                continue
            
            citation_html = str(field_item)
            if citation_html.strip():
                citations.append(AuthorityCitation(
                    node_id=node_id,
                    authority_name=authority_name,
                    citation_text=citation_html,
                    citation_order=0
                ))
        
        return citations
    
    def _parse_attestations(self, soup: BeautifulSoup, node_id: int) -> List[Attestation]:
        """Parse attestation fields"""
        attestations = []
        
        # English attestations
        eng_field = soup.find('div', class_=lambda x: bool(x and 'field-additionalnotes-lang1' in x))
        if eng_field:
            eng_field = cast(Tag, eng_field)
            field_item = eng_field.find('div', class_='field-item')
            if field_item:
                text = str(field_item)
                if text.strip():
                    attestations.append(Attestation(
                        node_id=node_id,
                        language='English',
                        attestation_text=text,
                        source_field='field_additionalnotes_lang1'
                    ))
        
        # Spanish attestations
        spa_field = soup.find('div', class_=lambda x: bool(x and 'field-additionalnotes-lang2' in x))
        if spa_field:
            spa_field = cast(Tag, spa_field)
            field_item = spa_field.find('div', class_='field-item')
            if field_item:
                text = str(field_item)
                if text.strip():
                    attestations.append(Attestation(
                        node_id=node_id,
                        language='Spanish',
                        attestation_text=text,
                        source_field='field_additionalnotes_lang2'
                    ))
        
        return attestations
    
    def _parse_themes(self, soup: BeautifulSoup, node_id: int) -> List[ThemeReference]:
        """Parse theme references"""
        themes = []
        
        themes_div = soup.find('div', class_=lambda x: bool(x and 'field-themes' in x))
        if not isinstance(themes_div, Tag):
            return themes
        
        theme_links = themes_div.find_all('a')
        for idx, link in enumerate(theme_links):
            if not isinstance(link, Tag):
                continue 
            theme_url = str(link.get('href') or '')
            theme_name = link.get_text(strip=True)
            
            # Extract slug from URL like /themes/water
            theme_slug = theme_url.split('/')[-1] if theme_url else ''
            
            if theme_slug:
                themes.append(ThemeReference(
                    entry_node_id=node_id,
                    theme_slug=theme_slug,
                    theme_name=theme_name,
                    delta=idx
                ))
        
        return themes
    
    def _parse_audio_references(self, soup: BeautifulSoup, node_id: int) -> List[AudioReference]:
        """Parse audio file references"""
        audio_refs = []
        
        # Audio for headword
        headword_field = soup.find('div', class_=lambda x: bool(x and 'field-audio-headword' in x))
        if isinstance(headword_field, Tag):
            articles = headword_field.find_all('article', class_='node-audio')
            for idx, article in enumerate(articles):
                if not isinstance(article, Tag):
                    continue
                audio_node_id = str(article.get('id') or "").replace('node-', '')
                if audio_node_id.isdigit():
                    audio_refs.append(AudioReference(
                        entry_node_id=node_id,
                        audio_node_id=int(audio_node_id),
                        reference_type='headword',
                        delta=idx
                    ))

        examples_field = soup.find('div', class_=lambda x: bool(x and 'field-audio-examples-in-context' in x))
        if isinstance(examples_field, Tag):
            articles = examples_field.find_all('article', class_='node-audio')
            for idx, article in enumerate(articles):
                if not isinstance(article, Tag):
                    continue
                audio_node_id = str(article.get('id') or "").replace('node-', '')
                if audio_node_id.isdigit():
                    audio_refs.append(AudioReference(
                        entry_node_id=node_id,
                        audio_node_id=int(audio_node_id),
                        reference_type='example',
                        delta=idx
                    ))
        
        return audio_refs
    
    def scrape_node(
        self, 
        node_id: Optional[int] = None, 
        url_alias: Optional[str] = None
    ) -> ScrapedNodeData:
        """
        Scrape a single dictionary entry node
        
        Args:
            node_id: Node ID to scrape (e.g., 173259)
            url_alias: URL alias to scrape (e.g., "/content/ach-0")
            
        Note: Must provide either node_id OR url_alias
        """
        if node_id is None and url_alias is None:
            raise ValueError("Must provide either node_id or url_alias")
        
        # Construct URL based on what we have
        if node_id is not None:
            url = f"https://nahuatl.wired-humanities.org/node/{node_id}"
            identifier = node_id
        else:
            url = f"https://nahuatl.wired-humanities.org{url_alias}"
            identifier = url_alias
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 404:
                    return ScrapedNodeData(
                        entry=DictionaryEntryData(node_id=node_id or 0),
                        scrape_status="not_found"
                    )
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if it's a dictionary entry
                if not soup.find('div', class_='node-dictionary-entry'):
                    return ScrapedNodeData(
                        entry=DictionaryEntryData(node_id=node_id or 0),
                        scrape_status="not_dictionary_entry"
                    )
                
                # Extract actual node_id from page if we only had url_alias
                if node_id is None:
                    node_id = self._extract_node_id_from_page(soup)
                    if node_id is None:
                        self.logger.warning(f"Could not extract node_id from page: {url_alias}")
                        node_id = 0  # Placeholder
                
                # Parse all components
                entry = self._parse_main_entry_fields(soup, node_id)
                citations = self._parse_authority_citations(soup, node_id)
                attestations = self._parse_attestations(soup, node_id)
                themes = self._parse_themes(soup, node_id)
                audio_refs = self._parse_audio_references(soup, node_id)
                
                return ScrapedNodeData(
                    entry=entry,
                    citations=citations,
                    attestations=attestations,
                    themes=themes,
                    audio_refs=audio_refs,
                    scrape_status="success"
                )
                
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {identifier}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_seconds * (attempt + 1))
        
        # All retries failed
        return ScrapedNodeData(
            entry=DictionaryEntryData(node_id=node_id or 0),
            scrape_status="error",
            error_message="Max retries exceeded"
        )
        
    def _extract_node_id_from_page(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract node_id from shortlink meta tag or body class
        
        Args:
            soup: Parsed HTML
            
        Returns:
            Node ID or None if not found
        """
        # Try shortlink meta tag: <link rel="shortlink" href="/node/173259" />
        shortlink = soup.find('link', rel='shortlink')
        if isinstance(shortlink, Tag):
            href = str(shortlink.get('href') or "")
            if href.startswith('/node/'):
                ...
                
        body = soup.find('body')
        if isinstance(body, Tag):
            classes = body.get('class') or []
            for cls in classes:
                if cls.startswith('page-node-') and not cls == 'page-node-':
                    try:
                        return int(cls.replace('page-node-', ''))
                    except ValueError:
                        pass
        
        return None
    
    def scrape_nodes_from_inventory(
        self,
        inventory_path: str,
        start_index: int = 0,
        end_index: Optional[int] = None,
        checkpoint_interval: int = 100
    ) -> List[ScrapedNodeData]:
        """
        Scrape nodes from inventory CSV
        
        Handles both node_id and url_alias entries
        """
        # Load inventory
        df = pd.read_csv(inventory_path)
        
        if end_index is None:
            end_index = len(df)
        
        df_subset = df.iloc[start_index:end_index]
        total = len(df_subset)
        
        self.logger.info(f"Scraping {total} nodes (indices {start_index}-{end_index})")
        
        all_scraped_data = []
        
        for idx, row in df_subset.iterrows():
            # Use node_id if available, otherwise use url_alias
            node_id = row['node_id'] if pd.notna(row['node_id']) else None
            url_alias = row['url_alias'] if pd.notna(row['url_alias']) else None
            
            if node_id is not None:
                scraped = self.scrape_node(node_id=int(node_id))
            elif url_alias:
                scraped = self.scrape_node(url_alias=url_alias)
            else:
                self.logger.error(f"Row {idx}: No node_id or url_alias available")
                continue
            
            all_scraped_data.append(scraped)
            
            # Progress logging
            progress = len(all_scraped_data)
            if progress % 50 == 0:
                self.logger.info(
                    f"Progress: {progress}/{total} nodes "
                    f"({progress/total*100:.1f}%)"
                )
            
            # Rate limiting
            if progress < total:
                time.sleep(self.delay_seconds)
        
        return all_scraped_data


def main():
    """Test scraper with a few nodes"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape dictionary entry nodes')
    parser.add_argument(
        '--inventory',
        default='data/interim/node_inventory.csv',
        help='Path to node inventory CSV'
    )
    parser.add_argument(
        '--start',
        type=int,
        default=0,
        help='Start index'
    )
    parser.add_argument(
        '--end',
        type=int,
        help='End index (default: all)'
    )
    parser.add_argument(
        '--test-node',
        type=int,
        help='Test scraping a single node ID'
    )
    
    args = parser.parse_args()
    
    scraper = NodeEntryScraper()
    
    if args.test_node:
        # Test single node
        result = scraper.scrape_node(args.test_node)
        print(f"\nNode {args.test_node}:")
        print(f"Status: {result.scrape_status}")
        print(f"Headword: {result.entry.headword}")
        print(f"Headword IDIEZ: {result.entry.headword_idiez}")
        print(f"Source: {result.entry.source_dataset}")
        print(f"Citations: {len(result.citations)}")
        print(f"Themes: {len(result.themes)}")
        print(f"Audio refs: {len(result.audio_refs)}")
    else:
        # Scrape from inventory
        results = scraper.scrape_nodes_from_inventory(
            inventory_path=args.inventory,
            start_index=args.start,
            end_index=args.end
        )
        print(f"\nScraped {len(results)} nodes")


if __name__ == "__main__":
    main()