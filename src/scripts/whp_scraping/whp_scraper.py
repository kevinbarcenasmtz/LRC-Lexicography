"""
WHP Nahuatl Website Scraper
Scrapes entries from nahuatl.wired-humanities.org to identify gaps in existing dataset
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import yaml


@dataclass
class ScrapingConfig:
    """Configuration for scraping parameters"""

    base_url: str = "https://nahuatl.wired-humanities.org/content/"
    delay_seconds: float = 1.5
    timeout: int = 30
    max_retries: int = 3
    output_dir: str = "../../../data/interim"
    user_agent: str = "Mozilla/5.0 (compatible; NahuatLEX-Research/1.0)"


@dataclass
class WHPEntry:
    """Data structure for a WHP website entry."""

    headword: str
    url: str
    orthographic_variants: str = ""
    ipa_spelling: str = ""
    principal_translation: str = ""
    scrape_status: str = "success"
    scrape_timestamp: str = ""
    error_message: str = ""


@dataclass
class WHPScraper:
    """Scraper for Wired HUmanities Project Nahuatl website."""

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.config = config or ScrapingConfig()
        self.session = self.setup_session()
        self.logger = self.setup_logger()

    def setup_session(self) -> requests.Session:
        """Configure requests session with headers and timeouts"""
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )
        return session

    def setup_logger(self) -> logging.Logger:
        """Configure logging for the scraper"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        return logging.getLogger(__name__)

    def _build_url(self, headword: str) -> str:
        """Build URL for a specific headword"""
        # Clean headword: remove periods, normalize spacing
        clean_headword = headword.strip().rstrip(".")
        return f"{self.config.base_url}{clean_headword}"

    def _has_field_class(self, css_class: str, field_name: str) -> bool:
        """Check if CSS class contains the field name"""
        return css_class is not None and f"field-{field_name}" in css_class

    def _extract_field_value(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract field value by field name from WHP page structure"""
        try:
            # Look for field with specific class pattern
            field_div = soup.find(
                "div", class_=lambda x: self._has_field_class(x, field_name)
            )

            if field_div is None:
                return ""

            items_div = field_div.find("div", class_="field-items")
            if items_div is None:
                return ""

            item_div = items_div.find("div", class_="field-item")
            if item_div is None:
                return ""

            return item_div.get_text(strip=True)

        except AttributeError as e:
            self.logger.warning("Error extracting field %s: %s", field_name, e)
            return ""

    def _parse_entry_page(self, html: str, url: str) -> WHPEntry:
        """Parse WHP entry page and extract all relevant fields"""
        soup = BeautifulSoup(html, "html.parser")

        # Extract headword from URL
        headword = url.split("/")[-1]

        entry = WHPEntry(
            headword=headword, url=url, scrape_timestamp=datetime.now().isoformat()
        )

        # Field mapping: WHP field name → entry attribute
        field_mappings = {
            "variants": "orthographic_variants",
            "ipaspelling": "ipa_spelling",
            # Add other field mappings as we discover them
        }

        # Extract standard fields
        for whp_field, entry_attr in field_mappings.items():
            value = self._extract_field_value(soup, whp_field)
            setattr(entry, entry_attr, value)

        # Extract more complex fields (those with HTML content)

        return entry

    def scrape_entry(self, headword: str) -> WHPEntry:
        """Scrape a single entry from WHP website"""
        url = self._build_url(headword)

        self.logger.info("Scraping entry: {headword - %s} -> {url - %s}", headword, url)

        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, timeout=self.config.timeout)

                if response.status_code == 200:
                    entry = self._parse_entry_page(response.text, url)
                    entry.scrape_status = "success"
                    self.logger.info("Successfully scraped: %s", headword)
                    return entry

                elif response.status_code == 404:
                    self.logger.warning("Entry not found: %s", headword)
                    entry = WHPEntry(
                        headword=headword,
                        url=url,
                        scrape_status="not_found",
                        scrape_timestamp=datetime.now().isoformat(),
                    )
                    return entry

                else:
                    self.logger.warning(f"HTTP {response.status_code} for {headword}")

            except requests.RequestException as e:
                self.logger.error(
                    f"Request failed for {headword} (attempt {attempt + 1}): {e}"
                )

            # Wait before retry
            if attempt < self.config.max_retries - 1:
                time.sleep(self.config.delay_seconds * (attempt + 1))

        # All attempts failed
        entry = WHPEntry(
            headword=headword,
            url=url,
            scrape_status="error",
            error_message="Max retries exceeded",
            scrape_timestamp=datetime.now().isoformat(),
        )
        return entry

    def scrape_entries(self, headwords: List[str]) -> List[WHPEntry]:
        """Scrape multiple entries with rate limiting"""
        entries = []

        self.logger.info(f"Starting scrape of {len(headwords)} entries")

        for i, headword in enumerate(headwords):
            entry = self.scrape_entry(headword)
            entries.append(entry)

            # Rate limiting
            if i < len(headwords) - 1:  # Don't wait after last entry
                time.sleep(self.config.delay_seconds)

            # Progress logging
            if (i + 1) % 10 == 0:
                self.logger.info(f"Progress: {i + 1}/{len(headwords)} entries scraped")

        return entries

    def save_results(
        self, entries: List[WHPEntry], filename: Optional[str] = None
    ) -> str:
        """Save scraped entries to Excel file with multiple sheets"""
        if not entries:
            raise ValueError("No entries to save")

        # Prepare output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"whp_scrape_results_{timestamp}.xlsx"

        filepath = output_dir / filename

        # Convert entries to DataFrames
        all_entries_data = [entry.__dict__ for entry in entries]
        all_df = pd.DataFrame(all_entries_data)

        # Separate by status
        successful_df = all_df[all_df["scrape_status"] == "success"].copy()
        not_found_df = all_df[all_df["scrape_status"] == "not_found"].copy()
        error_df = all_df[all_df["scrape_status"] == "error"].copy()

        # Save to Excel with multiple sheets
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            all_df.to_excel(writer, sheet_name="All_Results", index=False)

            if not successful_df.empty:
                successful_df.to_excel(
                    writer, sheet_name="Successful_Scrapes", index=False
                )

            if not not_found_df.empty:
                not_found_df.to_excel(writer, sheet_name="Not_Found", index=False)

            if not error_df.empty:
                error_df.to_excel(writer, sheet_name="Errors", index=False)

        self.logger.info(f"Results saved to: {filepath}")
        self.logger.info(
            f"Summary: {len(successful_df)} successful, "
            f"{len(not_found_df)} not found, {len(error_df)} errors"
        )

        return str(filepath)

    def save_not_found_entries(
        self, entries: List[WHPEntry], filename: Optional[str] = None
    ) -> Optional[str]:
        """Save list of not-found headwords to a text file for easy review"""
        not_found = [e for e in entries if e.scrape_status == "not_found"]
        
        if not not_found:
            self.logger.info("No not-found entries to save")
            return None
        
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"whp_not_found_{timestamp}.txt"
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Not Found Entries - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(not_found)}\n\n")
            for entry in not_found:
                f.write(f"{entry.headword}\n")
        
        self.logger.info(f"Not-found entries saved to: {filepath}")
        return str(filepath)

def main():
    """Test the scraper with the ayac example"""
    scraper = WHPScraper()

    headword = ["ayac"]
    try:
        entries = scraper.scrape_entries(headword)
        filepath = scraper.save_results(entries)

        # Print results for verification
        for entry in entries:
            print(f"\n=== Results for {entry.headword} ===")
            print(f"Status: {entry.scrape_status}")
            print(f"Orthographic Variants: {entry.orthographic_variants}")
            print(f"IPA Spelling: {entry.ipa_spelling}")
            print(f"URL: {entry.url}")
    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
