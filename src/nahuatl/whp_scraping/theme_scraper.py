"""
Theme Scraper for Nahuatl Dictionary
Scrapes theme/taxonomy pages to populate themes table
"""

import requests
import time
import logging
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import csv
import re


@dataclass
class ThemeData:
    """Theme/taxonomy term data"""

    tid: Optional[int]  # Taxonomy term ID
    slug: str  # URL slug (e.g., "water")
    name: str  # Display name (e.g., "Water")
    description: str = ""  # Theme description
    vocabulary_id: int = 1  # Default vocabulary ID
    entry_count: int = 0  # Number of entries with this theme
    url_alias: str = ""  # Full URL path
    scrape_timestamp: str = ""
    scrape_status: str = "success"
    error_message: str = ""


class ThemeScraper:
    """Scrape theme pages from the website"""

    def __init__(
        self,
        base_url: str = "https://nahuatl.wired-humanities.org/themes",
        delay_seconds: float = 0.5,
        timeout: int = 30,
        max_retries: int = 3,
        output_dir: str = "data/interim/scraped",
    ):
        self.base_url = base_url
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = self._setup_session()
        self.logger = self._setup_logger()

        # Progress tracking
        self.checkpoint_file = self.output_dir / "theme_scrape_checkpoint.txt"

    def _setup_session(self) -> requests.Session:
        """Configure requests session"""
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; NahuatLEX-ThemeScraper/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            }
        )
        return session

    def _setup_logger(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(__name__)

    def _extract_tid_from_page(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract taxonomy term ID from page

        Methods:
        1. Body class: page-taxonomy-term-{tid}
        2. Term div: taxonomy-term-{tid}
        3. Shortlink meta tag
        """
        # Try body class
        body = soup.find("body")
        if isinstance(body, Tag):
            if body and body.get("class"):
                for cls in body.get("class") or []:
                    if cls.startswith("page-taxonomy-term-"):
                        try:
                            return int(cls.replace("page-taxonomy-term-", ""))
                        except ValueError:
                            pass

        # Try term div
        term_div = soup.find("div", class_=lambda x: bool(x and "taxonomy-term-" in x))
        if isinstance(term_div, Tag):
            if term_div and term_div.get("class"):
                for cls in term_div.get("class") or []:
                    if cls.startswith("taxonomy-term-"):
                        try:
                            return int(cls.replace("taxonomy-term-", ""))
                        except ValueError:
                            pass

        # Try shortlink
        shortlink = soup.find("link", rel="shortlink")
        if shortlink and isinstance(shortlink, Tag):
            href = str(shortlink.get("href", ""))
            if "/taxonomy/term/" in href:
                try:
                    tid = href.split("/taxonomy/term/")[-1]
                    return int(tid)
                except ValueError:
                    pass

        return None

    def _extract_theme_description(self, soup: BeautifulSoup) -> str:
        """Extract theme description from page"""
        # Try field-description
        desc_div = soup.find(
            "div", class_=lambda x: bool(x and "field-description" in x)
        )
        if desc_div and isinstance(desc_div, Tag):
            field_item = desc_div.find("div", class_="field-item")
            if field_item:
                return field_item.get_text(strip=True)

        # Try description meta tag
        desc_meta = soup.find("meta", attrs={"name": "description"})
        if desc_meta and isinstance(desc_meta, Tag):
            return str(desc_meta.get("content", "")).strip()

        return ""

    def _extract_entry_count(self, soup: BeautifulSoup) -> int:
        """Extract number of entries for this theme"""
        # Look for view header or pager info
        view_header = soup.find("div", class_="view-header")
        if view_header:
            text = view_header.get_text()
            # Try to find "X entries" or similar
            match = re.search(r"(\d+)\s+(?:entries|items|results)", text, re.IGNORECASE)
            if match:
                return int(match.group(1))

        # Count table rows (less reliable)
        rows = soup.select("table.views-table tbody tr")
        if rows:
            # This might be paginated, so not accurate
            return len(rows)

        return 0

    def scrape_theme(self, slug: str) -> ThemeData:
        """
        Scrape a single theme page

        Args:
            slug: Theme slug (e.g., "water", "agriculture-gardens-stockraising")
        """
        url = f"{self.base_url}/{slug}"

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)

                if response.status_code == 404:
                    return ThemeData(
                        tid=None,
                        slug=slug,
                        name=slug.replace("-", " ").title(),
                        scrape_status="not_found",
                        error_message="404 Not Found",
                    )

                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")

                # Extract theme data
                tid = self._extract_tid_from_page(soup)

                # Get theme name from page title or h1
                name = ""
                title_tag = soup.find("h1", class_="page-title")
                if title_tag:
                    name = title_tag.get_text(strip=True)
                else:
                    # Fallback to title tag
                    title_meta = soup.find("title")
                    if title_meta:
                        name = title_meta.get_text(strip=True).split("|")[0].strip()

                if not name:
                    name = slug.replace("-", " ").title()

                description = self._extract_theme_description(soup)
                entry_count = self._extract_entry_count(soup)

                return ThemeData(
                    tid=tid,
                    slug=slug,
                    name=name,
                    description=description,
                    entry_count=entry_count,
                    url_alias=f"/themes/{slug}",
                    scrape_timestamp=datetime.now().isoformat(),
                    scrape_status="success",
                )

            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {slug}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_seconds * (attempt + 1))

        # All retries failed
        return ThemeData(
            tid=None,
            slug=slug,
            name=slug.replace("-", " ").title(),
            scrape_status="error",
            error_message="Max retries exceeded",
        )

    def scrape_themes_from_file(
        self, slug_file: str, resume: bool = True
    ) -> List[ThemeData]:
        """
        Scrape themes from a file containing slugs

        Args:
            slug_file: Path to file with one slug per line
            resume: Resume from checkpoint
        """
        # Load slugs from file
        with open(slug_file, "r", encoding="utf-8") as f:
            slugs = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]

        # Resume from checkpoint
        start_idx = 0
        if resume and self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r") as f:
                start_idx = int(f.read().strip()) + 1
                self.logger.info(f"Resuming from index {start_idx}")

        total = len(slugs)
        self.logger.info(
            f"Scraping {total - start_idx} themes (indices {start_idx}-{total})"
        )

        all_themes = []

        for idx, slug in enumerate(slugs[start_idx:], start=start_idx):
            theme_data = self.scrape_theme(slug)
            all_themes.append(theme_data)

            # Progress logging
            if (idx + 1) % 10 == 0:
                self.logger.info(
                    f"Progress: {idx + 1}/{total} themes "
                    f"({(idx + 1)/total*100:.1f}%)"
                )

            # Save checkpoint
            if (idx + 1) % 10 == 0:
                with open(self.checkpoint_file, "w") as f:
                    f.write(str(idx))

            # Rate limiting
            if idx < total - 1:
                time.sleep(self.delay_seconds)

        # Final checkpoint
        with open(self.checkpoint_file, "w") as f:
            f.write(str(total - 1))

        return all_themes

    def save_themes_csv(
        self, themes: List[ThemeData], filename: str = "themes.csv"
    ) -> str:
        """Save themes to CSV"""
        filepath = self.output_dir / filename

        df = pd.DataFrame([asdict(theme) for theme in themes])
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        self.logger.info(f"Saved {len(themes)} themes to {filepath}")

        # Print statistics
        successful = len(df[df["scrape_status"] == "success"])
        not_found = len(df[df["scrape_status"] == "not_found"])
        errors = len(df[df["scrape_status"] == "error"])

        self.logger.info("\n" + "=" * 70)
        self.logger.info("THEME SCRAPING SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Total themes: {len(themes)}")
        self.logger.info(f"Successful: {successful}")
        self.logger.info(f"Not found: {not_found}")
        self.logger.info(f"Errors: {errors}")
        self.logger.info(f"Themes with TID: {df['tid'].notna().sum()}")
        self.logger.info(f"Total entries across themes: {df['entry_count'].sum()}")

        return str(filepath)


def main():
    """Run theme scraper"""
    import argparse

    parser = argparse.ArgumentParser(description="Scrape theme pages")
    parser.add_argument(
        "--slug-file",
        default="theme_slugs_to_scrape.txt",
        help="File containing theme slugs (one per line)",
    )
    parser.add_argument(
        "--output-dir", default="data/interim/scraped", help="Output directory for CSV"
    )
    parser.add_argument("--no-resume", action="store_true", help="Start from beginning")
    parser.add_argument(
        "--delay", type=float, default=0.5, help="Delay between requests (seconds)"
    )

    args = parser.parse_args()

    scraper = ThemeScraper(delay_seconds=args.delay, output_dir=args.output_dir)

    themes = scraper.scrape_themes_from_file(
        slug_file=args.slug_file, resume=not args.no_resume
    )

    scraper.save_themes_csv(themes)


if __name__ == "__main__":
    main()
