"""
Audio File Scraper for Nahuatl Dictionary
Scrapes audio node pages and downloads audio files
"""

import requests
import time
import logging
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass, asdict, field as dataclass_field
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Tag
from datetime import datetime
import csv
import re
from urllib.parse import urlparse
import os


@dataclass
class AudioFileData:
    """Audio file metadata"""

    node_id: int
    headword: str = ""
    file_wav: str = ""  # URL
    file_mp3: str = ""  # URL
    file_aif: str = ""  # URL
    speaker: str = ""
    date_recorded: str = ""  # NULL for now
    url_alias: str = ""
    scrape_timestamp: str = ""

    # Download tracking
    local_wav_path: str = ""
    local_mp3_path: str = ""
    local_aif_path: str = ""

    # Status
    scrape_status: str = "success"
    error_message: str = ""


@dataclass
class DownloadLogEntry:
    """Individual file download tracking"""

    node_id: int
    headword: str
    file_url: str
    file_type: str  # wav|mp3|aif
    download_status: str  # success|failed|skipped
    file_size_kb: float = 0.0
    local_path: str = ""
    error_message: str = ""
    timestamp: str = ""


class AudioScraper:
    """Scrape audio nodes and download audio files"""

    def __init__(
        self,
        base_url: str = "https://nahuatl.wired-humanities.org/node",
        delay_seconds: float = 0.5,
        download_delay: float = 0.3,
        timeout: int = 30,
        max_retries: int = 3,
        output_dir: str = "data/interim/scraped",
        audio_dir: str = "data/interim/audio_files",
        download_audio: bool = True,
        download_format: str = "mp3",  # mp3|wav|aif|all
    ):
        self.base_url = base_url
        self.delay_seconds = delay_seconds
        self.download_delay = download_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.output_dir = Path(output_dir)
        self.audio_dir = Path(audio_dir)
        self.download_audio = download_audio
        self.download_format = download_format.lower()

        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.download_audio:
            self.audio_dir.mkdir(parents=True, exist_ok=True)

        self.session = self._setup_session()
        self.logger = self._setup_logger()

        # Progress tracking
        self.checkpoint_file = self.output_dir / "audio_scrape_checkpoint.txt"

        # Statistics
        self.stats = {
            "total_nodes": 0,
            "successful": 0,
            "not_found": 0,
            "not_audio": 0,
            "errors": 0,
            "downloads_attempted": 0,
            "downloads_successful": 0,
            "downloads_failed": 0,
            "total_bytes_downloaded": 0,
        }

        # Tracking lists
        self.download_log: List[DownloadLogEntry] = []
        self.error_log: List[Dict] = []

        # Validate download format
        valid_formats = ["mp3", "wav", "aif", "all"]
        if self.download_format not in valid_formats:
            raise ValueError(
                f"Invalid download_format: {self.download_format}. Must be one of {valid_formats}"
            )

    def _setup_session(self) -> requests.Session:
        """Configure requests session"""
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; NahuatLEX-AudioScraper/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            }
        )
        return session

    def _setup_logger(self) -> logging.Logger:
        """Configure logging"""
        log_file = self.output_dir / "audio_scraper.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        return logging.getLogger(__name__)

    def _extract_field_text(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract text from a field div"""
        field_div = soup.find("div", class_=lambda x: bool(x and field_name in x))
        if not field_div or not isinstance(field_div, Tag):
            return ""

        field_item = field_div.find("div", class_="field-item")
        if not field_item or not isinstance(field_item, Tag):
            return ""

        return field_item.get_text(strip=True)

    def _extract_audio_url(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract audio file URL from field"""
        field_div = soup.find("div", class_=lambda x: bool(x and field_name in x))
        if not field_div or not isinstance(field_div, Tag):
            return ""

        # Look for <source src="..."> tag inside audio player
        source_tag = field_div.find("source")
        if source_tag and isinstance(source_tag, Tag):
            src = source_tag.get("src", "")
            if src:
                return str(src)

        return ""

    def _extract_url_alias(self, soup: BeautifulSoup) -> str:
        """Extract URL alias from page"""
        # Try canonical link
        canonical = soup.find("link", rel="canonical")
        if canonical and isinstance(canonical, Tag):
            href = canonical.get("href", "")
            if href:
                return str(href)

        # Try article about attribute
        article = soup.find("article", class_="node-audio")
        if article and isinstance(article, Tag):
            about = article.get("about", "")
            if about:
                return str(about)

        return ""

    def _sanitize_filename(self, url: str, node_id: int) -> str:
        """Create safe filename from URL"""
        # Extract original filename from URL
        parsed = urlparse(url)
        original_filename = os.path.basename(parsed.path)

        # Prefix with node_id
        sanitized = f"{node_id}_{original_filename}"

        # Remove any problematic characters
        sanitized = re.sub(r"[^\w\-_\.]", "_", sanitized)

        return sanitized

    def _should_download_format(self, file_type: str) -> bool:
        """Check if this format should be downloaded"""
        if not self.download_audio:
            return False

        if self.download_format == "all":
            return True

        return file_type == self.download_format

    def download_audio_file(
        self, url: str, node_id: int, file_type: str, headword: str = ""
    ) -> Tuple[bool, str, str]:
        """
        Download audio file from URL

        Returns:
            (success: bool, local_path: str, error_message: str)
        """
        if not url:
            return False, "", "No URL provided"

        if not self._should_download_format(file_type):
            self.download_log.append(
                DownloadLogEntry(
                    node_id=node_id,
                    headword=headword,
                    file_url=url,
                    file_type=file_type,
                    download_status="skipped",
                    error_message=f"Format {file_type} not selected for download",
                    timestamp=datetime.now().isoformat(),
                )
            )
            return False, "", f"Format {file_type} not selected"

        filename = self._sanitize_filename(url, node_id)
        local_path = self.audio_dir / filename

        # Skip if already exists
        if local_path.exists():
            file_size_kb = local_path.stat().st_size / 1024
            self.logger.info(
                f"  File already exists: {filename} ({file_size_kb:.1f} KB)"
            )

            self.download_log.append(
                DownloadLogEntry(
                    node_id=node_id,
                    headword=headword,
                    file_url=url,
                    file_type=file_type,
                    download_status="already_exists",
                    file_size_kb=file_size_kb,
                    local_path=str(local_path),
                    timestamp=datetime.now().isoformat(),
                )
            )

            return True, str(local_path), ""

        # Download with retries
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout, stream=True)
                response.raise_for_status()

                # Write file
                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Verify file size
                file_size = local_path.stat().st_size
                file_size_kb = file_size / 1024

                if file_size == 0:
                    local_path.unlink()  # Delete empty file
                    raise ValueError("Downloaded file is empty")

                self.logger.info(f"  Downloaded: {filename} ({file_size_kb:.1f} KB)")

                # Log success
                self.download_log.append(
                    DownloadLogEntry(
                        node_id=node_id,
                        headword=headword,
                        file_url=url,
                        file_type=file_type,
                        download_status="success",
                        file_size_kb=file_size_kb,
                        local_path=str(local_path),
                        timestamp=datetime.now().isoformat(),
                    )
                )

                self.stats["downloads_successful"] += 1
                self.stats["total_bytes_downloaded"] += file_size

                return True, str(local_path), ""

            except requests.RequestException as e:
                error_msg = f"Attempt {attempt + 1} failed: {e}"
                self.logger.warning(f"  {error_msg}")

                if attempt < self.max_retries - 1:
                    wait_time = self.download_delay * (
                        2**attempt
                    )  # Exponential backoff
                    time.sleep(wait_time)
                else:
                    # All retries failed - log it
                    self.download_log.append(
                        DownloadLogEntry(
                            node_id=node_id,
                            headword=headword,
                            file_url=url,
                            file_type=file_type,
                            download_status="failed",
                            error_message=str(e),
                            timestamp=datetime.now().isoformat(),
                        )
                    )

                    self.stats["downloads_failed"] += 1
                    return False, "", str(e)

        return False, "", "Max retries exceeded"

    def scrape_audio_node(self, node_id: int) -> AudioFileData:
        """
        Scrape a single audio node

        Args:
            node_id: Audio node ID (e.g., 169642)
        """
        url = f"{self.base_url}/{node_id}"

        audio_data = AudioFileData(
            node_id=node_id, scrape_timestamp=datetime.now().isoformat()
        )

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)

                if response.status_code == 404:
                    audio_data.scrape_status = "not_found"
                    audio_data.error_message = "404 Not Found"
                    self.stats["not_found"] += 1

                    self.error_log.append(
                        {
                            "node_id": node_id,
                            "error_type": "not_found",
                            "error_message": "404 Not Found",
                            "http_status": 404,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

                    return audio_data

                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")

                # Verify it's an audio node
                body = soup.find("body")
                is_audio_node = False
                if body and isinstance(body, Tag):
                    for cls in body.get("class") or []:
                        if "node-type-audio" in cls:
                            is_audio_node = True
                            break

                if not is_audio_node:
                    audio_data.scrape_status = "not_audio"
                    audio_data.error_message = "Not an audio node"
                    self.stats["not_audio"] += 1

                    self.error_log.append(
                        {
                            "node_id": node_id,
                            "error_type": "not_audio",
                            "error_message": "Page is not an audio node",
                            "http_status": 200,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

                    return audio_data

                # Extract metadata
                audio_data.headword = self._extract_field_text(soup, "field-head-idiez")
                audio_data.speaker = self._extract_field_text(soup, "field-speaker")
                audio_data.url_alias = self._extract_url_alias(soup)

                # Extract audio URLs
                audio_data.file_wav = self._extract_audio_url(
                    soup, "field-audio-file-wav"
                )
                audio_data.file_mp3 = self._extract_audio_url(
                    soup, "field-audio-file-mp3"
                )
                audio_data.file_aif = self._extract_audio_url(
                    soup, "field-audio-file-aif"
                )

                # Validate: must have at least one audio URL
                if not any(
                    [audio_data.file_wav, audio_data.file_mp3, audio_data.file_aif]
                ):
                    audio_data.scrape_status = "no_audio_files"
                    audio_data.error_message = "No audio file URLs found"
                    self.stats["errors"] += 1

                    self.error_log.append(
                        {
                            "node_id": node_id,
                            "error_type": "no_audio_files",
                            "error_message": "No audio URLs found on page",
                            "http_status": 200,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

                    return audio_data

                # Download audio files based on format selection
                if self.download_audio:
                    download_results = {}

                    # WAV
                    if audio_data.file_wav and self._should_download_format("wav"):
                        self.stats["downloads_attempted"] += 1
                        success, path, error = self.download_audio_file(
                            audio_data.file_wav, node_id, "wav", audio_data.headword
                        )
                        if success:
                            audio_data.local_wav_path = path
                        download_results["wav"] = success
                        time.sleep(self.download_delay)

                    # MP3
                    if audio_data.file_mp3 and self._should_download_format("mp3"):
                        self.stats["downloads_attempted"] += 1
                        success, path, error = self.download_audio_file(
                            audio_data.file_mp3, node_id, "mp3", audio_data.headword
                        )
                        if success:
                            audio_data.local_mp3_path = path
                        download_results["mp3"] = success
                        time.sleep(self.download_delay)

                    # AIF
                    if audio_data.file_aif and self._should_download_format("aif"):
                        self.stats["downloads_attempted"] += 1
                        success, path, error = self.download_audio_file(
                            audio_data.file_aif, node_id, "aif", audio_data.headword
                        )
                        if success:
                            audio_data.local_aif_path = path
                        download_results["aif"] = success
                        time.sleep(self.download_delay)

                audio_data.scrape_status = "success"
                self.stats["successful"] += 1

                return audio_data

            except requests.RequestException as e:
                self.logger.warning(
                    f"Attempt {attempt + 1} failed for node {node_id}: {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_seconds * (attempt + 1))

        # All retries failed
        audio_data.scrape_status = "error"
        audio_data.error_message = "Max retries exceeded"
        self.stats["errors"] += 1

        self.error_log.append(
            {
                "node_id": node_id,
                "error_type": "connection_error",
                "error_message": "Max retries exceeded",
                "http_status": None,
                "timestamp": datetime.now().isoformat(),
            }
        )

        return audio_data

    def save_checkpoint(self, node_id: int):
        """Save progress checkpoint"""
        with open(self.checkpoint_file, "w") as f:
            f.write(str(node_id))

    def load_checkpoint(self) -> int:
        """Load last completed node ID"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r") as f:
                return int(f.read().strip())
        return -1

    def scrape_from_node_list(
        self, node_file: str, resume: bool = True, limit: Optional[int] = None
    ) -> List[AudioFileData]:
        """
        Scrape audio nodes from file

        Args:
            node_file: Path to file with node IDs (one per line)
            resume: Resume from checkpoint
        """
        # Load node IDs
        with open(node_file, "r", encoding="utf-8") as f:
            node_ids = [
                int(line.strip())
                for line in f
                if line.strip() and not line.startswith("#") and line.strip().isdigit()
            ]
            
        if limit is not None:
            node_ids = node_ids[:limit]  # Add this line
            self.logger.info(f"TESTING MODE: Limited to {limit} nodes")
        # Resume from checkpoint
        start_idx = 0
        last_completed = -1
        if resume:
            last_completed = self.load_checkpoint()
            if last_completed >= 0:
                # Find index of last completed node
                try:
                    start_idx = node_ids.index(last_completed) + 1
                    self.logger.info(
                        f"Resuming from node {last_completed} (index {start_idx})"
                    )
                except ValueError:
                    self.logger.warning(
                        f"Checkpoint node {last_completed} not in list, starting from beginning"
                    )

        total = len(node_ids)
        self.stats["total_nodes"] = total

        self.logger.info("=" * 70)
        self.logger.info("AUDIO NODE SCRAPER")
        self.logger.info("=" * 70)
        self.logger.info(f"Total nodes to scrape: {total:,}")
        self.logger.info(f"Starting from index: {start_idx}")
        self.logger.info(f"Download format: {self.download_format}")
        if self.download_audio:
            self.logger.info(f"Audio files will be saved to: {self.audio_dir}")
        else:
            self.logger.info("Download disabled (metadata only)")
        self.logger.info("=" * 70)

        all_audio_data = []

        for idx, node_id in enumerate(node_ids[start_idx:], start=start_idx):
            self.logger.info(f"Processing node {node_id} ({idx + 1}/{total})...")

            audio_data = self.scrape_audio_node(node_id)
            all_audio_data.append(audio_data)

            # Progress logging
            if (idx + 1) % 50 == 0:
                downloaded_mb = self.stats["total_bytes_downloaded"] / (1024 * 1024)
                self.logger.info("")
                self.logger.info(
                    f"Progress: {idx + 1}/{total} ({(idx + 1)/total*100:.1f}%)"
                )
                self.logger.info(f"  Successful: {self.stats['successful']}")
                self.logger.info(
                    f"  Errors: {self.stats['not_found'] + self.stats['errors']}"
                )
                if self.download_audio:
                    self.logger.info(f"  Downloaded: {downloaded_mb:.1f} MB")
                self.logger.info("")

            # Save checkpoint
            if (idx + 1) % 50 == 0:
                self.save_checkpoint(node_id)
                self.logger.info(f"Checkpoint saved at node {node_id}")

            # Rate limiting
            if idx < total - 1:
                time.sleep(self.delay_seconds)

        # Final checkpoint
        if node_ids:
            self.save_checkpoint(node_ids[-1])

        return all_audio_data

    def save_audio_csv(
        self, audio_data: List[AudioFileData], filename: str = "audio_files.csv"
    ) -> str:
        """Save audio metadata to CSV"""
        filepath = self.output_dir / filename

        df = pd.DataFrame([asdict(data) for data in audio_data])

        # Reorder columns for database import
        column_order = [
            "node_id",
            "headword",
            "file_wav",
            "file_mp3",
            "file_aif",
            "speaker",
            "date_recorded",
            "url_alias",
            "local_wav_path",
            "local_mp3_path",
            "local_aif_path",
            "scrape_timestamp",
            "scrape_status",
            "error_message",
        ]

        df = df[column_order]
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        self.logger.info(f"\nSaved {len(audio_data)} audio entries to {filepath}")

        return str(filepath)

    def save_download_log(self, filename: str = "audio_download_log.csv") -> str:
        """Save download log"""
        if not self.download_log:
            return ""

        filepath = self.output_dir / filename

        df = pd.DataFrame([asdict(entry) for entry in self.download_log])
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        self.logger.info(f"Saved download log to {filepath}")

        return str(filepath)

    def save_error_log(self, filename: str = "audio_scrape_errors.csv") -> str:
        """Save error log"""
        if not self.error_log:
            return ""

        filepath = self.output_dir / filename

        df = pd.DataFrame(self.error_log)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        self.logger.info(f"Saved error log to {filepath}")

        return str(filepath)

    def print_statistics(self):
        """Print final statistics"""
        downloaded_mb = self.stats["total_bytes_downloaded"] / (1024 * 1024)

        self.logger.info("\n" + "=" * 70)
        self.logger.info("SCRAPING COMPLETE")
        self.logger.info("=" * 70)
        self.logger.info(f"Total nodes processed: {self.stats['total_nodes']:,}")
        self.logger.info(f"Successful: {self.stats['successful']:,}")
        self.logger.info(f"Not found (404): {self.stats['not_found']:,}")
        self.logger.info(f"Not audio nodes: {self.stats['not_audio']:,}")
        self.logger.info(f"Other errors: {self.stats['errors']:,}")

        if self.download_audio:
            self.logger.info(f"\nDownload Statistics:")
            self.logger.info(f"  Format: {self.download_format}")
            self.logger.info(f"  Attempted: {self.stats['downloads_attempted']:,}")
            self.logger.info(f"  Successful: {self.stats['downloads_successful']:,}")
            self.logger.info(f"  Failed: {self.stats['downloads_failed']:,}")
            self.logger.info(f"  Total downloaded: {downloaded_mb:.1f} MB")
            self.logger.info(f"  Saved to: {self.audio_dir}")

        self.logger.info(f"\nOutput files saved to: {self.output_dir}")
        self.logger.info("=" * 70)


def main():
    """Run audio scraper"""
    import argparse

    parser = argparse.ArgumentParser(description="Scrape audio node files")
    parser.add_argument(
        "--node-file",
        default="audio_nodes_to_scrape.txt",
        help="File containing audio node IDs (one per line)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/interim/scraped",
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--audio-dir",
        default="data/interim/audio_files",
        help="Directory to save downloaded audio files",
    )
    parser.add_argument(
        "--no-download", action="store_true", help="Skip file downloads (metadata only)"
    )
    parser.add_argument(
        "--download-format",
        choices=["mp3", "wav", "aif", "all"],
        default="mp3",
        help="Audio format to download (default: mp3)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.5, help="Delay between requests (seconds)"
    )
    parser.add_argument(
        "--download-delay",
        type=float,
        default=0.3,
        help="Delay between file downloads (seconds)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start from beginning (ignore checkpoint)",
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of nodes to scrape (for testing)"
    )

    args = parser.parse_args()

    # Create scraper
    scraper = AudioScraper(
        delay_seconds=args.delay,
        download_delay=args.download_delay,
        output_dir=args.output_dir,
        audio_dir=args.audio_dir,
        download_audio=not args.no_download,
        download_format=args.download_format,
    )

    # Run scraping
    audio_data = scraper.scrape_from_node_list(
        node_file=args.node_file,
        resume=not args.no_resume,
        limit=args.limit
    )

    # Save results
    scraper.save_audio_csv(audio_data)
    scraper.save_download_log()
    scraper.save_error_log()
    scraper.print_statistics()


if __name__ == "__main__":
    main()
