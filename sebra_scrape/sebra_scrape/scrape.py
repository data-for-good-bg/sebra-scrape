"""Core scraping functionality for minfin.bg XLS files."""

import logging
import os
from html.parser import HTMLParser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

from .zyte import ZyteApi


logger = logging.getLogger(__name__)


@dataclass
class ScrapeDownloadResult:
    """
    Structure describing the result of a scrape operation.
    * files is a list of filepaths to successfully downloaded files.
    * failed_fils is an optional path to a file containing URLs of attempted
      but not downloaded URLs (even after retries)
    * no_files_found_file is an optional path to a file containing the
      timestamp when a scrape operation was executed for a date_str but no
      files were found (scraped) in the page
    """
    files: list[str]
    failed_file: Optional[str]
    no_files_found_file: Optional[str]


class XLSLinkParser(HTMLParser):
    """HTMLParser to extract XLS links from the page."""

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.xls_links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href' and value:
                    # Check if it's an XLS file (case insensitive)
                    if value.lower().endswith('.xls') or value.lower().endswith('.xlsx'):
                        # Make it an absolute URL
                        absolute_url = urljoin(self.base_url, value)
                        self.xls_links.append(absolute_url)


class MinfinScraper:
    """
    Scraper for minfin.bg that:
    - Uses cloudscraper to get initial session cookies
    - Uses requests.Session for subsequent requests with those cookies
    - Parses HTML with stdlib htmlparser
    - Finds and downloads XLS files
    """

    def __init__(self):
        """Initialize the scraper."""
        pass

    def _build_date_path(self, date_str: str) -> str:
        """
        Build the path for a given date.

        Args:
            date_str: Date in YYYY-MM-DD format

        Returns:
            Full URL for the transparency page
        """
        return f'bg/transparency/{date_str}'

    def scrape_xls_links(self, zyte: ZyteApi, url_path: str, target_dir: str) -> list[str]:
        """
        Scrape XLS links from the given URL.

        Args:
            url: URL to scrape

        Returns:
            List of absolute URLs to XLS files
        """

        html = zyte.get_html_with_retries(url_path)

        parser = XLSLinkParser(zyte.base_url)
        parser.feed(html)

        xls_links = parser.xls_links
        if not xls_links:
            last_response_body_path = Path(target_dir) / 'last_response_body.html'
            with open(last_response_body_path, 'wt') as f:
                f.write(html)
        return xls_links

    def download_file(self, zyte: ZyteApi, url_path: str, target_dir: str) -> str:
        """
        Download XLS files from the given URLs.

        Args:
            urls: List of URLs to download
            target_dir: Directory to save files.

        Returns:
            List of paths to downloaded files
        """
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        filename = os.path.basename(url_path)
        if not filename:
            # If URL doesn't have a basename, use a hash or skip
            raise RuntimeError('Cannot extract filename from url %s', url_path)

        try:
            filepath = Path(target_dir) / filename

            downloaded_bytes = zyte.get_content(url_path)

            with open(filepath, 'wb') as f:
                f.write(downloaded_bytes)

            logger.info('Downloaded: %s -> %s', url_path, filepath)
            return filepath
        except Exception as e:
            logger.exception('Failed to download %s: %s', url_path, e)


    def scrape_and_download(self, date_str: str, target_dir: str) -> ScrapeDownloadResult:
        """
        Combined method: scrape XLS links for a date and download them.

        Args:
            date_str: Date in YYYY-MM-DD format
            target_dir: Directory to save files

        Returns:
            List of paths to downloaded files
        """
        url_path = self._build_date_path(date_str)
        logger.info('Scraping XLS links from: %s', url_path)

        zyte = ZyteApi('https://www.minfin.bg', 3, target_dir)

        xls_links = self.scrape_xls_links(zyte, url_path, target_dir)
        logger.info('Found %d XLS links', len(xls_links))

        if not xls_links:
            logger.info('No XLS files found on the page.')

            no_files_found_file = Path(target_dir) / 'no-files-found.txt'
            with open(no_files_found_file, 'wt') as f:
                f.write(datetime.now().isoformat())

            return ScrapeDownloadResult(
                files=[],
                failed_file=None,
                no_files_found_file=str(no_files_found_file)
            )

        logger.debug(f'Found link: %s', str(xls_links))

        url_paths = [urlparse(u).path for u in xls_links]
        download_result = zyte.download_urls(date_str, url_paths)

        return ScrapeDownloadResult(
            files=download_result.files,
            failed_file=download_result.failed_file,
            no_files_found_file=None
        )