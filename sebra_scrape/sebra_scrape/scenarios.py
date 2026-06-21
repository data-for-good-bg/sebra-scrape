"""Business logic scenarios for sebra-scrape.

This module contains the core functionality scenarios that can be used
both by CLI commands and by Airflow DAGs.
"""

import logging
import os
from pathlib import Path
from typing import Optional

from dataclasses import dataclass
from datetime import datetime, timedelta

from gdrive import GoogleDriveUploader
from .scrape import MinfinScraper, ScrapeDownloadResult

logger = logging.getLogger(__name__)


@dataclass
class CollectResultOperation:
    date_str: str
    local_cache_files: list[str]
    downloaded_files: list[str]
    failed_file: Optional[str]
    no_files_found_file: Optional[str]

    def get_all_available_files(self) -> list:
        result = []

        if self.local_cache_files:
            result.extend(self.local_cache_files)
        if self.downloaded_files:
            result.extend(self.downloaded_files)
        if self.failed_file:
            result.append(self.failed_file)
        if self.no_files_found_file:
            result.append(self.no_files_found_file)

        return result



def download_for_date(
    date_str: str,
    target_dir: str
) -> ScrapeDownloadResult:
    """Download XLS files for a specific date.

    This is the core download scenario that can be used by both CLI and Airflow.

    Args:
        date_str: Date in YYYY-MM-DD format
        target_dir: Target directory for downloaded files.

    Returns:
        Tuple of (list of downloaded file paths, path to failed URLs file or None)
    """
    scraper = MinfinScraper()

    os.makedirs(target_dir, exist_ok=True)

    download_result = scraper.scrape_and_download(date_str, target_dir=str(target_dir))

    logger.info(f'scrape download result: {download_result}')
    return download_result


def collect_to_gdrive(
    date_str: str,
    target_dir: Optional[str] = None,
    use_local_cache: bool = False
) -> CollectResultOperation:
    """Download XLS files and upload to Google Drive.

    This is the core collect-to-gdrive scenario that can be used by both CLI and Airflow.
    Relies on download_for_date for the download portion.

    Args:
        date_str: Date in YYYY-MM-DD format
        target_dir: Target directory for local downloads
        use_local_cache: If True and local date directory exists, skip download
    """
    gdrive_creds_path = os.environ['SEBRA_GOOGLE_DRIVE_CREDS']
    gdrive_folder_url = os.environ['SEBRA_GOOGLE_DRIVE_FOLDER_URL']

    if target_dir is None:
        raise ValueError('target_dir is not specified')

    # Determine local target directory
    target_path = Path(target_dir) / date_str

    # Check for local cache
    local_files = []

    result : CollectResultOperation = None

    if use_local_cache \
        and target_path \
        and target_path.exists() \
        and target_path.is_dir():
        # Find existing local files, if possible to skip download
        logger.info('Using local cache from: %s', target_path)
        local_files = [str(f) for f in target_path.glob('*.xlsx') if f.is_file()]

    # TODO: handle partial downloads from previous execution?
    if len(local_files) == 3:
        result = CollectResultOperation(
            date_str=date_str,
            local_cache_files=local_files,
            downloaded_files=[],
            failed_file=None,
            no_files_found_file=None
        )
    else:
        # Download files locally using download_for_date
        downloaded_result = download_for_date(date_str, target_dir=str(target_path))
        result = CollectResultOperation(
            date_str=date_str,
            local_cache_files=[],
            downloaded_files=downloaded_result.files,
            failed_file=downloaded_result.failed_file,
            no_files_found_file=downloaded_result.no_files_found_file
        )

    # Upload to Google Drive
    uploader = GoogleDriveUploader(gdrive_folder_url, gdrive_creds_path)

    # Upload all files
    for filepath in result.get_all_available_files():
        uploader.upload_file(date_str, str(filepath))
        logger.info('Uploaded %s to Google Drive', filepath)

    return result


def collect_to_gdrive_for_days(
    date_str: str,
    lookback_days: int,
    target_dir: str,
    use_local_cache: bool
) -> None:
    """Collect and upload files for a range of weekdays.

    Starts from the specified date and looks back for the specified number of days.
    Only processes weekdays (Monday-Friday), skips weekends.

    Args:
        date_str: Start date in YYYY-MM-DD format
        lookback_days: Number of days to look back (inclusive of start date)
        target_dir: Target directory for local downloads
        use_local_cache: Whether to use local cache if available

    Returns:
        0 on success, 1 on failure
    """
    start_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    collected_results : list[CollectResultOperation] = []
    collected_exceptions : list[Exception] = []

    for i in range(lookback_days):
        current_date = start_date - timedelta(days=i)
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() >= 5:
            logger.info('Skipping %s because it is %s', str(current_date), current_date.strftime('%A'))
            continue

        date_str_formatted = current_date.strftime('%Y-%m-%d')
        try:
            result = collect_to_gdrive(
                date_str=date_str_formatted,
                target_dir=target_dir,
                use_local_cache=use_local_cache
            )
            collected_results.append(result)
        except Exception as e:
            logger.exception(f'Failed to collect files for date {date_str_formatted}')
            collected_exceptions.append(e)

    # Log summary

    cnt_local_cache_files = sum(len(r.local_cache_files) for r in collected_results)
    cnt_downloaded_files = sum(len(r.downloaded_files) for r in collected_results)
    cnt_dates_with_no_files_found = sum(
        1 for r in collected_results
        if r.no_files_found_file is not None
    )

    total_failed_urls = 0
    for r in collected_results:
        if r.failed_file:
            with open(r.failed_file, 'rt') as f:
                total_failed_urls += len(f.readlines())

    logger.info((
        'Summary:\n'
        '  days attempted: %d,\n'
        '  uploaded from local cache: %d,\n'
        '  downloaded files: %d,\n'
        '  failed to download URLs: %d,\n'
        '  dates with no files found: %d\n'
        '  dates for which collection completed with exception: %d'),
        lookback_days,
        cnt_local_cache_files,
        cnt_downloaded_files,
        total_failed_urls,
        cnt_dates_with_no_files_found,
        len(collected_exceptions)
    )
    if collected_exceptions:
        raise RuntimeError('There were exceptions during the execution. Check the logs')