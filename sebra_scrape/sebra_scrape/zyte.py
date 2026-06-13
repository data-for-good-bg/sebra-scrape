import logging
import os
import time
import random
from base64 import b64decode
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4
from urllib.parse import urljoin
from collections import OrderedDict


import requests

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """
    The structure describes the result of a download_urls operation.
    * files is a list of filepaths to the downloaded files.
    * failed_file is an optional path to a file containing URLs that were attempted
      but not downloaded (even after retries).
    """
    files: list[str]
    failed_file: Optional[str] = None


# Zyte API status codes that indicate temporary failures and should trigger retries
# check example response in https://docs.zyte.com/zyte-api/usage/reference.html
# also https://docs.zyte.com/zyte-api/usage/errors.html#retrying-requests
STATUS_CODE_TO_RETRY = [
    429,  # User rate limit, Domain limit
    503,  # Global rate limit
    520,  # Website Ban
    521,  # Internal Downloading Error
]

RATE_LIMIT_WAIT_TIMES = [
    (20, 40),
    (20, 40),
    (30, 38),
    (30, 46),
    (30, 62),
    (30, 94),
    (30, 158),
    (30, 286),
    (30, 542),
    (30, 630)
]

UNSUCCESSFUL_WAIT_TIMES = [
    (3, 9),
    (3, 11),
    (3, 15),
    (3, 23),
    (3, 39),
    (3, 62),
    (3, 62),
    (3, 62),
    (3, 62),
    (3, 62),
]

FIRST_ATTEMPT_WAIT_TIMES = (3, 5)

WAIT_TIMES_PER_STATUS_CODE = {
    429: RATE_LIMIT_WAIT_TIMES,
    503: RATE_LIMIT_WAIT_TIMES,

    520: UNSUCCESSFUL_WAIT_TIMES,
    521: UNSUCCESSFUL_WAIT_TIMES,
}


class ZyteApi:
    """Client for the Zyte API (zyte.com).

    Handles website scraping with anti-bot bypass, session management,
    and automatic retries for certain error conditions.

    The API uses browser-like sessions to maintain cookies and state
    across multiple requests to the same domain.
    """

    def __init__(self, url: str, retry_cnt: int, target_dir: str):
        """Initialize the Zyte API client.

        Args:
            url: Base URL of the website to scrape (e.g., 'https://www.minfin.bg')
            retry_cnt: Number of retry attempts for retryable status codes.
                      Defaults to 3.

        Raises:
            RuntimeError: If ZYTE_API_KEY environment variable is not set
        """
        self._api_key = os.environ.get('ZYTE_API_KEY')
        if not self._api_key:
            raise RuntimeError('ZYTE_API_KEY environment variable is not set')

        self.base_url = url
        self._session_id: Optional[str] = None
        self._retry_cnt = retry_cnt
        self._target_dir = target_dir

    @property
    def session_id(self) -> str:
        """Get the current session ID, creating one if it doesn't exist.

        Retries session creation on retryable HTTP status codes.

        Returns:
            The active session ID
        """
        if self._session_id is None:
            self._session_id = self._create_session_with_retries()

        return self._session_id

    def _create_session_with_retries(self) -> str:
        """Create a new Zyte API session with automatic retry.

        Retries on retryable HTTP status codes (429, 503, 520, 521).

        Returns:
            The new session ID

        Raises:
            requests.HTTPError: If session creation fails after all retries
        """
        attempts = 0
        last_status_code = 0
        last_error = None

        while attempts < self._retry_cnt:
            try:
                # wait before the attempt
                self._wait(self.base_url, attempts, last_status_code)

                return self._create_session()

            except requests.HTTPError as e:
                last_error = e
                if e.response.status_code in STATUS_CODE_TO_RETRY:
                    logger.info(
                        'Caught a retryable status code %d while trying to create session, attempt %d',
                        e.response.status_code, attempts
                    )

                    last_status_code = e.response.status_code
                    attempts += 1
                else:
                    # Non-retryable error, re-raise immediately
                    raise

        raise requests.HTTPError(f'Failed to create session after {self._retry_cnt} retries', response=last_error.response)

    def _clear_session_id(self) -> None:
        """Clear the current session ID, forcing a new session on next request."""
        self._session_id = None

    def get_html(self, url_path: str) -> str:
        """Get the browser-rendered HTML of a webpage.

        Uses Zyte's browserHtml parameter to get fully rendered HTML,
        including JavaScript-generated content.

        Args:
            url_path: Path to append to the base URL (e.g., '/bg/transparency/2024-05-20')

        Returns:
            The browser-rendered HTML content as a string

        Raises:
            RuntimeError: If the API request fails after all retries
        """
        final_url = urljoin(self.base_url, url_path)

        payload = {
            'url': final_url,
            'browserHtml': True,
            'session': {'id': self.session_id},
            'requestHeaders': {'referer': self.base_url}
        }

        logger.debug('getting browser html from %s', final_url)

        api_response = self._make_zyte_api_call(payload=payload)

        logger.info('successfully extracted browser html from %s', final_url)
        return api_response.json()['browserHtml']

    def get_html_with_retries(self, url_path: str) -> str:
        """Get the browser-rendered HTML with automatic retry and session rotation.

        Similar to get_html but with retry logic for retryable HTTP status codes.
        If a retryable error occurs, clears the session and retries with a new one.

        Args:
            url_path: Path to append to the base URL (e.g., '/bg/transparency/2024-05-20')

        Returns:
            The browser-rendered HTML content as a string

        Raises:
            requests.HTTPError: If the API request fails after all retries
        """
        attempts = 0
        last_status_code = 0
        last_error = None

        while attempts < self._retry_cnt:
            try:
                # wait before the attempt
                self._wait(url_path, attempts, last_status_code)

                return self.get_html(url_path)

            except requests.HTTPError as e:
                last_error = e
                if e.response.status_code in STATUS_CODE_TO_RETRY:
                    logger.info(
                        'Caught a retryable status code %d while trying to get url %s, attempt %d',
                        e.response.status_code, url_path, attempts
                    )

                    self._clear_session_id()  # Force new session for next attempt
                    last_status_code = e.response.status_code
                    attempts += 1
                else:
                    # Non-retryable error, re-raise immediately
                    raise

        raise requests.HTTPError(
            f'Failed to get html after {self._retry_cnt} retries for {url_path}',
            response=last_error.response
        )

    def get_content(self, url_path: str) -> bytes:
        """Returns raw content (binary content) from a URL.

        Returns the raw file bytes. Useful for downloading XLS, PDF, etc.

        Args:
            url_path: Path to append to the base URL (e.g., '/path/to/file.xlsx')

        Returns:
            The file content as bytes

        Raises:
            RuntimeError: If the API request fails after all retries
        """
        final_url = urljoin(self.base_url, url_path)

        # effective_referer = referer if referer else self.base_url

        payload = {
            'url': final_url,
            'httpResponseBody': True,
            'session': {'id': self.session_id},
            'customHttpRequestHeaders': [
                {
                    'name': 'referer',
                    'value': self.base_url
                }
            ]
        }

        logger.debug('downloading file from %s', final_url)
        api_response = self._make_zyte_api_call(payload=payload)

        decoded_bytes = b64decode(api_response.json()['httpResponseBody'])
        logger.info('successfully downloaded %d bytes from %s', len(decoded_bytes), final_url)
        return decoded_bytes

    def _create_session(self) -> str:
        """Create a new Zyte API session.

        Creates a new browser session that maintains cookies and state.

        Returns:
            The new session ID

        Raises:
            RuntimeError: If session creation fails
        """
        self._session_id = None

        session_id = str(uuid4())

        payload = {
            'url': self.base_url,
            'browserHtml': True,
            'session': {'id': session_id}
        }
        self._make_zyte_api_call(payload=payload)

        logger.info('created session %s', session_id)
        return session_id

    def _make_zyte_api_call(self, payload: dict) -> requests.Response:
        """Make a request to the Zyte API.

        Args:
            payload: The request payload to send to Zyte API

        Returns:
            The successful requests.Response object

        Raises:
            requests.HTTPError if retryable status code is caught
        """
        logger.info('making attempt to get content for payload %s', str(payload))


        api_response = requests.post(
            'https://api.zyte.com/v1/extract',
            auth=(self._api_key, ''),
            json=payload
        )
        if api_response.ok:
            return api_response

        self._raise_for_status(api_response)

    def _wait(self, url: str, attempts: int, status_code: int) -> None:
        if attempts == 0:
            min_wait_time, max_wait_time = FIRST_ATTEMPT_WAIT_TIMES
        else:
            if status_code not in WAIT_TIMES_PER_STATUS_CODE:
                raise ValueError('Failed to find status_code %d in %s', status_code, str(WAIT_TIMES_PER_STATUS_CODE.keys()))

            wait_times = WAIT_TIMES_PER_STATUS_CODE[status_code]
            min_wait_time, max_wait_time = wait_times[attempts % len(wait_times)]

        sleep_time = random.uniform(min_wait_time, max_wait_time)
        logger.info('Sleeping for %s seconds before making attempt %d for url %s', sleep_time, attempts+1, url)
        time.sleep(sleep_time)

    def download_urls(self, date_str: str, url_paths: list[str]) -> DownloadResult:
        """Download multiple files with automatic retry and session rotation.

        If a URL fails, creates a new session and continues with another URL.
        Cycles back to failed URLs until reaching retry limit for each URL.
        Failed URLs are written to a text file in the target directory.

        Args:
            url_paths: List of URL paths to download (relative to base_url)
            target_dir: Directory to save downloaded files

        Returns:
            DownloadResult object describing the result of the operation.
        """
        os.makedirs(self._target_dir, exist_ok=True)

        # Track remaining URLs with their (attempt counts, last status_code)
        remaining = OrderedDict({url: (0, 0) for url in url_paths})
        successful_filepaths = []
        failed_urls = []

        while remaining:
            url_path, url_params = remaining.popitem(last=False)
            attempts, last_status_code = url_params

            try:
                # wait before the get_content call
                self._wait(url_path, attempts, last_status_code)

                # Download the file
                content = self.get_content(url_path)
                filename = os.path.basename(url_path)
                filepath = os.path.join(self._target_dir, filename)

                with open(filepath, 'wb') as f:
                    f.write(content)

                logger.info('Downloaded %s', url_path)
                successful_filepaths.append(filepath)

            except requests.HTTPError as e:
                if e.response.status_code in STATUS_CODE_TO_RETRY:
                    logger.info(
                        'Caught a retryable status code %d while trying to get url %s',
                        e.response.status_code, e.response.url
                    )

                    self._clear_session_id()  # Force new session for next attempt

                    if attempts + 1 < self._retry_cnt:
                        # Re-add to remaining with incremented attempt count
                        remaining[url_path] = (attempts + 1, e.response.status_code)
                    else:
                        logger.error('Permanently failed %s after %d attempts', url_path, self._retry_cnt)
                        failed_urls.append(urljoin(self.base_url, url_path))
                else:
                    raise e

        # Write failed URLs to file
        if failed_urls:
            failed_file = os.path.join(self._target_dir, f'unsuccessful-{date_str}.txt')
            with open(failed_file, 'w') as f:
                for url in failed_urls:
                    f.write(url + '\n')
            logger.info('Wrote %d failed URLs to %s', len(failed_urls), failed_file)
        else:
            failed_file = None

        return DownloadResult(
            files=successful_filepaths,
            failed_file=failed_file
        )

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        """Raise an exception for non-200 HTTP responses.

        Args:
            response: The requests.Response object to check

        Raises:
            RuntimeError: Always raised for non-200 responses, with status code and body
        """
        if not response.ok:
            if response.status_code in STATUS_CODE_TO_RETRY:
                msg = (
                    f'Request {response.request.method} to {response.url} '
                    f'completed with {response.status_code} '
                    f'and body {response.text}'
                )
                raise requests.HTTPError(msg, response=response)
            else:
                # let the original raise_for_status
                response.raise_for_status()
