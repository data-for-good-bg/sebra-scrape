import datetime
import os.path
import re

import requests
import logging

from .constants import (
    SEBRA_REPORT_ID_STRING,
    SEBRA_MF_REPORT_ID_STRING,
    SEBRA_NF_REPORT_ID_STRING,
    SEBRA_FILE_DESTINATIONS,
)

logger = logging.getLogger(__name__)

# When we look at the response we get from https://www.minfin.bg/bg/transparency, we expect to find links to let us
#   download reports that look like this:
#   https://www.minfin.bg/upload/57933/SEBRA-26032024.xlsx
#   https://www.minfin.bg/upload/57934/SEBRA-MF-2024-03-26.xlsx
#   https://www.minfin.bg/upload/57935/NF_SEBRA_26.03.2024.xlsx
SEBRA_LINKS_REGEXES = {
    SEBRA_REPORT_ID_STRING: r"/upload/\d+/SEBRA-\d+\.xlsx",
    SEBRA_MF_REPORT_ID_STRING: r"/upload/\d+/SEBRA-MF.*?\.xlsx",
    SEBRA_NF_REPORT_ID_STRING: r"/upload/\d+/NF_SEBRA.*?\.xlsx",
}


class WrongNumberOfDownloadLinksFoundException(Exception):
    """
    We expect to find exactly one link of each type on every page. This is raised if 0 or >=2 links found
    """

    pass


# TODO: return links including https://www.minfin.bg/ and refactor download report use those
def extract_links(date_for: datetime.date) -> dict[str, str]:
    """
    Extracts links for downloading SEBRA reports from https://www.minfin.bg/bg/transparency

    The extracted links look like this:
    https://www.minfin.bg/upload/57933/SEBRA-26032024.xlsx
    https://www.minfin.bg/upload/57934/SEBRA-MF-2024-03-26.xlsx
    https://www.minfin.bg/upload/57935/NF_SEBRA_26.03.2024.xlsx

    Args:
        date_for: The date for which we want to extract the links

    Returns:
        A dict mapping report identifiers (we have several types of reports, see our constants file) to links to
        download the corresponding report to the given date

    Raises:
        WrongNumberOfDownloadLinksFoundException: If the number of links we find does not match the number of report
            types we have. NOTE: will always be raised for weekend dates and official holidays since no reports exist
            for those dates.
    """
    url = f"https://www.minfin.bg/bg/transparency/{date_for.strftime('%Y-%m-%d')}"
    logger.warning(f"Fetching from f{url}")
    resp = requests.get(url)
    resp.raise_for_status()
    logger.warning(f'text: {resp.text}')

    # We will get one param string
    download_url_param_strings = dict()
    for report_type_id, expr in SEBRA_LINKS_REGEXES.items():
        matches = re.findall(expr, resp.text)
        logger.debug(f"Found {len(matches)} using r'{expr}'")
        if len(matches) != 1:
            logger.warning(f"Expected 1 link, got {matches} using r'{expr}'")
            continue
            #raise WrongNumberOfDownloadLinksFoundException(
            #    f"Expected 1 link, got {matches} using r'{expr}'"
            #)
        download_url_param_strings[report_type_id] = matches[0]
    logger.info(f"Links found: {download_url_param_strings}")
    return download_url_param_strings


def download_report(destination_dir: str, param_string: str) -> None:
    """
    Downloads a report from a URL constructed using the given parameter string and saves it to the destination
        directory.

    Args:
        destination_dir: The directory where the downloaded report will be saved.
        param_string: The parameter string used to construct the URL.

    Returns:
        None

    Raises:
        requests.HTTPError: If there is an HTTP error during the request to download the report.
        IOError: If there is an error writing the downloaded report to the destination file.
    """
    url = f"https://www.minfin.bg{param_string}"
    resp = requests.get(url)
    resp.raise_for_status()

    file_name = param_string.split("/")[
        -1
    ]  # The param string will end with the filename

    os.makedirs(destination_dir, exist_ok=True)
    dest_file = os.path.join(destination_dir, file_name)
    logger.info(f"Writing to {dest_file} ...")
    with open(dest_file, "wb") as fp:
        fp.write(resp.content)

    return dest_file
