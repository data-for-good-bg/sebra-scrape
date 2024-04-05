import unittest
import os
from unittest.mock import patch, mock_open, MagicMock
from datetime import date
from src.download import (
    extract_links,
    download_report,
    WrongNumberOfDownloadLinksFoundException,
)
from src.constants import (
    SEBRA_REPORT_ID_STRING,
    SEBRA_MF_REPORT_ID_STRING,
    SEBRA_NF_REPORT_ID_STRING,
    SEBRA_FILE_DESTINATIONS,
)


class TestExtractLinks(unittest.TestCase):
    @patch("requests.get")
    def test_extract_links_success(self, mock_get):
        # Read mock response text from HTML file
        with open("fixtures/mock_minfin_transparency_response.html", "r") as file:
            mock_response_text = file.read()

        # Mock response from requests.get
        mock_response = mock_get.return_value
        mock_response.text = mock_response_text

        # Test extract_links function
        result = extract_links(date(2024, 3, 26))

        # Check if the correct URLs are extracted
        self.assertEqual(
            result[SEBRA_REPORT_ID_STRING], "/upload/57933/SEBRA-26032024.xlsx"
        )
        self.assertEqual(
            result[SEBRA_MF_REPORT_ID_STRING], "/upload/57934/SEBRA-MF-2024-03-26.xlsx"
        )
        self.assertEqual(
            result[SEBRA_NF_REPORT_ID_STRING], "/upload/57935/NF_SEBRA_26.03.2024.xlsx"
        )

    @patch("requests.get")
    def test_extract_links_failure(self, mock_get):
        # Mock response from requests.get
        mock_response = mock_get.return_value
        mock_response.text = ""

        # Test extract_links function
        with self.assertRaises(WrongNumberOfDownloadLinksFoundException):
            extract_links(date(2024, 3, 26))


class TestDownloadReport(unittest.TestCase):

    @patch("requests.get")
    @patch("builtins.open", new_callable=MagicMock)
    @patch("os.makedirs")
    def test_download_report_success(self, mock_makedirs, mock_open, mock_get):
        # Mock response from requests.get
        mock_response = MagicMock()
        mock_response.content = b"Mock report content"
        mock_get.return_value = mock_response

        # Mock destination directory
        destination_dir = "/path/to/destination"
        param_string = "/upload/57933/SEBRA-26032024.xlsx"
        report_type_id = SEBRA_REPORT_ID_STRING

        # Test download_report function
        download_report(destination_dir, param_string, report_type_id)

        # Check if requests.get and open were called with correct arguments
        mock_get.assert_called_once_with(f"https://www.minfin.bg{param_string}")
        mock_makedirs.assert_called_once()
        mock_open.assert_called_once_with(
            os.path.join(
                destination_dir,
                SEBRA_FILE_DESTINATIONS["raw"][report_type_id],
                "SEBRA-26032024.xlsx",
            ),
            "wb",
        )

    @patch("requests.get")
    def test_download_report_failure(self, mock_get):
        # Mock response from requests.get to raise HTTPError
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("Mock HTTP error")
        mock_get.return_value = mock_response

        # Mock destination directory
        destination_dir = "/path/to/destination"
        param_string = "/mock/report/url"
        report_type_id = "mock_report"

        # Test download_report function
        with self.assertRaises(Exception):
            download_report(destination_dir, param_string, report_type_id)


if __name__ == "__main__":
    unittest.main()
