"""
Tasks
    - Upload raw files
        - Upload a file
    - Append new data to final data sheet
        - Append data to a google sheet
    - Upload unparsable files
        - Upload a file
    - Download fixed files (and remove them?)
        - Download all files in a directory

Functions:
- Upload a file
- Download all files from a directory
- append data to a google sheet

"""

import io
import logging
from mimetypes import MimeTypes

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

logger = logging.getLogger(__name__)


def download_files(source_drive_folder_id: str, local_target_dir: str):
    pass


def append_data_to_sheet(google_sheet_id: str, df: pd.DataFrame):
    pass


class GDriveUploadError(Exception):
    pass


class GDriveDownloadError(Exception):
    pass


class GDriveManager:
    def __init__(self, credentials_file: str) -> None:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=["https://www.googleapis.com/auth/drive"]
        )

        self.service = build("drive", "v3", credentials=credentials)

    def upload_local_file_to_gdrive(self, local_file_path, gdrive_folder_id) -> str:
        logger.info(
            f"Uploading {local_file_path} to a GDrive folder with id {gdrive_folder_id}"
        )
        file_name = local_file_path.split("/")[-1]
        mimetype = MimeTypes().guess_type(file_name)[0]
        file_metadata = {"name": file_name, "parents": [gdrive_folder_id]}
        try:
            media = MediaFileUpload(local_file_path, mimetype=mimetype, resumable=True)

            # Create a new file in the Drive storage
            file = (
                self.service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )

            file_id = file.get("id")
            logger.info(
                f"Successfully uploaded {local_file_path} to a file in GDrive with id {file_id}"
            )
            return file_id

        except Exception as e:
            raise GDriveUploadError("Can't Upload File.") from e

    # TODO: support both exporting google docs and downloading blobs: https://developers.google.com/drive/api/guides/manage-downloads
    def download_file(
        self,
        file_id,
        destination_file: str,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ) -> None:
        """Downloads a file

        See https://developers.google.com/drive/api/guides/ref-export-formats for export MIME types

        Args:
            file_id: ID of the file to download

        """

        try:
            # pylint: disable=maybe-no-member
            request = self.service.files().export_media(
                fileId=file_id, mimeType=mime_type
            )
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.info(f"Download {int(status.progress() * 100)}.")

        except HttpError as error:
            raise GDriveDownloadError from error

        with open(destination_file, "wb") as fp:
            fp.write(file.getvalue())

    def list_all_files_in_folder(self, gdrive_folder_id: str) -> list[dict[str, str]]:
        results = (
            self.service.files()
            .list(
                corpora="allDrives",
                q=f"'{gdrive_folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )

        return results.get("files", [])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    mngr = GDriveManager(
        "/home/yasen/workspace/dataforgood/sebra-scrape/creds/service_acct.json"
    )
    #
    # f = mngr.download_file("1RVTKXPoKUgvFTblu-f-aQm-QxNRkVwt-jqkWUgHxxjk",
    #                        "/home/yasen/workspace/dataforgood/sebra-scrape/downloaded_files/sth.xlsx")
    # print(f)

    drive_id = "1tdmzPYf1ZPKuJhFni-lRW8dTBYOXh7vm"
    items = mngr.list_all_files_in_folder(drive_id)
    print(items)
