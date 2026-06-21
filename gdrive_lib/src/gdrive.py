"""Google Drive upload functionality for sebra-scrape."""

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from functools import cache

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)


@dataclass
class UnsuccessfulFile:
    """Information about an unsuccessful file in Google Drive."""
    name: str
    relative_path: str
    file_id: str
    parent_folder_id: str


class GoogleDriveUploader:
    """
    Google Drive uploader that creates yyyy/mm/dd folder structure
    and uploads files to the appropriate date-based subfolder.
    """

    DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
    FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    PROPERTY_ROOT_FOLDER_ID = 'sebra_root_folder_id'

    def __init__(self, folder_url: str, credentials_path: str):
        """
        Initialize the Google Drive uploader.

        Args:
            folder_url: Google Drive folder URL (e.g., https://drive.google.com/drive/folders/abc123)
            credentials_path: Path to the service account JSON credentials file
        """
        self.root_folder_id = self._extract_folder_id(folder_url)
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=[self.DRIVE_SCOPE]
        )
        self.service = build('drive', 'v3', credentials=self.credentials)

    @staticmethod
    def _calculate_md5(filepath: str) -> str:
        """Calculate MD5 checksum of a local file.

        Args:
            filepath: Path to the local file

        Returns:
            MD5 hash as hex string
        """
        hash_md5 = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _find_file_by_name(self, folder_id: str, filename: str) -> Optional[dict]:
        """Find a file by name in a specific folder.

        Args:
            folder_id: Parent folder ID
            filename: Name of the file to find

        Returns:
            File resource dict if found, None otherwise
        """
        query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
        results = self.service.files().list(
            q=query,
            fields='files(id, name, md5Checksum)',
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        if files:
            return files[0]
        return None

    @staticmethod
    def _extract_folder_id(folder_url: str) -> str:
        """
        Extract folder ID from Google Drive URL.

        Args:
            folder_url: URL like https://drive.google.com/drive/folders/abc123

        Returns:
            The folder ID
        """
        # Handle both formats: /drive/folders/ID and /open?id=ID
        if '/folders/' in folder_url:
            return folder_url.split('/folders/')[-1].split('?')[0].split('/')[0]
        elif 'id=' in folder_url:
            return folder_url.split('id=')[-1].split('&')[0]
        else:
            raise ValueError(f'Could not extract folder ID from URL: {folder_url}')

    def _get_folder_id_by_name(self, parent_id: str, folder_name: str) -> Optional[str]:
        """
        Get folder ID by name under a parent folder.

        Args:
            parent_id: Parent folder ID
            folder_name: Name of the folder to find

        Returns:
            Folder ID if found, None otherwise
        """
        query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='{self.FOLDER_MIME_TYPE}' and trashed=false"
        results = self.service.files().list(
            q=query,
            fields='files(id, name)',
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']

    def _create_folder(self, parent_id: str, folder_name: str) -> str:
        """
        Create a new folder under a parent folder.

        Args:
            parent_id: Parent folder ID
            folder_name: Name of the new folder

        Returns:
            The new folder ID
        """
        folder_metadata = {
            'name': folder_name,
            'mimeType': self.FOLDER_MIME_TYPE,
            'parents': [parent_id]
        }
        folder = self.service.files().create(
            body=folder_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        logger.info('Created folder: %s (ID: %s)', folder_name, folder['id'])
        return folder['id']

    def _ensure_folder(self, parent_id: str, name: str) -> str:
        folder_id = self._get_folder_id_by_name(parent_id, name)
        if folder_id is None:
            folder_id = self._create_folder(parent_id, name)

        return folder_id

    def _ensure_folder_structure(self, parent_id: str, year: str, month: str, day: str) -> str:
        """
        Ensure yyyy/mm/dd folder structure exists under parent, create if missing.

        Args:
            parent_id: Root folder ID
            year: Year as string (e.g., '2024')
            month: Month as string (e.g., '05')
            day: Day as string (e.g., '20')

        Returns:
            The day folder ID
        """
        year_folder_id = self._ensure_folder(parent_id, year)
        month_folder_id = self._ensure_folder(year_folder_id, month)
        day_folder_id = self._ensure_folder(month_folder_id, day)

        return day_folder_id

    def upload_file(self, date_str: str, local_filepath: str) -> str:
        """
        Upload a file to yyyy/mm/dd folder structure under the root folder.
        If a file with the same name exists and has the same MD5 checksum,
        the existing file is not updated. Otherwise, the file is updated/created.

        Args:
            date_str: Date in YYYY-MM-DD format
            local_filepath: Local path to the file to upload

        Returns:
            The Google Drive file ID of the uploaded/updated file
        """
        # Parse date
        year, month, day = date_str.split('-')

        # Ensure folder structure exists
        target_folder_id = self._ensure_folder_structure(
            self.root_folder_id, year, month, day
        )

        # Get filename and calculate local MD5
        filename = Path(local_filepath).name
        local_md5 = self._calculate_md5(local_filepath)

        # Check if file already exists in target folder
        existing_file = self._find_file_by_name(target_folder_id, filename)

        if existing_file:
            existing_md5 = existing_file.get('md5Checksum')
            logger.info('Found existing file %s with md5 sum %s, local''s file md5 sum is %s', filename, existing_md5, local_md5)
            if existing_md5 == local_md5:
                logger.info('File %s unchanged (MD5 matches), skipping update', filename)
                return existing_file['id']

        # Upload/update file
        file_metadata = {
            'name': filename,
            'properties': {
                self.PROPERTY_ROOT_FOLDER_ID: self.root_folder_id
            }
        }

        media = MediaFileUpload(local_filepath)

        if existing_file:
            # Update existing file (creates new version in Drive history)
            file = self.service.files().update(
                fileId=existing_file['id'],
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            logger.info('Updated existing file %s in folder ID %s (date: %s)',
                        filename, target_folder_id, date_str)
        else:
            # Create new file
            file_metadata['parents'] = [target_folder_id]
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            logger.info('Uploaded %s to folder ID %s (date: %s)',
                        filename, target_folder_id, date_str)

        return file['id']

    def _get_unsuccessful_files_via_property(self) -> List[dict]:

        query = (
            f"name contains 'unsuccessful-' and "
            f"properties has "
            f"  {{key='{self.PROPERTY_ROOT_FOLDER_ID}' and "
            f"    value='{self.root_folder_id}'"
            f"  }} and "
            f"trashed=false "
        )
        logger.debug('query %s', query)
        results = self.service.files().list(
            q=query,
            fields='files(id, name, parents)',
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        result = []
        for item in results.get('files', []):
            relative_path = self._get_relative_path(
                item['parents'][0], self.root_folder_id
            )
            result.append(
                UnsuccessfulFile(
                    file_id=item['id'],
                    name=item['name'],
                    relative_path=relative_path,
                    parent_folder_id=item['parents'],
                )
            )

        return result

    def get_unsuccessful_files(self) -> List[UnsuccessfulFile]:
        """Search for files with names starting with 'unsuccessful-' in the root folder tree.

        Returns:
            List of UnsuccessfulFile dataclass instances with file info
        """
        return self._get_unsuccessful_files_via_property()

    @cache
    def _get_folder_details(self, folder_id) -> tuple[str, str]:
        folder = self.service.files().get(
            fileId=folder_id,
            fields='id, name, parents',
            supportsAllDrives=True
        ).execute()

        if 'parents' not in folder:
            raise ValueError((
                'The specified folder id does not have `parents`. '
                f'Folder name: {folder["name"]}, Folder id: {folder["id"]}'
            ))

        return folder['name'], folder['parents'][0]

    @cache
    def _get_relative_path(self, folder_id: str, relative_to_id: str) -> str:
        name, parent_id = self._get_folder_details(folder_id)
        if folder_id == relative_to_id:
            return name
        else:
            parent_name = self._get_relative_path(parent_id, relative_to_id)
            return f'{parent_name}/{name}'
