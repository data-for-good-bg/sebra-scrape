import os
import pandas as pd
import os

from gdrive_manage import GDriveManager
from googleapiclient.http import MediaFileUpload
from mimetypes import MimeTypes

class SebraGDrive:
    def __init__(self, credentials_file, scopes, main_folder_id, raw_data_folder_id, parsed_data_folder_id) -> None:
        self.main_folder_id = main_folder_id
        self.credentials_file = credentials_file
        self.scopes = scopes
        self.raw_data_folder_id = raw_data_folder_id
        self.parsed_data_folder_id = parsed_data_folder_id

        self.GDM = GDriveManager(self.credentials_file, self.scopes)
        self.GDM.create_gdrive_service()

    def download_parsed_file(self, fname):
        parsed_file = self.GDM.search_by_fname(fname, self.parsed_data_folder_id, exact=False)
        self.parsed_id = parsed_file['id'][0]
        self.GDM.download_by_file_id(self.parsed_id, f'downloaded_files/{fname}')

    def read_parsed_file(self, fname):
        self.parsed_df = pd.read_csv(f'downloaded_files/{fname}')

        self.parsed_df['Start Date'] = pd.to_datetime(self.parsed_df['Start Date'], format='%Y-%m-%d', errors = 'coerce')
        self.parsed_df['End Date'] = pd.to_datetime(self.parsed_df['End Date'], format='%Y-%m-%d', errors = 'coerce')

        self.max_date = self.parsed_df['Start Date'].max() + pd.DateOffset(1)
        self.min_year = self.max_date.year
        self.min_month = self.max_date.month
        self.min_day = self.max_date.day

    def append_parsed_df(self, new_parsed_df):
        self.parsed_df = pd.concat([self.parsed_df, new_parsed_df])
        self.parsed_df.to_csv('downloaded_files/parsed_drive_file.csv', index=False)

    
    def fix_dates(self):
        self.parsed_df['Start Date'] = pd.to_datetime(self.parsed_df['Start Date'], format='%d.%m.%Y', errors = 'coerce')
        self.parsed_df['End Date'] = pd.to_datetime(self.parsed_df['End Date'], format='%d.%m.%Y', errors = 'coerce')
        

    def upload_raw_files(self, raw_file_list):
        for fname in raw_file_list:
            name = fname.split('/')[-1]
            try:
                self.GDM.upload_to_target_drive(fname, name, self.raw_data_folder_id)
                os.remove(fname)
            except:
                print(f'Cannot upload file {name}')
                continue

    def upload_parsed_file(self, source_location, parsed_fname, sheets_id = None):
        file_metadata = {'name': parsed_fname,
        'fileId': self.parsed_id}

        mimetype = MimeTypes().guess_type(parsed_fname)[0]
        media = MediaFileUpload(f'{source_location}/{parsed_fname}', mimetype=mimetype,
                        resumable=True)
        try:
            self.GDM.service.files().update(
                    body=file_metadata, media_body=media, fields='id', fileId = self.parsed_id, 
                    supportsAllDrives=True).execute()
            
            print('Parsed File Updated')
            os.remove(f'{source_location}/{parsed_fname}')
        except:
            raise Exception("Can't Upload File.")
        if sheets_id is not None:
            fileName = self.GDM.service.files().get(fileId=sheets_id).execute()["name"]
            file_sheets_metadata = {'name': fileName,
            'fileId': sheets_id,
            'mimeType': 'application/vnd.google-apps.spreadsheet'}

            self.GDM.service.files().update(
                    body=file_sheets_metadata, media_body=media, fileId = sheets_id, fields='id',
                    supportsAllDrives=True).execute()

    