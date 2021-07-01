from __future__ import print_function
import os.path
import io
import shutil
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from mimetypes import MimeTypes
from google.oauth2 import service_account
import pandas as pd

# If modifying these scopes, delete the file token.json.

class GDriveManager:
    def __init__(self, credentials_file, scopes) -> None:
        self.scopes = scopes
        self.credentials_file = credentials_file

    def create_gdrive_service(self):
        credentials = service_account.Credentials.from_service_account_file(
        self.credentials_file, scopes=self.scopes)

        self.service = build('drive', 'v3', credentials=credentials)

    def search_by_fname(self, filename, foldername = None, exact = False):
        if exact:

            if foldername is not None:
                qry = f"name='{filename}' and '{foldername}' in parents"
            else:
                qry = f"name='{filename}'"
        
        else:
            if foldername is not None:
                qry = f"name contains '{filename}' and '{foldername}' in parents"
            else:
                qry = f"name contains '{filename}'"
        
        results = self.service.files().list(q=qry,
                includeItemsFromAllDrives=True, supportsAllDrives=True, 
                fields='nextPageToken, files(id, name)',
                pageToken=None).execute()

        
        items = results.get('files', [])
        files_df = pd.DataFrame(items)
        return files_df

    def get_drive_metadata(self, drivename = None):
        if drivename is not None:
            
            results = self.service.drives().list(q=f"name='{drivename}'", pageSize=10).execute()
        
        else:
            results = self.service.drives().list(pageSize=10).execute()
        return results


    def download_by_file_id(self, file_id, destination_fname):
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        try:
            # Download the data in chunks
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)
                
            # Write the received data to the file
            with open(destination_fname, 'wb') as f:
                shutil.copyfileobj(fh, f)

            print("File Downloaded")

        except:
            
            # Return False if something went wrong
            print("Something went wrong.")

    def upload_to_target_drive(self, source_fname, destination_fname, folder_id, file_id = None):
        name = source_fname.split('/')[-1]
          
        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]
          
        # create file metadata
        if file_id is None:
            file_metadata = {'name': destination_fname,
                'parents': [folder_id]}
        else:
            file_metadata = {'name': destination_fname,
                'parents': [folder_id], 
                'id': file_id}
  
        try:
            media = MediaFileUpload(source_fname, mimetype=mimetype,
                        resumable=True)
              
            # Create a new file in the Drive storage
            self.service.files().create(
                body=file_metadata, media_body=media, fields='id', 
                supportsAllDrives=True).execute()
              
            print("File Uploaded.")
          
        except:
              
            # Raise UploadError if file is not uploaded.
            raise Exception("Can't Upload File.")
        
        # pass

            

