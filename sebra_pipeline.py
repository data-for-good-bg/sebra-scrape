import os
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine
from settings import postgres_proj_str, main_project_drive_id, raw_data_folder_id, parsed_data_folder_id

from sebra_gdrive_manager import SebraGDrive
from sebra_downloader import SebraDownloader
from sebra_parser import SebraParser
import logging

def fix_dates(df):
    df['Start Date'] = pd.to_datetime(df['Start Date'], format='%d.%m.%Y', errors = 'coerce')
    df['End Date'] = pd.to_datetime(df['End Date'], format='%d.%m.%Y', errors = 'coerce')
    return df

class SebraPipeline:
    def __init__(self, chrome_path, file_loc, folders, parsed_fname):
        self.chrome_path = chrome_path
        self.file_loc = file_loc
        self.folders = folders
        self.cnx = create_engine(postgres_proj_str, paramstyle="format")
        self.parsed_fname = parsed_fname

        date_range = pd.date_range(start="2019-01-01",end=str(datetime.now().date()))
    
        self.sebra_dates = pd.read_sql(f'''select "Start Date" as final_date from {os.path.splitext(self.parsed_fname)[0].replace('_python', '')}''', self.cnx)
        self.missing_dates = set(date_range.astype(str)).difference(self.sebra_dates.final_date.astype(str).values)
        print(f"""Missing dates: {tuple(sorted(self.missing_dates))}""")
        # self.min_year = self.sebra_dates['final_date'][0].year
        # self.min_month = self.sebra_dates['final_date'][0].month
        # self.min_day = self.sebra_dates['final_date'][0].day

        # Download Parsed File from GDrive and Get Final Dates
    def initialize_sebra_gdrive(self,service_acct_file, SCOPES):
        self.SGD = SebraGDrive(service_acct_file, SCOPES,
            main_project_drive_id,
            raw_data_folder_id,
            parsed_data_folder_id)

    def download_parsed_file_from_gdrive(self):
        if not os.path.exists('downloaded_files'):
            os.makedirs('downloaded_files')
        self.SGD.download_parsed_file(self.parsed_fname) 

    def download_new_reports(self):
        self.sd = SebraDownloader(self.file_loc, self.chrome_path, self.folders, 
        self.missing_dates)
        self.sd.get_urls()
        self.sd.download_reports()

    def parse_new_reports(self):
        self.sp = SebraParser(self.sd.parent_location)
        self.ops_df = self.sp.run_parser()
        if self.ops_df.shape[0]>0:
            self.ops_df = fix_dates(self.ops_df)
        # self.SGD.read_parsed_file(self.parsed_fname)
        parsed_df = pd.read_sql(f"select * from {os.path.splitext(self.parsed_fname)[0].replace('_python', '')}", self.cnx)
        self.SGD.parsed_df = parsed_df.copy()
        self.SGD.append_parsed_df(self.ops_df)
        self.SGD.parsed_df = self.SGD.parsed_df[[c for c in self.SGD.parsed_df.columns if 'Unnamed' not in c]]
        # self.SGD.fix_dates()
        
    
    def upload_parsed_file_to_gdrive(self, sheets_id = None):
        self.SGD.parsed_df.to_csv(f'{self.file_loc}/{self.parsed_fname}')
        self.SGD.upload_parsed_file(self.file_loc, self.parsed_fname, sheets_id=sheets_id)

    def append_new_parse_to_db(self):
        self.ops_df.reset_index().drop('index', axis='columns').to_sql(os.path.splitext(self.parsed_fname)[0].replace('_python', ''), self.cnx, index=False, if_exists='append', method='multi')

    def upload_new_reports_to_gdrive(self):
        raw_files = self.sp.get_all_excel_files()
        self.SGD.upload_raw_files(raw_files)



def main():
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    chrome_path = '/usr/bin/chromedriver'
    file_loc = './downloaded_files'
    folders = ['/SEBRA']#['/SEBRA','/NF_SEBRA','/MF_SEBRA']
    parsed_fname = 'sebra_parsed_python.csv'
    service_acct_file = 'service_acct.json'

    
    SebraPipe = SebraPipeline(chrome_path, file_loc, folders, parsed_fname)
    SebraPipe.initialize_sebra_gdrive(service_acct_file, SCOPES)

    logging.info('Downloading Previously Parsed File')
    SebraPipe.download_parsed_file_from_gdrive()

    logging.info('Downloading new reports. This might take a while.')
    SebraPipe.download_new_reports()

    logging.info('Parsing new reports. This might take a while.')
    SebraPipe.parse_new_reports()

    logging.info('Uploading parsed file to GDrive and updating the public sheet.')
    SebraPipe.upload_parsed_file_to_gdrive(sheets_id='1VoB4dIH2Y2x2O-eH0ivNmBUYCcT-1NR6T5h8eWkE33Y')

    logging.info('Uploading new reports. This might take a while.')
    SebraPipe.upload_new_reports_to_gdrive()

    logging.info('Writing to DB')
    SebraPipe.append_new_parse_to_db()

if __name__ == '__main__':
    logging.info('Starting SEBRA Pipeline')
    main()

    

    

    

    
    
    
    
    