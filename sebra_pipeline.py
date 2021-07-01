import os
import pandas as pd
import os

from sqlalchemy import create_engine
from settings import postgres_proj_str, main_project_drive_id, raw_data_folder_id, parsed_data_folder_id

from sebra_gdrive_manager import SebraGDrive
from sebra_downloader import SebraDownloader
from sebra_parser import SebraParser

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
    
        self.sebra_dates = pd.read_sql('select max("Start Date") as final_date from sebra_parsed', self.cnx)
        self.min_year = self.sebra_dates['final_date'][0].year
        self.min_month = self.sebra_dates['final_date'][0].month
        self.min_day = self.sebra_dates['final_date'][0].day

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
        self.min_year, self.min_month, self.min_day)
        self.sd.get_urls()
        self.sd.download_reports()

    def parse_new_reports(self):
        self.sp = SebraParser(self.sd.parent_location)
        self.ops_df = self.sp.run_parser()
        self.SGD.read_parsed_file(self.parsed_fname)
        self.SGD.append_parsed_df(self.ops_df)
        self.SGD.fix_dates()
        self.ops_df = fix_dates(self.ops_df)
    
    def upload_parsed_file_to_gdrive(self):
        self.SGD.parsed_df.to_csv(f'{self.file_loc}/{self.parsed_fname}')
        self.SGD.upload_parsed_file(self.file_loc, self.parsed_fname)

    def append_new_parse_to_db(self):
        self.ops_df.reset_index().to_sql(os.path.splitext(self.parsed_fname)[0], self.cnx, index=False, if_exists='append', method='multi')

    def upload_new_reports_to_gdrive(self):
        raw_files = self.sp.get_all_excel_files()
        self.SGD.upload_raw_files(raw_files)



def main():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    chrome_path = '/usr/local/bin/chromedriver'
    file_loc = './downloaded_files'
    folders = ['/SEBRA']#['/SEBRA','/NF_SEBRA','/MF_SEBRA']
    parsed_fname = 'sebra_parsed.csv'
    service_acct_file = 'service_acct.json'

    SebraPipe = SebraPipeline(chrome_path, file_loc, folders, parsed_fname)
    SebraPipe.initialize_sebra_gdrive(service_acct_file, SCOPES)
    SebraPipe.download_parsed_file_from_gdrive()
    SebraPipe.download_new_reports()
    SebraPipe.parse_new_reports()
    SebraPipe.upload_parsed_file_to_gdrive()
    SebraPipe.upload_new_reports_to_gdrive()
    SebraPipe.append_new_parse_to_db()

if __name__ == '__main__':
    main()

    

    

    

    
    
    
    
    