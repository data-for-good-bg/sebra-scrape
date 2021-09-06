import os
import numpy as np
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine
from settings import postgres_proj_str, main_project_drive_id, raw_data_folder_id, parsed_data_folder_id

from sebra_gdrive_manager import SebraGDrive
from sebra_downloader import SebraDownloader
from sebra_parser import SebraParser
import logging
import gspread
from gspread_dataframe import set_with_dataframe

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

        date_range = pd.date_range(start="2019-01-01",end=str(datetime.now().date()))

        self.date_range = date_range.copy()

        self.parsed_df = pd.read_csv(f'{self.file_loc}/{self.parsed_fname}')
        self.parsed_df = self.parsed_df.drop_duplicates(subset=['Organization ID', 'Operations Code', 'Start Date', 'Operations Amount (BGN)'])

        self.parsed_df = self.parsed_df[[c for c in self.parsed_df.columns if 'Unnamed' not in c]]
    
        self.missing_dates = set(date_range.astype(str)).difference(self.parsed_df['Start Date'].astype(str).values)
        print(f"""Missing dates: {tuple(sorted(self.missing_dates))}""")

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
        
        self.SGD.parsed_df = self.parsed_df.copy()
        self.SGD.append_parsed_df(self.ops_df)
        self.SGD.parsed_df = self.SGD.parsed_df[[c for c in self.SGD.parsed_df.columns if 'Unnamed' not in c]]
        # self.SGD.fix_dates()

    def fix_dupes_categories(self):
        # Download Duplicates
        self.SGD.GDM.download_by_file_id('1pTMfqfK9ixiTIWaUhQwwpJGg12g7QWFx', 'downloaded_files/duplicates.csv')

        # Download Categories
        self.SGD.GDM.download_by_file_id('1wRF1VqNfxe9Glu5FdrwMOGyegNgSENL6',
        'downloaded_files/categories.csv')

        duplicates = pd.read_csv('downloaded_files/duplicates.csv')
        duplicates_dict = dict(zip(duplicates.Find, duplicates.ReplaceWith))

        categories = pd.read_csv('downloaded_files/categories.csv')[['Organization ID', 'Category']]

        parsed_df = self.SGD.parsed_df.copy()
        parsed_df['Organization Name'] = np.where(parsed_df['Organization Name'].isin(list(duplicates_dict.keys())), parsed_df['Organization Name'].map(duplicates_dict), parsed_df['Organization Name'])

        parsed_df = parsed_df.drop(['Category', 'Latest Date'], axis='columns')
        parsed_df = parsed_df.merge(categories, how = 'left', on = ['Organization ID'])

        self.SGD.parsed_df = parsed_df.copy()
        self.SGD.parsed_df['Latest Date'] = pd.to_datetime(self.SGD.parsed_df['Start Date']) == (pd.to_datetime(self.SGD.parsed_df['Start Date']).max())
        self.SGD.parsed_df['Start Date'] = pd.to_datetime(self.SGD.parsed_df['Start Date'])
        self.SGD.parsed_df['End Date'] = pd.to_datetime(self.SGD.parsed_df['End Date'])
        
        os.remove('downloaded_files/categories.csv')
        os.remove('downloaded_files/duplicates.csv')
        
    
    def upload_parsed_file_to_gdrive(self, sheets_id = None):
        
        self.SGD.parsed_df.to_csv(f'{self.file_loc}/{self.parsed_fname}')
        self.SGD.upload_parsed_file(self.file_loc, self.parsed_fname, sheets_id=sheets_id)

        gc = gspread.service_account(filename = self.SGD.credentials_file)
        sht1 = gc.open_by_key(sheets_id)
        worksheet = sht1.add_worksheet(title="dates", rows = '1', cols = '1')
        date_df = pd.DataFrame({'Date': self.date_range.date})
        set_with_dataframe(worksheet, date_df)

    def append_new_parse_to_db(self):
        self.parsed_df.reset_index().drop('index', axis='columns').to_sql(os.path.splitext(self.parsed_fname)[0].replace('_python', ''), self.cnx, index=False, if_exists='replace', method='multi')

    def upload_new_reports_to_gdrive(self):
        raw_files = self.sp.get_all_excel_files()
        self.SGD.upload_raw_files(raw_files)

    def get_all_excel_files(self):
        data_dir = f'{self.file_loc}/SEBRA'
        folders = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]
        excel_files = []
        for fold in folders:
            if int(fold) < 2019:
                continue
            excel_files += [
                os.path.join(data_dir, fold, f) \
                    for f in os.listdir(os.path.join(data_dir, fold)) \
                        if os.path.isfile(os.path.join(data_dir, fold, f)) \
                            and (f.endswith('.xlsx') or f.endswith('.xls'))
            ]
        return sorted(excel_files)

    def cleanup(self):
        raw_file_list = self.get_all_excel_files()
        for fname in raw_file_list:
            name = fname.split('/')[-1]
            os.remove(fname)

        filelist = [ f for f in os.listdir(self.file_loc) if f.endswith(".csv") ]
        for f in filelist:
            os.remove(os.path.join(self.file_loc, f))


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

    SebraPipe.fix_dupes_categories()

    logging.info('Uploading parsed file to GDrive and updating the public sheet.')
    SebraPipe.upload_parsed_file_to_gdrive(sheets_id='1VoB4dIH2Y2x2O-eH0ivNmBUYCcT-1NR6T5h8eWkE33Y')

    SebraPipe.cleanup()

    

    # logging.info('Uploading new reports. This might take a while.')
    # SebraPipe.upload_new_reports_to_gdrive()

    # logging.info('Writing to DB')
    # SebraPipe.append_new_parse_to_db()

if __name__ == '__main__':
    logging.info('Starting SEBRA Pipeline')
    main()

    

    

    

    
    
    
    
    