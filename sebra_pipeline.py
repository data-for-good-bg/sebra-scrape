import sys
import os
import time

#!{sys.executable} -m pip install scrapy

from datetime import date,datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pickle
import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pandas as pd
import numpy as np
import os

class SebraDownloader:
    def __init__(self, file_location, chrome_path, folders, min_year, min_month,min_day) -> None:
        
        self.path = file_location
        self.chrome_exec_path = chrome_path
        self.min_year = min_year
        self.min_month = min_month
        self.min_day = min_day
        self.folders = folders
        

        self.issues_requests = {}
        self.issues_download = {}
        self.issues_rename = {}
        self.soups = {}
        self.processed_files = 0

    def download_parsed_file_from_gcloud():
        pass

    def get_urls(self):
    #max date always current date
        max_year = datetime.now().date().year
        years = [str(year) for year in range(self.min_year,max_year + 1)]

        for folder in self.folders:
            #if folder already exists, check if there are subfolders with years, create if necessary
            if os.path.exists(self.path + folder) == True:
                print ("Already existing directory: ", self.path + folder)
                for y in years:
                    try:
                        os.mkdir(self.path + folder + '/' + y)
                    except FileExistsError:
                        pass
            else:
                try:
                    os.makedirs(self.path + folder)
                    print ("Successfully created the directory: ", self.path + folder)
                except OSError:
                    print ("Failure creation of the directory:", self.path + folder)

                for y in years:
                    try:
                        os.mkdir(self.path + folder + '/' + y)
                    except FileExistsError:
                        pass

        #get latest SEBRA date imported

        imported_dates = []
        for year in years:
            try:
                # Why is this hard-coded?
                max_date = max([datetime.strptime(date[:10],'%Y-%m-%d').date() for date in os.listdir(self.path + '/SEBRA/' + year) if type(re.search('\d{4}-\d{2}-',date)) == re.Match])
                imported_dates.append(max_date)
            except ValueError:
                pass

        # if there are already imported files, re-assign     
        if len(imported_dates) == 0:
            pass
        else:
            max_imported_date = max(imported_dates)
            self.min_day = max_imported_date.day
            self.min_month = max_imported_date.month
            self.min_year = max_imported_date.year

        #starting date to search for data
        start_date = date(self.min_year,self.min_month, self.min_day) #2011-07-22
        end_date = datetime.now().date() #date(2021, 12, 31)
        
        assert start_date <= end_date,'Start date {} is not before end date {}'.format(start_date,end_date)

        print('Path where files will be downloaded: ', self.path)
        print('Start date selected: {}'.format(start_date))
        print('End date selected: {}'.format(end_date))
        
        #defines the date range: weekends are not included (normally no data is expected here)
        dates =[str(date.date()) for date in pd.date_range(start  = start_date, end = end_date, periods = None, freq='B')]

        ## extract the urls with the relevant dates
        urls = {}
        for d in dates:
            url = 'https://www.minfin.bg/bg/transparency/' + d
            urls[d] = url
        print('All relevant urls to be crawled for the specified period exported')

        self.urls = urls
        self.dates = dates
        self.start_date = start_date
        self.end_date = end_date
        
    
    def download_reports(self):
        """
        Downloads the SEBRA reports from the url.

        [extended_summary]
        """
        with requests.Session() as s:
            for d in self.urls.keys():
                
                
                year = d[:4]
                html = s.get(self.urls[d])

                if html.status_code != 200:
                    self.issues_requests[d] = 'Status request: ' + str(html.status_code)
                else:
                    self.soups[d] = {'num_files':0,'num_upload':0,'files':[]}
                    soup = BeautifulSoup(html.content,'html.parser')

                    num_files = 0
                    num_upload = 0
                    files = []

                    for link in soup.find_all('a'):
                        #checks and counts if upload is mentioned in the html
                        if 'upload' in link.get('href'):
                            num_upload += 1 
                        #checks and counts if .xl is mentioned in the html, extracts the links !! to do might need to look for other extensions
                        if '.xl' in link.get('href'):
                            num_files += 1
                            files.append('https://www.minfin.bg' + link.get('href'))

                    if len(files) > 0:  #if we have any files downloaded
                        # check whether the file already exists -> if it does -> replace with new file
                        for file in files:
                            
                            # @change files
                            if 'MF' in file or 'NF' in file:  #this line needs to be dropped if we want to download all of them
                                continue   #this line needs to be dropped if we want to download all of them
                            else:  #this line needs to be dropped if we want to download all of them

                                processed_files =+ 1
                                url = file
                                if "NF" in file:
                                        self.parent_location = self.path + self.folders[1]
                                        location  = self.path + self.folders[1] + '/' + year
                                elif "MF" in file:
                                        self.parent_location = self.path + self.folders[2]
                                        location  = self.path + self.folders[2] + '/' + year
                                else:
                                        self.parent_location = self.path + self.folders[0]  
                                        location  = self.path + self.folders[0] + '/' + year

                                file_name = [r.end() for r in re.finditer('/',file)][-1] - 1

                                #if os.path.exists(location + file[last:]) == True:
                                    #continue
                                #else:
                                try:
                                    # dir(webdriver.common.html5.application_cache.ApplicationCache)
                                    customOption = Options()
                                    customOption.add_experimental_option('prefs',{'download.default_directory':location})
                                    customOption.add_argument("--headless")
                                    driver = webdriver.Chrome(executable_path = self.chrome_exec_path, options = customOption)
                                    driver.get(url)
                                    time.sleep(5) ## this needs to be addressed
                                    driver.close()
                                except Exception as e:
                                    self.issues_download[file] = e

                                ##normalize the date
                                try:
                                    os.rename(location + '/'+ file.split('/')[-1],location + '/'+ d + '_' + file.split('/')[-1])
                                except Exception as e:
                                    self.issues_rename[file] = e
                    else: 
                        continue

                    self.soups[d]['num_files']  = num_files
                    self.soups[d]['num_upload'] = num_upload
                    self.soups[d]['files'] = files
                
    def write_metadata(self):
        dates_meta = pd.DataFrame.from_dict(self.soups,orient ='index')
        name = 'dates_meta_' + str(self.start_date) + "_" + str(self.end_date)
        with open(self.path + '/'+ name, 'wb') as config_dictionary_file:
            pickle.dump(dates_meta, config_dictionary_file)

    def upload_to_gc(self, remove_from_local = True):
        """
        Uploads raw reports and the combined data into relevant GCloud folder

        [extended_summary]

        Args:
            remove_from_local (bool, optional): Flag to remove all files from the local directory. Defaults to True.
        """
        pass


class SebraParser:
    def __init__(self, parent_folder):
        self.parent_folder = parent_folder

    def get_all_excel_files(self):
        data_dir = self.parent_folder
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

    def read_into_df(self, excel_file_path):
        self.df = pd.read_excel(excel_file_path)

    # def parse_sebra_excel_file_to_pandas(self):
    def find_organizations_start_row(self, excel_file_path):
        org_start_phrases = [
            'ПЛАЩАНИЯ ПО ПЪРВОСТЕПЕННИ СИСТЕМИ В СЕБРА ',
            'ПЛАЩАНИЯ ОТ БЮДЖЕТА, ИЗВЪРШЕНИ ЧРЕЗ СЕБРА, ПО ПЪРВОСТЕПЕННИ СИСТЕМИ'
        ]
        correct = False
        for t in org_start_phrases:
            mask = self.df.iloc[:, 0] == t
            if mask.sum() == 1:
                correct = True
                break
        assert correct, f'There should be exactly one row containing any of the phrases {org_start_phrases}. File "{excel_file_path}".'
        return mask[mask].index[0]
    
    def find_operations_blocks(self, org_start_row, excel_file_path):
        op_start_text = 'Описание'
        op_end_text = 'Общо:'
        mask_start = self.df.iloc[:, 1] == op_start_text
        mask_end = self.df.iloc[:, 0].str.startswith(op_end_text).fillna(False)
        op_start_index = mask_start[mask_start].index
        op_end_index = mask_end[mask_end].index
        
        if not (len(op_start_index) == len(op_end_index)):
            print(op_start_index, op_end_index)
            # display(df)
            
        assert len(op_start_index) == len(op_end_index), f'The number of occurances of "{op_start_text}" is not the same as the number of occurances of "{op_end_text}". File "{excel_file_path}".'
        op_blocks = list(zip(op_start_index, op_end_index))
        op_blocks_dict = {
            'general': [],
            'organizations': []
        }
        for i in range(len(op_blocks)):
            b_start, b_end = op_blocks[i]
            assert b_start < b_end, f'Block start index ({b_start}) should be smaller that the end index ({b_end}). File "{excel_file_path}".'
            if i > 0:
                _, prev_b_end = op_blocks[i - 1]
                assert prev_b_end < b_start, f'Block end index ({prev_b_end}) of the previous block should be smaller that the start index of the next block ({b_start}). File "{excel_file_path}".'
            assert not (b_start < org_start_row and org_start_row < b_end), f'The starting row for organizations shouldn\'t be within an operations block. File "{excel_file_path}".'

            if b_end < org_start_row:
                op_blocks_dict['general'].append((b_start, b_end))
            else:
                op_blocks_dict['organizations'].append((b_start, b_end))

            assert len(op_blocks_dict['general']) == 1, 'There should be exactly one block for general totals. File "{excel_file_path}".'

        return op_blocks_dict

    def get_general_totals(self, general_op_block, excel_file_path):
        new_columns = ['Operations Code', 'Operations Description', 'Operations Count', 'Operations Amount (BGN)']
        b_start, b_end = general_op_block
        block_df = self.df.iloc[b_start + 1: b_end].copy()
        block_df.columns = new_columns
        del block_df['Operations Count']

        # drop all rows that do not have amount value
        block_df = block_df[~block_df['Operations Amount (BGN)'].isna()]

        block_df['Operations Amount (BGN)'] = block_df['Operations Amount (BGN)'] \
            .astype(str) \
            .str.replace(' ', '') \
            .str.replace(',', '.') \
            .astype('float64')
        block_df = block_df.set_index('Operations Code')

        # check if sums are correct
        sum_row = self.df.iloc[b_end, :]
        sum_row.index = new_columns
        if isinstance(sum_row['Operations Amount (BGN)'], str):
            sum_row['Operations Amount (BGN)'] = float(
                sum_row['Operations Amount (BGN)'] \
                    .replace(' ', '') \
                    .replace(',', '.')
            )
        if not round(sum_row['Operations Amount (BGN)'], 2) == round(block_df['Operations Amount (BGN)'].sum(), 2):
            print(f"Warning: The sums of \"Operations Amount (BGN)\" do not match for block ({b_start}, {b_end}). Expected value \"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value \"{round(block_df['Operations Amount (BGN)'].sum(), 2)}\". File \"{excel_file_path}\".")

        return block_df

    def get_organization_name_and__period(self, b_start, excel_file_path):
        block_header = self.df.iloc[b_start - 1]
        org_name = block_header[0]
        if isinstance(block_header[2], str):
            # this is the standard case - the header is just one row
            period = block_header[2].split(' ')
        else:
            # this is a rare case where the header takes two rows
            block_header_1 = self.df.iloc[b_start - 2]
            # concat the organization name along the two rows
            assert isinstance(block_header_1[0], str), f'The first column does not contain the organization name around row {b_start} in file "{excel_file_path}".'
            org_name = block_header_1[0] + ' ' + org_name
            assert isinstance(block_header_1[2], str), f'The third column does not contain the period string around row {b_start} in file "{excel_file_path}".'
            period = block_header_1[2].split(' ')
        assert period[0] == 'Период:', f'Field for time period should start with string "Период". File "{excel_file_path}".'
        start_date = period[1]
        end_date = period[3]
        return org_name, start_date, end_date

    def get_organization_operations_blocks(self, org_op_blocks, excel_file_path):
        
    
        new_columns = ['Operations Code', 'Operations Description', 'Operations Count', 'Operations Amount (BGN)']
        org_op_blocks_dfs = []
        for b_start, b_end in org_op_blocks:
            org_df = self.df.iloc[b_start + 1: b_end, :].copy()
            assert not org_df.iloc[:, 1].isna().any(), f'There are empty cells within the operations block between rows ({b_start}, {b_end}). File "{excel_file_path}".'
            org_df.columns = new_columns
            del org_df['Operations Count']
            org_df['Operations Amount (BGN)'] = org_df['Operations Amount (BGN)'] \
                .astype(str) \
                .str.replace(' ', '') \
                .str.replace(',', '.') \
                .astype('float64')

            # get organization name and time period
            org_name, start_date, end_date = self.get_organization_name_and__period(b_start, excel_file_path)
            org_df['Organization Name'] = org_name
            org_df['Start Date'] = start_date
            org_df['End Date'] = end_date

            # check if sums are correct
            sum_row = self.df.iloc[b_end, :]
            sum_row.index = new_columns
            if isinstance(sum_row['Operations Amount (BGN)'], str):
                sum_row['Operations Amount (BGN)'] = float(
                    sum_row['Operations Amount (BGN)'] \
                        .replace(' ', '') \
                        .replace(',', '.')
                )
            if not round(sum_row['Operations Amount (BGN)'], 2) == round(org_df['Operations Amount (BGN)'].sum(), 2):
                print(f"Warning: The sums of \"Operations Amount (BGN)\" do not match for block ({b_start}, {b_end}). Expected value \"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value \"{round(org_df['Operations Amount (BGN)'].sum(), 2)}\". File \"{excel_file_path}\".")

            org_op_blocks_dfs.append(org_df)
        return pd.concat(org_op_blocks_dfs).reset_index().drop(columns=['index'])

    def check_sums(self, ops_df, general_block_df, excel_file_path):
        ops_totals_df = ops_df.groupby('Operations Code').sum()
        
        assert (ops_df['Operations Code'].dtypes == object) and (general_block_df.reset_index()['Operations Code'].dtypes == object), f'There are missing operation codes in file "{excel_file_path}".'
        
        sum_check_df = pd.merge(
            ops_totals_df,
            general_block_df,
            how='left',
            left_on='Operations Code',
            right_index=True
        )
        if not (sum_check_df['Operations Amount (BGN)_x'].round(2) == sum_check_df['Operations Amount (BGN)_y'].round(2)).all():
            print(f'Warning: Some sums do not match Operations Amount (BGN). File "{excel_file_path}".')

    def run_parser(self):
        # Get all excel files
        excel_files = self.get_all_excel_files()

        dfs = []
        for i, f in enumerate(excel_files):
            print(i, f)

            # Read data
            self.read_into_df(f)

            # get the row that indicates the start of the organizations section
            org_start_row = self.find_organizations_start_row(f)
            
            # get the start and end rows for all blocks of operations
            op_blocks = self.find_operations_blocks(org_start_row, f)
            
            # parse the data containing the general totals for all operations in the file
            # (we will use those just to check the sums at the end for correctness)
            general_block_df = self.get_general_totals(op_blocks['general'][0], f)
            
            # parse all operations by organization in the file
            ops_df = self.get_organization_operations_blocks(op_blocks['organizations'], f)
            
            # check if the sums for all opearions by organization match the general totals
            self.check_sums(ops_df, general_block_df, f)

            # extract the Organization ID from the Name
            ops_df['Organization ID'] = ops_df['Organization Name'].str.findall(r'\((.*?)\)').map(
                lambda x: x[-1].strip() if len(x) > 0 else np.nan
            )

            dfs.append(ops_df)
        
        df = pd.concat(dfs)
        return df
        

def main():
    file_loc = './downloaded_files'
    folders = ['/SEBRA']#['/SEBRA','/NF_SEBRA','/MF_SEBRA']
    min_year = 2020
    min_month = 1
    min_day = 1
    chrome_path = '/usr/local/bin/chromedriver'

    sd = SebraDownloader(file_loc, chrome_path, folders, 
        min_year, min_month, min_day)
    sd.get_urls()
    sd.download_reports()

    sp = SebraParser(sd.parent_location)
    ops_df = sp.run_parser()