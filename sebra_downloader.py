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
        # if len(imported_dates) == 0:
        #     pass
        # else:
        #     max_imported_date = max(imported_dates)
        #     self.min_day = max_imported_date.day
        #     self.min_month = max_imported_date.month
        #     self.min_year = max_imported_date.year

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
                print(html.status_code)

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
