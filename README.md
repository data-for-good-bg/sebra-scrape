# sebra-scrape
Scraper and parser to obtain data from SEBRA 'https://www.minfin.bg/bg/transparency/'

The project does the following:

1. Crawls the reports from the ministry website since the date of the last downloaded report
2. Downloads and renames the reports
3. Downloads the previously parsed CSV from Google Drive
4. Parses the newly downloaded reports into a large data frame
5. Uploads the raw reports and large data frame with all historic data to Google Drive
6. Appends the DfG POSTGRESQL database with the newly downloaded and parsed reports. DB only has the parsed data, raw data is stored in Google Drive

## Installation

1. Clone the repo
2. Fill in the USR and PASS variables in `sample.env` file with your DB credentials and save as `.env`. If you don't have these credentials, contact a member of the administration to obtain them.
3. Download the `service_acct.json` file from here: https://drive.google.com/file/d/1GwnCsSQLM6XaPGhFGCb4jRLltWOJLYd-/view?usp=sharing If you rename this file (or download it to another location), make sure to rename the relevant bit in the `sebra_pipeline.py`.
4. Install the Python packages in `requirements.txt`

## Notes and Known Issues
1. All paths in the project are RELATIVE, so if your development environment does not automatically change the working directory to the main directory of this repo, make sure you switch before you run the pipeline
2. Sometimes the parser fails to assert code equivalence, in this case the data for that day is not parsed. __Since the collection always starts from the latest date of collection, historical dates would be missing.__ TODO: Do not start from the latest date but crawl all missing dates from the start.

## Contact
For details please contact elvan.aydemir@data-for-good.bg 