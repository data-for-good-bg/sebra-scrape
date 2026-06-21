import os

from gdrive import GoogleDriveUploader
from xlsx_parser import parse_sebra_payments_xlsx


def etl_sebra_payments_xlsx_files(target_dir: str, use_local_cache: bool):
    """
    This function ETLs the SEBRA-<date>.xlsx files.

    In short the process is the following:
    1. It downloads the sebra-payments.csv and sebra-processed-sebra-xlsx.csv
      from the root google drive folder, if they exists
      * the files should be downloaded in the target_dir
      * if the files does not exist in google drive, empty files will be
        created in the target_dir
    2. After that using the GoogleDriveUploader it searches for files
       starting with SEBRA- but not starting with SEBRA-MF in the root google
       drive folder
    3. It loads the sebra-processed-sebra-xlsx.csv files. Each row in this CSV
       consists:
       * of relative path of a .xlsx file as it is returned by
         the GoogleDriveUploader
       * google drive ID
       * processing date
    4. Compares the list of files returned in point 2 with the list of files
       loaded in point 3. Those files which are found in the google drive, but not
       in the processed list should be processed.
    5. Each files from point 4 is processed by using _etl_sebra_payment_file()
       function
    """
    gdrive_creds_path = os.environ['SEBRA_GOOGLE_DRIVE_CREDS']
    gdrive_folder_url = os.environ['SEBRA_GOOGLE_DRIVE_FOLDER_URL']


def _process_sebra_payment_file(gd: GoogleDriveUploader, file_info: Any, target_dir: str, use_local_cache: bool) -> None:
    """
    The starts by downloading the file described in file_info in the target_dir,
    by putting it in sub-dir with name `yyyy-mm-dd`. If the use_local_cache is
    true and such file exists already locally the download step will be skipped.

    After that it executes the parse_sebra_payments_xlsx() function and:
    * stores the merged_org_data() into a csv file in the same directory
      where the local xlsx file is. The file should be named SEBRA-<date>-payments.csv
    * in the same directory it stores also the summary data frame into a file
      SEBRA-<date>-summary.csv
    * a json file representing the result output of validate_total_sum() method
      the file should be named SEBRA-<date>-validation-result.json
    """
    pass

def _update_combined_files(gd: GoogleDriveUploader, file_info: Any, target_dir: str, use_local_cache: bool) -> None:
    """
    This function updates the sebra-payments.csv and sebra-processed-sebra-xlsx.csv
    files after the xlsx file described in file_info has been processed
    using _process_sebra_payment_file.
    After that it uploads the following files into google drive:
    * the sebra-payments.csv and sebra-processed-sebra-xlsx.csv  in the root folder
    * the SEBRA-<date>-payments.csv, SEBRA-<date>-summary.csv and SEBRA-<date>-validation-result.json
      files should be uploaded in the folder where their .xlsx file is


    In the sebra-processed-xlsx.csv it will add a row for the file being etled.
    The sebra-payments.csv file should be updated by inserting the rows from
    the SEBRA-<date>-payments.csv at right place, which is decide by the
    first column in both CSV files which are dates.
    """
    pass