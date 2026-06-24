import os
import csv
import json
import re
import logging

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from gdrive import GoogleDriveUploader, DriveFile
from .xlsx_parser import parse_sebra_payments_xlsx


logger = logging.getLogger(__name__)


# Helper functions for date parsing

def _parse_ddmmyyyy_date(date_str: str) -> str:
    """
    Parse a date string in ddmmyyyy format and return in yyyy-mm-dd format.

    Args:
        date_str: Date string in ddmmyyyy format (e.g., '20062024')

    Returns:
        Date string in yyyy-mm-dd format (e.g., '2024-06-20')
    """
    if len(date_str) != 8:
        raise ValueError(f"Expected ddmmyyyy format (8 digits), got: {date_str}")

    day = date_str[:2]
    month = date_str[2:4]
    year = date_str[4:]

    return f"{year}-{month}-{day}"


def _extract_date_from_filename(filename: str) -> str:
    """
    Extract the date part from a SEBRA-<date>.xlsx filename.

    Args:
        filename: Filename like 'SEBRA-20062024.xlsx'

    Returns:
        Date string in ddmmyyyy format (e.g., '20062024')
    """
    match = re.match(r'^SEBRA-(\d{8})\.xlsx$', filename)
    if not match:
        raise ValueError(f"Filename doesn't match SEBRA-<date>.xlsx pattern: {filename}")
    return match.group(1)


def _get_date_iso_from_filename(filename: str) -> str:
    """
    Extract date from filename and convert to yyyy-mm-dd format.

    Args:
        filename: Filename like 'SEBRA-20062024.xlsx'

    Returns:
        Date string in yyyy-mm-dd format (e.g., '2024-06-20')
    """
    date_ddmmyyyy = _extract_date_from_filename(filename)
    return _parse_ddmmyyyy_date(date_ddmmyyyy)


# Helper functions for CSV operations

def _load_processed_csv(target_dir: str) -> list[dict]:
    """
    Load the sebra-processed-sebra-xlsx.csv file from target_dir.

    Returns:
        List of dicts with keys: relative_path, google_drive_id, processing_date
    """
    csv_path = os.path.join(target_dir, 'sebra-processed-sebra-xlsx.csv')

    if not os.path.exists(csv_path):
        return []

    processed_files = []
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_files.append(row)

    return processed_files


def _load_payments_csv(target_dir: str) -> list[dict]:
    """
    Load the sebra-payments.csv file from target_dir.

    Returns:
        List of dicts representing payment rows
    """
    csv_path = os.path.join(target_dir, 'sebra-payments.csv')

    if not os.path.exists(csv_path):
        return []

    payments = []
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            payments.append(row)

    return payments


def _save_payments_csv(target_dir: str, payments_data: list[dict]) -> None:
    """Save payment data to sebra-payments.csv in target_dir."""
    csv_path = os.path.join(target_dir, 'sebra-payments.csv')

    if not payments_data:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'start_date', 'end_date', 'operation_code',
                'operation_description', 'currency', 'amount',
                'organization_id', 'organization_name'
            ])
            writer.writeheader()
        return

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=payments_data[0].keys())
        writer.writeheader()
        writer.writerows(payments_data)


def _save_processed_csv(target_dir: str, processed_files: list[dict]) -> None:
    """Save processed file info to sebra-processed-sebra-xlsx.csv in target_dir."""
    csv_path = os.path.join(target_dir, 'sebra-processed-sebra-xlsx.csv')

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['relative_path', 'google_drive_id', 'processing_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_files)



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
    5. Each files from point 4 is processed by using _process_sebra_payment_file()
       function
    """
    gdrive_creds_path = os.environ['SEBRA_GOOGLE_DRIVE_CREDS']
    gdrive_folder_url = os.environ['SEBRA_GOOGLE_DRIVE_FOLDER_URL']

    # Initialize GoogleDriveUploader
    gd = GoogleDriveUploader(gdrive_folder_url, gdrive_creds_path)

    # Step 1: Download combined CSV files from root folder if they exist
    target_dir_path = Path(target_dir)
    target_dir_path.mkdir(parents=True, exist_ok=True)

    # Download sebra-payments.csv
    payments_file = gd.search_files_in_root(name_contains='sebra-payments.csv')
    if payments_file:
        gd.download_file(payments_file[0].file_id, str(target_dir_path / 'sebra-payments.csv'))
    else:
        # Create empty file
        with open(target_dir_path / 'sebra-payments.csv', 'w') as f:
            # TODO: add column headers?
            pass

    # Download sebra-processed-sebra-xlsx.csv
    processed_file = gd.search_files_in_root(name_contains='sebra-processed-sebra-xlsx.csv')
    if processed_file:
        gd.download_file(processed_file[0].file_id, str(target_dir_path / 'sebra-processed-sebra-xlsx.csv'))
    else:
        # Create empty file
        with open(target_dir_path / 'sebra-processed-sebra-xlsx.csv', 'w') as f:
            # TODO: add column headers
            pass

    # Step 2: Search for SEBRA-*.xlsx files (excluding SEBRA-MF-*.xlsx) in root folder
    sebra_files = gd.search_files_in_root(
        name_contains='SEBRA-',
        name_not_contains='SEBRA-MF'
    )

    # Filter to only .xlsx files
    sebra_xlsx_files = [f for f in sebra_files if f.name.endswith('.xlsx')]

    # Step 3: Load processed CSV
    processed_files = _load_processed_csv(target_dir)

    # Build set of already processed file IDs
    processed_rel_paths = set()
    for row in processed_files:
        processed_rel_paths.add(row.get('relative_path', ''))

    # Step 4: Find files to process (in Drive but not in processed list)
    files_to_process = []
    for file_info in sebra_xlsx_files:
        if file_info.relative_path not in processed_rel_paths:
            files_to_process.append(file_info)

    logger.info(f'{files_to_process=}')

    # # Step 5: Process each file
    # for file_info in files_to_process:
    #     _process_sebra_payment_file(gd, file_info, target_dir, use_local_cache)
    #     _update_combined_files(gd, file_info, target_dir, use_local_cache)


def _process_sebra_payment_file(gd: GoogleDriveUploader, file_info: DriveFile, target_dir: str, use_local_cache: bool) -> None:
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
    # Extract date from filename
    date_iso = _get_date_iso_from_filename(file_info.name)

    # Create date-based subdirectory (yyyy-mm-dd)
    date_dir = Path(target_dir) / date_iso
    date_dir.mkdir(parents=True, exist_ok=True)

    # Local xlsx path
    local_xlsx_path = date_dir / file_info.name

    # Download file if needed
    if use_local_cache and local_xlsx_path.exists():
        logger.info(f"Skipping download of {file_info.name} (local cache enabled)")
    else:
        gd.download_file(file_info.file_id, str(local_xlsx_path))

    # Parse the xlsx file
    sebra_data = parse_sebra_payments_xlsx(str(local_xlsx_path))

    # Generate output filenames with ISO date format
    date_prefix = f"SEBRA-{date_iso}"
    payments_csv_path = date_dir / f"{date_prefix}-payments.csv"
    summary_csv_path = date_dir / f"{date_prefix}-summary.csv"
    validation_json_path = date_dir / f"{date_prefix}-validation-result.json"

    # Save merged org data to payments CSV
    merged_df = sebra_data.merged_org_data()
    merged_df.to_csv(payments_csv_path, index=False)

    # Save summary data to summary CSV
    summary_df = sebra_data.summary.data
    summary_df.to_csv(summary_csv_path, index=False)

    # Save validation result to JSON
    validation_result = sebra_data.validate_total_sum()
    with open(validation_json_path, 'w', encoding='utf-8') as f:
        json.dump(validation_result, f, indent=2, ensure_ascii=False, default=str)

def _update_combined_files(gd: GoogleDriveUploader, file_info: DriveFile, target_dir: str, use_local_cache: bool) -> None:
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

    date_iso = _get_date_iso_from_filename(file_info.name)

    # Local paths
    date_dir = Path(target_dir) / date_iso
    date_prefix = f"SEBRA-{date_iso}"
    payments_csv_path = date_dir / f"{date_prefix}-payments.csv"
    summary_csv_path = date_dir / f"{date_prefix}-summary.csv"
    validation_json_path = date_dir / f"{date_prefix}-validation-result.json"

    combined_payments_path = Path(target_dir) / 'sebra-payments.csv'
    combined_processed_path = Path(target_dir) / 'sebra-processed-sebra-xlsx.csv'

    # Step 1: Update sebra-payments.csv by merging with new payments data
    existing_payments = _load_payments_csv(target_dir)

    # Load new payments data
    new_payments = []
    if payments_csv_path.exists():
        with open(payments_csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            new_payments = list(reader)

    # Combine and sort by start_date (first date column)
    combined_payments = existing_payments + new_payments
    if combined_payments and 'start_date' in combined_payments[0]:
        combined_payments.sort(key=lambda x: x.get('start_date', ''))

    # Save updated sebra-payments.csv
    _save_payments_csv(target_dir, combined_payments)

    # Step 2: Update sebra-processed-sebra-xlsx.csv
    processed_files = _load_processed_csv(target_dir)

    # Add new entry for this file
    processing_date = datetime.now().isoformat()
    new_entry = {
        'relative_path': file_info.relative_path,
        'google_drive_id': file_info.file_id,
        'processing_date': processing_date
    }
    processed_files.append(new_entry)

    # Save updated sebra-processed-sebra-xlsx.csv
    _save_processed_csv(target_dir, processed_files)

    # Step 3: Upload updated combined CSVs to root folder
    gd.upload_file_to_root(str(combined_payments_path))
    gd.upload_file_to_root(str(combined_processed_path))

    # Step 4: Upload processed files to the same folder as the xlsx
    gd.upload_file(date_iso, str(payments_csv_path))
    gd.upload_file(date_iso, str(summary_csv_path))
    gd.upload_file(date_iso, str(validation_json_path))


if __name__ == '__main__':
    etl_sebra_payments_xlsx_files(
        target_dir='/home/vitali/projects/data-for-good/sebra-scrape/sebra_scrape/temp',
        use_local_cache=True
    )
