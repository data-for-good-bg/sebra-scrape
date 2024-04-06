import pendulum

from airflow.decorators import dag, task

import os
import sys

sebra_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(sebra_path)

from src.sebra_scrape.constants import (
    SEBRA_ID_STRINGS,
    SEBRA_FILE_DESTINATIONS,
    SEBRA_REPORT_ID_STRING,
)


# TODO: read from env?
PATH_TO_SEBRA_PYTHON_BINARY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "venv", "bin", "python"
)

# TODO: read from env/config
SEBRA_REPORTS_LOCAL_DOWNLOAD_DIR = (
    "/home/yasen/workspace/dataforgood/sebra-scrape/downloaded_files"
)


# TODO: set up schedule such that we do not scrape weekends and (?)national holidays(?)
# TODO: fix logging
@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 2, tz="UTC"),
    catchup=False,
    tags=["example", "sebra"],
)
def sebra_minfin_etl():
    """
    ### Extract and process SEBRA reports from minfin.bg
    """

    def upload_unparsable_file_to_dgrive(context):
        print(f"oops, we couldn't parse a file! Context: {context}.")

    @task.external_python(python=PATH_TO_SEBRA_PYTHON_BINARY, multiple_outputs=True)
    def fetch_sebra_report_links_for_date(*, ds):
        from sebra_scrape.download import extract_links
        from datetime import datetime

        return extract_links(datetime.strptime(ds, "%Y-%m-%d").date())

    @task.external_python(
        python=PATH_TO_SEBRA_PYTHON_BINARY,
        on_failure_callback=upload_unparsable_file_to_gdrive,
    )
    def download_sebra_report(url, destination_dir):
        print(f"Downloading SEBRA report from {url}...")
        from sebra_scrape.download import download_report

        # TODO: config download destination
        downloaded_file_path = download_report(
            destination_dir=destination_dir,
            param_string=url,
        )

        return downloaded_file_path

    @task.external_python(python=PATH_TO_SEBRA_PYTHON_BINARY, multiple_outputs=True)
    def parse_sebra_report(raw_file_path):
        import pandas as pd
        from sebra_scrape.parse import parse_sebra_report

        raw_df = pd.read_excel(raw_file_path)
        parsed_df = parse_sebra_report(raw_df)
        # TODO: save to GDrive
        print(parsed_df)

    sebra_report_download_links = fetch_sebra_report_links_for_date()
    for report_type_id in SEBRA_ID_STRINGS:
        dest_dir = os.path.join(
            SEBRA_REPORTS_LOCAL_DOWNLOAD_DIR,
            SEBRA_FILE_DESTINATIONS["raw"][report_type_id],
        )
        raw_file_path = download_sebra_report.override(
            task_id=f"download_report_{report_type_id}"
        )(destination_dir=dest_dir, url=sebra_report_download_links[report_type_id])
        # TOOD: Should we enable parsing of other reports?
        if report_type_id == SEBRA_REPORT_ID_STRING:
            parse_sebra_report(raw_file_path)


sebra_minfin_etl()
