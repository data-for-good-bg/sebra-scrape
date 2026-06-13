This directory contains the python application which is used for
collecting SEBRA xlsx files in a google drive folder.

# Setting up the local dev environment

## with uv
The easiest way to run it is to use the `uv` tool.

After cloning the repository one can run the following commands:
```bash
cd <sebra-scrape-repo-root>/sebra_scrape
uv sync
uv run python -m sebra_scrape.app --help
```

# with python venv

NB: The `requirements.txt` file is manually created by copying the python dependencies
from the `pyproject.toml`. It could be outdated.

```bash
cd <sebra-scrape-repo-root>/sebra_scrape
mkdir venv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m sebra_scrape.app --help
```

# The main app commands

The app.py module (which can be started with `python -m sebra_scrape.app --help`)
is a CLI app with several commands.


The most important commands are `collect-to-gdrive` and `collect-to-gdrive-for-days`.

## collect-to-gdrive

The first one accepts a date in the form `yyyy-mm-dd` and a local target
directory.

It also requires two environment variables:
* SEBRA_GOOGLE_DRIVE_FOLDER_URL - url to the google drive in the form `https://drive.google.com/drive/folders/<id>`
* SEBRA_GOOGLE_DRIVE_CREDS - full path to a json file with the service account credentials

Once the command is started it will:
* try to download the files locally in `<specified-local-target-dir>/<date>`
  * in the same directory it may create `unsuccessful-<date>.txt` in case it
    fails to download any of the found `xlsx` files
  * in the same directory it may create `no-files-found.txt` in case there
    were not found any links pointing to `xlsx` files in the page
* upload all the files found `<specified-local-target-dir>/<date>`

If `--use-local-cache` is specified in subsequent execution for the same date
the app will first look `<specified-local-target-dir>/<date>` and if there
are three xlsx files, it will upload them and will skip scraping the target
page.


## collect-to-gdrive-for-days

This is an extension of the first command which allows with single
execution of the app to collect the xlsx files for multiple days by
specifying an additional argument `--lookback-days`.

## Running collect-go-drive* commands in parallel

In theory it is possible, in practice one should be careful with the dates
which run in parallel.
Google Drive API allows creating two folders with the same name, so
if you run it in parallel for two sequential dates, let say `2026-05-01` and
`2026-05-02` you might end up with two `2026` directories or two month `05`
directories.


# Running the apps in Airflow

## DAGs

The `dag/` directory contains a py file with definitions of four
Airflow DAGs.
* Two testing dags
  * `sebra_scrape_manual_test` - executes `collect-to-gdrive` for a specified date
  * `sebra_scrape_monday_test` - executes `collect-to-gdrive-for-days` every Monday,
     and collects data for the previous week
  * both are configured to collect the files into the folder `test-folder` in the shared drive `sebra`
* Two official dags
  * `sebra_scrape_manual` - executes `collect-to-gdrive` for a specified date
  * `sebra_scrape_monday` - executes `collect-to-gdrive-for-days` every Monday,
     and collects data for the previous week
  * both are configured to collect the files into the folder `sebra` in the shared drive `sebra`

All the DAGs are using the BashOperator, which means that Airflow runs
a bash script which runs our python application.

This is the easiest way to run a python DAG which has its own dependencies
installed in separate virtual env.

## Airflow machine organization

Once you ssh in the machine, switch to the `airflow` user.

On the airflow machine:
* The project is pulled in `/home/airflow/airflow/projects/sebra-scrape`
* Its virtual env is created under `/home/airflow/airflow/venvs/sebra_scrape`
  (see also below `VENVS_ROOT` and `AIRFLOW_VENV_DIR`)
* In the Airflow UI, menu Admin/Variables are specified the variables for
  google drive folders and google service account credentials.
* In `/home/airflow/airflow/secrets/var.json` are added other variables
  like `WORKDIR_ROOT`, `VENVS_ROOT` and `ZYTE_API` on which the DAGs depend

One can use the `bin/install_dag.sh` script to create the venv and install
the dag definition file in the directory from where Airflow will pick it up.

Before running the script one should execute these commands:
```bash
export AIRFLOW_DAG_DIR=/home/airflow/airflow/dags
export AIRFLOW_VENV_DIR=/home/airflow/airflow/venvs
export AIRFLOW_VERSION=2.8.4
```

### Backfill with airflow CLI

One should run this in screen or tmux session, because the CLI will be
managing creation of DAG runs.

One should ssh in the airflow machine, then switch to the airflow user
and finally run:
```bash
airflow dags backfill \
  --continue-on-failures \
  -s <yyyy-mm-dd> \
  -e <yyyy-mm-dd> \
  sebra_scrape_monday \
  --rerun-failed-tasks \
  --run-backwards
```

This command will execute a DAG run for each moment between the two dates
which also matches the configured schedule of the specified DAG
For the `sebra_scrape_monday` this means every Monday.

Airflow will not re-run a run for certain Monday if such already exists.
If you want to re-run the failed, then you can pass the `--rerun-failed-tasks`.

The `--continue-on-failures` means that if a DAG run fails, airflow backfill
will continue with the execution of the other runs.

The `--run-backwards` means that airflow will create runs from the end date going
backwards to the start date.


# Scraping

The app uses zyte.com service for passing through the captcha on the target
site.

The DAG is scheduled in the evening hours in order to not load the target
site during business hours.
