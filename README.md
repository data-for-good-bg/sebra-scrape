This repo contains applications, notebooks and Airflow DAGs working
with SEBRA payments data which can be found in www.minfin.bg/transparency

The current plan is that th will be two applications:
* one which will collect the `xlsx` files in a google drive folder
  * it will collect all `xlsx` available files, usually there are 3 files,
    but sometimes there are just 2
* another one which will ETL the files with name pattern `SEBRA-<date>.xlsx`
  and produce a single CSV file
* a Tablue dashboard will be used for displaying the data

In addition we will use Airflow for scheduling


# Collecting the files

The `sebra_scrape` directory contains the python application used for
collecting the files in a google drive folder.

It works like this:
* It opens the page for certain date, parses it and searches for links pointing
  to `xlsx` files
* Downloads the files in a local directory for the specified date
  * In it fails to download one or more files, it creates a filed named
    `unsuccessful-<date>.txt` containing the URLs that were not downloaded
  * In case the page for the date does not contain any `xlsx` links it creates
    a file `no-files-found.txt`
  * The `unsuccessful` and `no-files-found` would make easier to find the dates
    which may require additional execution of the app to collect the files.
    (This functionality is not implemented at the moment of writing this notes).
* Uploads the files collected in the local directory in to the specified
  google drive directory using the following layout:

  ```
  <shared google drive>/
    <dedicated root folder>/
      2026/
        04/
          10/
            no-files-found.txt
          ...
          ...
          29/
            NF_SEBRA_09.04.2026.xlsx
            SEBRA-09042026.xlsx
            SEBRA-MF-2026-04-09.xlsx
  ```

## Searching for unsuccessful-<date>.txt or no-files-found.txt

There are cases in which the captcha which runs on minfin.bg leads to
unsuccessful downloads. In these cases a file `unsuccessful-<date>.txt` is
created in the target day folder.
Similarly, sometimes the `xlsx` files might not be uploaded when the
scraper app was executed. In this case the app will create `no-files-found.txt`.

These files are markers for the days for which we would like to re-run the
app in order to get the files.

The google drive API allows to attach custom properties to files,
these properties can be used for searching files later.

Each uploaded file has a property with name `sebra_root_folder_id` and
value the `id` of the dedicated root folder where all files are uploaded.

This makes possible to query for all unsuccessful files with query like this
one (check the gdrive.py for more details):
```python
    query = (
        f"name contains 'unsuccessful-' and "
        f"properties has "
        f"  {{key='{self.PROPERTY_ROOT_FOLDER_ID}' and "
        f"    value='{self.root_folder_id}'"
        f"  }} and "
        f"trashed=false "
    )
```

Using properties is way faster than walking recursively through the whole
google drive directory.

# Google Drive organization and authentication

There's dedicated shared google drive named `sebra`.

It contains two folders:
* `sebra` - where the files are collected
* `test-sebra` - used for collecting files during development


## Authentication

Google Drive API recommendations is:
* to use a dedicated google cloud project
* to enable only the Google Drive API
* to create a service account and use it

This is what is done:
* there's a project named `sebra`
* with only `Google Drive API` enabled
* with `sebra-airflow` service account

## Granting access to change files in a shared drive

The service account email should be added as `Contributor` on the
`shared drive`. Adding it only on a folder is not sufficient.
