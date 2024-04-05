import os

# We have 3 types of reports available for download, each requiring specific processing. We use the strings below across
#   our codebase to identify these consistently
SEBRA_REPORT_ID_STRING = "sebra"
SEBRA_MF_REPORT_ID_STRING = "sebra_mf"
SEBRA_NF_REPORT_ID_STRING = "sebra_nf"
SEBRA_ID_STRINGS = [
    SEBRA_REPORT_ID_STRING,
    SEBRA_MF_REPORT_ID_STRING,
    SEBRA_NF_REPORT_ID_STRING,
]

# raw -> files that were just downloaded
# success -> files that have successfully been parsed
# to_fix -> files which failed to parse and need manual fixing
# fixed -> files which were manually fixed
SEBRA_FILE_DESTINATIONS = {
    parent_folder: {
        id_string: os.path.join(parent_folder, id_string)
        for id_string in SEBRA_ID_STRINGS
    }
    for parent_folder in ["raw", "success", "to_fix", "fixed"]
}
