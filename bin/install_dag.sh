#!/usr/bin/env bash

#
# This script installs the specified module DAGs on the Airflow machine.
# 1. It creates python virtual environment under directory specified via
#    AIRFLOW_VENV_DIR env var.
#    The venv consists of:
#      * requirements described in pyproject.toml
#      * the specified module installed in editable mode
# 2. It copies the *.py files with the DAG definitions into directory specified
#    with AIRFLOW_DAG_DIR env var
#

set -euo pipefail

log() {
    echo $* >&2
}

SCRIPT_DIR="$(dirname "$0")"
REPO_DIR="$(cd "$SCRIPT_DIR/.."; pwd)"
MODULE_DIR="${1?Specify python module dir as first argument}"
MODULE_DIR="$(cd "$MODULE_DIR"; pwd)"
MODULE_NAME="$(basename $MODULE_DIR)"

log "SCRIPT_DIR: $SCRIPT_DIR"
log "REPO_DIR: $REPO_DIR"
log "MODULE_DIR: $MODULE_DIR"
log "MODULE_NAME: $MODULE_NAME"


check_var() {
    local var_name="$1"
    if [ ! -n "${!var_name+x}" ]; then
        log "Variable $var_name should be defined."
        exit 1
    fi
}

ensure_vnv() {
    local VENV_DIR
    VENV_DIR="$AIRFLOW_VENV_DIR/$MODULE_NAME"
    log "Creating $VENV_DIR directory"
    mkdir -p "$VENV_DIR"

    log "Create python venv in $VENV_DIR"
    python3 -m venv "$VENV_DIR"

    source "$VENV_DIR/bin/activate"

    log "Installing the module in editable mode"
    (
        cd "$MODULE_DIR/"
        pip install -e .
    )
}


main() {
    check_var AIRFLOW_DAG_DIR
    check_var AIRFLOW_VENV_DIR

    ensure_vnv

    cp "$MODULE_DIR"/dag/*.py "$AIRFLOW_DAG_DIR"
}

main
