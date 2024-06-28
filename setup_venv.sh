#!/bin/bash

set -e # exit if any command fails. Makes sure we don't pip install outside venv

# create and activate a virtualenv inside a `venv` folder
python3 -m venv venv
source venv/bin/activate

# install package defined in current project in venv
pip install .
