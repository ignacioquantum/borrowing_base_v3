#!/bin/bash

pip3 install -r test_requirements.txt

python3 -m pytest -vv src/tests/ --disable-warnings


