# Hello Repo

`Alarm_algo.py` merges trend and alarm data from INSQL or CSV files, flags alarms within a five-second window, and marks equipment failure periods. The script can read database query settings from `input_data.ini`, which is optional and can be passed as a command-line argument.

## Requirements
Python 3.7+ with the `pyodbc`, `pandas`, and `openpyxl` packages.
