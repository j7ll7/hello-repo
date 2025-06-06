# Hello Repo

This repository demonstrates how to combine process trend data with discrete alarm logs to highlight periods of equipment failure. The project consists of two main artifacts:

* **alarm.py** – a Python script generated from the notebook that reads historian data from the INSQL database and merges it with alarm records.
* **Alarm.ipynb** – a Jupyter notebook containing a step-by-step version of the workflow with additional calculations and summary statistics.

## Notebook workflow

The notebook works through the following stages:

1. **Alarm extraction** – Reads an Excel export (`alarm.viewer.F4.09.xlsx`) and filters for tags containing `313PT0`. Only "HiHi level" or "LoLo level" messages are kept. Alarm exports are in month‑day‑year format, so timestamps are parsed with `dayfirst=False`. Dates are handled robustly to accommodate Excel serial values and other string formats. The timestamp is rounded to the next five‑second boundary and the tag is split to create separate `asset` and `alarm` columns.
2. **Trend retrieval** – Uses settings from `input_data.ini` to query the INSQL database via `pyodbc`. The data is fetched in chunks, concatenated and then unpivoted into a long format with `asset` and `valve pos` columns. Asset numbers are normalised by removing suffixes such as `.Status` and converting to integers.
3. **Data merge** – Trend and alarm tables are merged on `asset` and timestamp to align events with their corresponding process values. Alarm text is mapped so that `HiHi` becomes `1`, `LoLo` becomes `-1` and missing values become `0`.
4. **Failure detection** – For each asset the notebook searches for a high‑pressure alarm followed by `valve pos` equal to `1` within five seconds. Once triggered, the failure state persists until `valve pos` remains `2` for at least ten seconds. This yields a `failure_period_HiHi` flag indicating times when a device is considered failed.
5. **Additional drive warnings** – A second pass through the alarm file extracts rows for `140M0` drive alarms. These are converted into a `drive warning` flag and merged with the previous data set.
6. **Summary metrics** – The final section counts the number of transitions into failure states and calculates the duration of the first failure for each asset. The results are assembled into a summary table and exported to `failures_summary.xlsx`.

Both the notebook and script produce a merged CSV file with the computed alarm flags and failure periods for further analysis.

## Configuration

`input_data.ini` specifies the historian tags and query parameters for database queries.

## Requirements

Python 3.7+ with the `pyodbc`, `pandas`, and `openpyxl` packages.
