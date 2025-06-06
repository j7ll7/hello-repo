#!/usr/bin/env python
from __future__ import annotations
"""Utility script to merge trend and alarm data and mark failure periods."""

import configparser
import pandas as pd
import pyodbc


def fetch_trend_db(config_path: str = "input_data.ini") -> pd.DataFrame:
    """Fetch trend data from INSQL using settings in the given config file."""
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    tags = cfg.get("QUERY", "tags").split(", ")
    start = cfg.get("QUERY", "start_datetime")
    end = cfg.get("QUERY", "end_datetime")

    mode = cfg.get("QUERY_SETTINGS", "ww_retrieval_mode", fallback="Cyclic")
    res = cfg.getint("QUERY_SETTINGS", "ww_resolution", fallback=10000)
    rule = cfg.get("QUERY_SETTINGS", "ww_quality_rule", fallback="Extended")
    ver = cfg.get("QUERY_SETTINGS", "ww_version", fallback="Latest")
    threshold = cfg.getint("QUERY_SETTINGS", "production_mode_threshold", fallback=5)

    conn = pyodbc.connect(
        r"Driver={SQL Server};Server=IJMHISDBS03.EDIS.TATASTEEL.COM;Database=Runtime;Trusted_Connection=yes;"
    )
    cursor = conn.cursor()

    tags_str = ", ".join(f"[{tag}]" for tag in tags)
    query = (
        "SET QUOTED_IDENTIFIER OFF "
        "SELECT * FROM OPENQUERY(INSQL, \"SELECT "
        "DateTime = convert(nvarchar, DateTime, 21), "
        f"{tags_str} "
        "FROM WideHistory "
        f"WHERE wwRetrievalMode = '{mode}' "
        f" AND wwResolution = {res} "
        f" AND wwQualityRule = '{rule}' "
        f"AND wwVersion = '{ver}' "
        f"AND DateTime >= '{start}' "
        f"AND DateTime <= '{end}' "
        f"AND [PlantInformation.ProductionMode] >= {threshold} \" )"
    )

    cursor.execute(query)
    rows: list[pyodbc.Row] = []
    while True:
        chunk = cursor.fetchmany(1000)
        if not chunk:
            break
        rows.extend(chunk)
    cursor.close()
    conn.close()

    df = pd.DataFrame.from_records(rows, columns=["DateTime"] + tags)
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    return df


def load_trend(path: str = "historian.db.F5.03.csv", config_path: str | None = None) -> pd.DataFrame:
    """Return melted trend data from CSV or database with normalized asset numbers."""
    if config_path:
        df = fetch_trend_db(config_path)
    else:
        df = pd.read_csv(path, parse_dates=["DateTime"])
    trend = df.melt(id_vars=["DateTime"], var_name="asset", value_name="valve pos")
    trend["asset"] = (
        trend["asset"].str.replace(r"\.Status$", "", regex=True).str[-3:].astype(int) - 20
    )
    trend.rename(columns={"DateTime": "dt"}, inplace=True)
    return trend.sort_values("dt").reset_index(drop=True)


def load_alarm(path: str = "alarm.viewer.F5.03.xlsx") -> pd.DataFrame:
    """Return alarm data with extracted asset numbers."""
    df = pd.read_excel(path, engine="openpyxl")
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    parts = df["TagName"].str.split(".", n=2, expand=True)
    alarm = pd.DataFrame(
        {
            "DateTime": df["DateTime"],
            "asset": parts[0] + "." + parts[1],
            "Alarm": parts[2],
        }
    )
    alarm["asset"] = alarm["asset"].str.extract(r"(\d{3})\.PV")[0].astype(int)
    return alarm


def merge_data(trend: pd.DataFrame, alarm: pd.DataFrame) -> pd.DataFrame:
    trend = trend.rename(columns={"dt": "DateTime"})
    merged = pd.merge(trend, alarm, on=["DateTime", "asset"], how="left")
    return merged


def add_alarm_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("DateTime").reset_index(drop=True)
    mask = df["Alarm"].notna() & df["Alarm"].str.strip().ne("")
    df["alarm_time"] = df["DateTime"].where(mask).ffill()
    df["alarm_flag"] = (
        (df["DateTime"] - df["alarm_time"] <= pd.Timedelta(seconds=5))
        .fillna(False)
        .astype(int)
    )
    return df.drop(columns="alarm_time")


def mark_failures(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["asset", "DateTime"]).reset_index(drop=True)
    failure_state = 0
    periods = []
    for flag, valve in zip(df["alarm_flag"], df["valve pos"]):
        if not failure_state and flag == 1 and valve == 1:
            failure_state = 1
        elif failure_state and valve == 2:
            failure_state = 0
        periods.append(failure_state)
    df["failure_period"] = periods
    return df


def main(config_path: str | None = None) -> None:
    trend = load_trend(config_path=config_path)
    alarms = load_alarm()
    merged = merge_data(trend, alarms)
    merged = add_alarm_flags(merged)
    merged = mark_failures(merged)
    merged.to_csv("merged_with_alarm_flag.csv", index=False)
    print(merged.count())


if __name__ == "__main__":
    import sys

    cfg = sys.argv[1] if len(sys.argv) > 1 else None
    main(config_path=cfg)
