import configparser
import os
from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
import pyodbc
from dateutil import parser

ALARM_FILE = "alarm.viewer.F4.09.xlsx"
TREND_CSV = "historian.db.F5.03.csv"


def robust_parse(value):
    """Parse dates that may be Excel serials or various strings."""
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, (int, float)):
        return pd.to_datetime("1899-12-30") + pd.to_timedelta(value, unit="D")
    return parser.parse(str(value), dayfirst=True)


def load_313pt0_alarms(path: str = ALARM_FILE) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    alarm = df[df["TagName"].str.contains("313PT0", na=False)].copy()
    alarm = alarm[alarm["MessageText"].str.contains(r"HiHi level|LoLo level", na=False, regex=True)]
    alarm = alarm[["DateTime", "TagName"]].copy()
    alarm["DateTime"] = alarm["DateTime"].apply(robust_parse).dt.ceil("5S")
    parts = alarm["TagName"].str.split(".", expand=True)
    alarm["asset"] = parts[0] + "." + parts[1]
    alarm["alarm"] = parts[2]
    alarm = alarm[["DateTime", "asset", "alarm"]].reset_index(drop=True)
    alarm["asset"] = alarm["asset"].str.extract(r"313PT0(\d+)").astype(int)
    return alarm


def fetch_trend_data(config_path: str) -> pd.DataFrame:
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    tags = cfg.get("QUERY", "tags").split(", ")
    start = cfg.get("QUERY", "start_datetime")
    end = cfg.get("QUERY", "end_datetime")

    mode = cfg.get("QUERY_SETTINGS", "ww_retrieval_mode", fallback="Cyclic")
    resolution = cfg.getint("QUERY_SETTINGS", "ww_resolution", fallback=10000)
    rule = cfg.get("QUERY_SETTINGS", "ww_quality_rule", fallback="Extended")
    version = cfg.get("QUERY_SETTINGS", "ww_version", fallback="Latest")
    threshold = cfg.getint("QUERY_SETTINGS", "production_mode_threshold", fallback=5)

    conn_str = (
        r"Driver={SQL Server};"
        r"Server=IJMHISDBS03.EDIS.TATASTEEL.COM;"
        r"Database=Runtime;"
        r"Trusted_Connection=yes;"
    )
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()

    tags_str = ", ".join(f"[{t}]" for t in tags)
    query = (
        "SET QUOTED_IDENTIFIER OFF "
        "SELECT * FROM OPENQUERY(INSQL, \"SELECT "
        "DateTime = convert(nvarchar, DateTime, 21), "
        f"{tags_str} "
        "FROM WideHistory "
        f"WHERE wwRetrievalMode = '{mode}' "
        f" AND wwResolution = {resolution} "
        f" AND wwQualityRule = '{rule}' "
        f"AND wwVersion = '{version}' "
        f"AND DateTime >= '{start}' "
        f"AND DateTime <= '{end}' "
        f"AND [PlantInformation.ProductionMode] >= {threshold} \" )"
    )

    rows: list[pyodbc.Row] = []
    cursor.execute(query)
    while True:
        chunk = cursor.fetchmany(1000)
        if not chunk:
            break
        rows.extend(chunk)
    cursor.close()
    connection.close()

    return pd.DataFrame.from_records(rows, columns=["DateTime"] + tags)


def unpivot_trend(df: pd.DataFrame) -> pd.DataFrame:
    trend = df.melt(id_vars=[df.columns[0]], var_name="asset", value_name="valve pos")
    trend.rename(columns={df.columns[0]: "DateTime"}, inplace=True)
    trend["asset"] = trend["asset"].str.extract(r"(\d+)\.Status").astype(int) - 20
    trend["DateTime"] = pd.to_datetime(trend["DateTime"])
    return trend.sort_values(["asset", "DateTime"]).reset_index(drop=True)


def merge_alarm(trend: pd.DataFrame, alarm: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(trend, alarm, on=["asset", "DateTime"], how="left")
    merged["alarm"] = merged["alarm"].map({"HiHi": 1, "LoLo": -1}).fillna(0)
    return merged


def mark_failure_period(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["asset", "DateTime"]).reset_index(drop=True)
    df["failure_period_HiHi"] = 0

    for asset in df["asset"].unique():
        group = df[df["asset"] == asset]
        ts = group["DateTime"].values
        alarm = group["alarm"].values
        valve = group["valve pos"].values

        is_v2 = valve == 2
        run2 = (~is_v2).cumsum()
        first2 = pd.Series(ts).groupby(run2).transform("first").values
        since2 = (pd.Series(ts) - first2).dt.total_seconds().values

        in_failure = False
        for i, idx in enumerate(group.index):
            if not in_failure:
                if alarm[i] == 1:
                    t0 = ts[i]
                    mask = (ts > t0) & (ts <= t0 + np.timedelta64(5, "s")) & (valve == 1)
                    if mask.any():
                        in_failure = True
                        df.at[idx, "failure_period_HiHi"] = 1
            else:
                if is_v2[i] and since2[i] >= 10:
                    in_failure = False
                else:
                    df.at[idx, "failure_period_HiHi"] = 1
    return df


def load_drive_warnings(path: str = ALARM_FILE) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    alarm = df[df["TagName"].str.contains("140M0", na=False)].copy()
    alarm = alarm[alarm["MessageText"].str.contains(r"Drive alarm", na=False, regex=True)]
    alarm = alarm[["DateTime", "TagName"]].copy()
    if alarm.empty:
        return pd.DataFrame({"DateTime": pd.Series(dtype="datetime64[ns]"),
                             "asset": pd.Series(dtype="int"),
                             "drive warning": pd.Series(dtype="int")})

    alarm["DateTime"] = alarm["DateTime"].apply(robust_parse).dt.floor("S")
    parts = alarm["TagName"].str.split(".", expand=True)
    alarm["asset"] = parts[0].str.extract(r"140M0(\d+)").astype(int)
    alarm["drive warning"] = 1
    return alarm[["DateTime", "asset", "drive warning"]].reset_index(drop=True)


def merge_drive_warnings(df: pd.DataFrame, warnings: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(df, warnings, on=["asset", "DateTime"], how="left")
    merged["drive warning"] = merged["drive warning"].fillna(0)
    return merged


def compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["DateTime", "asset"])
    grp = df.groupby("asset")

    def count_transitions(series: pd.Series, a, b) -> int:
        return ((series.shift(1) == a) & (series == b)).sum()

    high_pressure = grp["failure_period_HiHi"].apply(lambda s: count_transitions(s, 0, 1))
    low_pressure = grp["alarm"].apply(lambda s: count_transitions(s, 0, -1))
    rotary = grp["drive warning"].apply(lambda s: count_transitions(s, 0, 1))

    def first_failure_duration(asset_df: pd.DataFrame):
        ser = asset_df.sort_values("DateTime")["failure_period_HiHi"]
        times = asset_df.sort_values("DateTime")["DateTime"]
        mask_start = (ser.shift(1) == 0) & (ser == 1)
        if not mask_start.any():
            return pd.NA
        start_idx = mask_start.idxmax()
        start_time = times.loc[start_idx]
        post = ser.loc[start_idx + 1:]
        zeros = post[post == 0]
        if zeros.empty:
            return pd.NA
        end_idx = zeros.index[0]
        end_time = times.loc[end_idx]
        return (end_time - start_time).total_seconds() / 60.0

    durations = grp.apply(first_failure_duration)

    summary = pd.concat([
        high_pressure.rename("High pressure failures"),
        low_pressure.rename("Low pressure failures"),
        rotary.rename("Rotary feeder failures"),
        durations.rename("failure time HiHi injector (min)")
    ], axis=1).reset_index()
    return summary


def main(config_path: str = "input_data.ini") -> None:
    alarm_313 = load_313pt0_alarms(ALARM_FILE)
    trend_raw = fetch_trend_data(config_path)
    trend = unpivot_trend(trend_raw)
    merged = merge_alarm(trend, alarm_313)
    merged = mark_failure_period(merged)

    drive_warn = load_drive_warnings(ALARM_FILE)
    merged = merge_drive_warnings(merged, drive_warn)
    merged.to_csv("merged_df.csv", index=False)

    summary = compute_summary(merged)
    summary.to_excel("failures_summary.xlsx", index=False)


if __name__ == "__main__":
    main()
