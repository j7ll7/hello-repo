#!/usr/bin/env python
"""Utility script to merge trend and alarm data and mark failure periods."""

import pandas as pd


def load_trend(path: str = "historian.db.F5.03.csv") -> pd.DataFrame:
    """Return melted trend data with normalized asset numbers."""
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


def main() -> None:
    trend = load_trend()
    alarms = load_alarm()
    merged = merge_data(trend, alarms)
    merged = add_alarm_flags(merged)
    merged = mark_failures(merged)
    merged.to_csv("merged_with_alarm_flag.csv", index=False)
    print(merged.count())


if __name__ == "__main__":
    main()
