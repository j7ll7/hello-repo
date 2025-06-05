#!/usr/bin/env python
# coding: utf-8

# In[191]:


#alle packages
import pandas as pd


# In[192]:


# 1. Read the CSV, parsing the DateTime column as datetime64
df = pd.read_csv(
    'historian.db.F5.03.csv',
    parse_dates=['DateTime']        # ← this ensures df['DateTime'] is datetime64[ns]
)

# 2. Melt into long format
trend = df.melt(
    id_vars=['DateTime'],
    var_name='asset',
    value_name='valve_pos'
)

# 3. Clean up column names
trend['asset'] = trend['asset'].str.replace(r'\.Status$', '', regex=True)

# 4. Rename columns to match your desired output
trend.rename(columns={
    'DateTime': 'dt', 
    'valve_pos': 'valve pos'
}, inplace=True)

# 5. (Optional) Display or save the result
print(trend.head())
# trend.to_csv('trend.csv', index=False)


# In[193]:


# assuming df is your big or small DataFrame
trend['asset'] = trend['asset'].str[-3:].astype(int) - 20


# In[194]:


print(trend.sort_values(by = 'dt').head)


# In[195]:


def load_alarm_dataframe(filepath: str = 'alarm.viewer.F5.03.xlsx') -> pd.DataFrame:
    """
    Loads an Excel file of alarms, parses TagName into Tag and Alarm,
    and returns a DataFrame with DateTime, Tag and Alarm columns.
    """
    # Read in the Excel file
    df = pd.read_excel(filepath, engine='openpyxl')

    
    # Ensure DateTime is a datetime dtype
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    
    # Split TagName into three parts: prefix1, prefix2, and alarm name
    parts = df['TagName'].str.split('.', n=2, expand=True)
    
    # Recombine first two parts for Tag and take the third part as Alarm
    df['Tag']   = parts[0] + '.' + parts[1]
    df['Alarm'] = parts[2]
    
    # Return only the columns you care about
    return df[['DateTime','Tag','Alarm']]
    

# --- usage elsewhere in your code ---
alarm_df = load_alarm_dataframe()   # now `alarm_df` holds your processed data

alarm_df.rename(columns={'DateTime': 'dt'}, inplace=True)

# in-place overwrite:
alarm_df['Tag'] = (
    alarm_df['Tag']
      .str.extract(r'(\d{3})\.PV$')[0]  # grab the "001"… "012"
      .astype(int)                      # turn "001"→1, … "012"→12
)


# You can reuse `alarm_df` anytime later:
print(alarm_df)


# In[198]:


# --- 2) Extract base Tag and map to asset in alarm_df ---
#   e.g. "313PT009.PV" → "313PT009" → maps to "313XV029"
alarm_df['asset'] = (
    alarm_df['Tag']
)

# --- 3) Normalize both DataFrames: dt → DateTime, parse, sort by asset & time ---
def prep(df):
    # drop any duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    # rename 'dt' → 'DateTime'
    if 'dt' in df.columns:
        df = df.rename(columns={'dt':'DateTime'})
    # ensure DateTime exists
    if 'DateTime' not in df.columns:
        raise KeyError(f"'DateTime' not found in {df.columns.tolist()}")
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    return df

trend    = prep(trend)
alarm_df = prep(alarm_df)

trend    = trend   .sort_values(['asset','DateTime']).reset_index(drop=True)
alarm_df = alarm_df.sort_values(['asset','DateTime']).reset_index(drop=True)




# In[209]:


import pandas as pd

# ensure DateTime is datetime, and asset_id is int in both tables
for df in (trend, alarm_df):
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df['asset'] = df['asset'].astype(int)

# helper to asof-merge one asset's worth of rows
def asof_for_asset(asset, trend_grp):
    # take only the alarm rows for this asset
    alarms = alarm_df[alarm_df['asset'] == asset]
    # both must be sorted by time for merge_asof
    trend_grp = trend_grp.sort_values('DateTime')
    alarms    = alarms.sort_values('DateTime')
    # merge—the last alarm at or before each trend timestamp
    return pd.merge_asof(
        trend_grp,
        alarms,
        on='DateTime',
        direction='backward',
        suffixes=('','_alarm')
    )

# apply per-asset and reassemble
merged = (
    trend
      .groupby('asset', group_keys=False)
      .apply(lambda grp: asof_for_asset(grp.name, grp))
      .reset_index(drop=True)
)


# In[210]:


merged.count(
)


# In[ ]:


merged = pd.merge(
    trend,
    alarm_df,
    on=['DateTime','asset'],
    how='left'          # ← keeps every row from big_df
)


# In[ ]:


counts = merged.count()
print(counts)


# In[ ]:


# 1. Make sure your DataFrame is sorted by time
merged = merged.sort_values('DateTime')

# 2. Mark the times when an alarm appears
#    We’ll create a helper column that is the timestamp when alarm is non-empty,
#    and NaT everywhere else.
merged['alarm_time'] = merged['DateTime'].where(merged['Alarm'].notna() & (merged['Alarm'].str.strip() != ''))

# 3. Propagate (forward-fill) that timestamp to subsequent rows
merged['alarm_time'] = merged['alarm_time'].ffill()

# 4. Compute flag: 1 if current row is within 5 seconds of the last alarm_time
merged['alarm_flag'] = (
    (merged['DateTime'] - merged['alarm_time']) <= pd.Timedelta(seconds=5)
).fillna(False).astype(int)



# In[ ]:


# 1. Make sure your rows are in chronological order
merged = merged.sort_values(['asset','DateTime']).reset_index(drop=True)

# 2. Prepare the output column and a little “state” variable
merged['failure_period'] = 0
in_failure = 0

# 3. Walk through each row and update the state
for i, row in merged.iterrows():
    # if we’re not in a failure, check for the start condition
    if in_failure == 0 and row['alarm_flag'] == 1 and row['valve pos'] == 1:
        in_failure = 1
    # if we are in a failure, check for the stop condition
    elif in_failure == 1 and row['valve pos'] == 2:
        in_failure = 0

    # write the current state into the new column
    merged.at[i, 'failure_period'] = in_failure


# In[ ]:


merged.info()


# In[ ]:


# save without the index column
merged.to_csv('merged_with_alarm_flag.csv', index=False)


# In[ ]:


counts = merged.count()
print(counts)

