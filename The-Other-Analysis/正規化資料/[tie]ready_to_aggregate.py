import pandas as pd
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import filter_invalid_stop_id, pivot_table_by_hour

# Config
YM = '202403'
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/tie'
OUTPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/tie'

# Load data
tie_by_date_by_hour_by_stop = pd.read_csv(
    f'{INPUT_PATH}/tie_by_stop_by_date_by_hour.csv'
)
date_time = pd.to_datetime(tie_by_date_by_hour_by_stop['date'])
tie_by_date_by_hour_by_stop['weekday'] = date_time.dt.weekday
tie_by_date_by_hour_by_stop['weekday_type'] = tie_by_date_by_hour_by_stop['weekday'].apply(
    lambda x: 'weekday' if x <= 5 else 'weekend'
)

# Filter
tie_by_date_by_hour_by_stop = filter_invalid_stop_id(tie_by_date_by_hour_by_stop)
is_active = (tie_by_date_by_hour_by_stop['dist'] != '台北市暫停營運專區')
tie_by_date_by_hour_by_stop = tie_by_date_by_hour_by_stop.loc[is_active]

# by_weekday_type_by_hour_by_stop
agg_by_weekday_type_by_hour_by_stop = tie_by_date_by_hour_by_stop.groupby(
    ['stop_id', 'weekday_type', 'hour']
).agg({
    'dist': 'first',
    'stop_name': 'first',
    'tie_bike': 'sum'
}).reset_index()

# Pivot tie_bike table by hour
index_cols = ['dist', 'stop_id', 'stop_name', 'weekday_type']
tie_pivot_by_hour = pivot_table_by_hour(
    agg_by_weekday_type_by_hour_by_stop, 'tie_bike', index_cols
)

# Save agg data
agg_by_weekday_type_by_hour_by_stop.to_csv(
    f'{OUTPUT_PATH}/tie_agg_by_weekday_type_by_hour_by_stop.csv',
    index=False, encoding='utf-8'
)
tie_pivot_by_hour.to_csv(
    f'{OUTPUT_PATH}/tie_bike_agg_by_weekday_type_pivot_by_hour.csv',
    index=False, encoding='utf-8'
)