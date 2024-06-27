import pandas as pd
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import pivot_table_by_hour

# Config
YM = '202311'
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/availability'
OUTPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/availability'

# Load data
ava_by_date_by_hour_by_stop = pd.read_csv(
    f'{INPUT_PATH}/availability_by_stop_by_date_by_hour.csv'
)
date_time = pd.to_datetime(ava_by_date_by_hour_by_stop['date'])
ava_by_date_by_hour_by_stop['weekday'] = date_time.dt.weekday
ava_by_date_by_hour_by_stop['weekday_type'] = ava_by_date_by_hour_by_stop['weekday'].apply(
    lambda x: 'weekday' if x <= 5 else 'weekend'
)

# by_weekday_type_by_hour_by_stop
agg_by_weekday_type_by_hour_by_stop = ava_by_date_by_hour_by_stop.groupby(
    ['stop_id', 'weekday_type', 'hour']
).agg({
    'dist': 'first',
    'stop_name': 'first',
    'empty_minute': 'mean',
    'full_minute': 'mean'
}).reset_index()

# Pivot empty_minute table by hour
index_cols = ['dist', 'stop_id', 'stop_name', 'weekday_type']
empty_pivot_by_hour = pivot_table_by_hour(
    agg_by_weekday_type_by_hour_by_stop, 'empty_minute', index_cols
)
full_pivot_by_hour = pivot_table_by_hour(
    agg_by_weekday_type_by_hour_by_stop, 'full_minute', index_cols
)

# Save agg data
agg_by_weekday_type_by_hour_by_stop.to_csv(
    OUTPUT_PATH+'/availability_agg_by_weekday_type_by_hour_by_stop.csv',
    index=False, encoding='utf-8'
)
empty_pivot_by_hour.to_csv(
    OUTPUT_PATH+'/empty_minute_agg_by_weekday_type_pivot_by_hour.csv',
    index=False, encoding='utf-8'
)
full_pivot_by_hour.to_csv(
    OUTPUT_PATH+'/full_minute_agg_by_weekday_type_pivot_by_hour.csv',
    index=False, encoding='utf-8'
)