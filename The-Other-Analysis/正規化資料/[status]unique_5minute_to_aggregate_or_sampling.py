import sys
import time

import pandas as pd

sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import (
    generate_hour_to_period_dict,generate_time_sequence_index
)

ym = '202403'
root_path = r'D:\iima\ubike分析'
status_path = f'{root_path}/DM/{ym}/prepared_data/status'
stime = time.time()

# load
file_path = f'{status_path}/filled_missing_value_by_5minute.csv'
ubike_status_by_1m = pd.read_csv(file_path)

# generate info
ubike_status_by_1m['data_time'] = pd.to_datetime(ubike_status_by_1m['data_time'])
ubike_status_by_1m['date'] = ubike_status_by_1m['data_time'].dt.date
ubike_status_by_1m['weekday'] = ubike_status_by_1m['data_time'].dt.weekday + 1
ubike_status_by_1m['hour'] = ubike_status_by_1m['data_time'].dt.hour
ubike_status_by_1m['is_disabled'] = ubike_status_by_1m['is_disabled'].map({True: 1, False: 0})
print(f'finished generate info, cost {time.time() - stime} secs.')

# aggregate by date by hour, 以小時呈現每分鐘的資料
status_by_date_by_hour = ubike_status_by_1m.groupby(
    ['stop_id', 'date', 'hour']
).agg(
    weekday=pd.NamedAgg(column='weekday', aggfunc='first'),
    raw_data_count=pd.NamedAgg(column='raw_data_count', aggfunc='sum'),
    raw_data_disabled_count=pd.NamedAgg(column='raw_data_disabled_count', aggfunc='sum'),
    total_minutes=pd.NamedAgg(column='is_disabled', aggfunc='count'),
    disabled_count_by1m=pd.NamedAgg(column='is_disabled', aggfunc='sum'),
    median_available_rent=pd.NamedAgg(column='available_rent_bikes', aggfunc='median'),
    median_available_return=pd.NamedAgg(column='available_return_bikes', aggfunc='median'),
    empty_minutes=pd.NamedAgg(column='is_empty', aggfunc='sum'),
    full_minutes=pd.NamedAgg(column='is_full', aggfunc='sum'),
    continuous_empty_minutes=pd.NamedAgg(column='is_continuous_empty', aggfunc='sum'),
    continuous_full_minutes=pd.NamedAgg(column='is_continuous_full', aggfunc='sum')
).reset_index()

# save
file_path = f'{status_path}/aggregate_by_date_by_hour.csv'
status_by_date_by_hour.to_csv(file_path, index=False, encoding='UTF-8')
print(f'finished by date by hour, cost {time.time() - stime} secs.')


# sampleing by weekday 用4天中的median代表weekday
status_by_weekday_by_hour = status_by_date_by_hour.groupby(['stop_id', 'weekday', 'hour']).agg({
    'raw_data_count': 'sum',
    'raw_data_disabled_count': 'sum',
    'total_minutes': 'mean',
    'disabled_count_by1m': 'mean',
    'median_available_rent': 'median',
    'median_available_return': 'median',
    'empty_minutes': 'median',
    'full_minutes': 'median',
    'continuous_empty_minutes': 'median',
    'continuous_full_minutes': 'median'
}).reset_index()
# 轉換成見車率
empty_prob = status_by_weekday_by_hour['empty_minutes']/status_by_weekday_by_hour['total_minutes']
status_by_weekday_by_hour['available_rent_prob'] = round(1-empty_prob, 2)
# ceate time index for powerbi
status_by_weekday_by_hour['wh'] = (
    status_by_weekday_by_hour['weekday'].astype(str) 
    + '_' 
    + status_by_weekday_by_hour['hour'].astype(str)
)
weekday_hour_mapping = generate_time_sequence_index((1, 8), (0, 24))
status_by_weekday_by_hour['wh_index'] = status_by_weekday_by_hour['wh'].map(weekday_hour_mapping)
# save
file_path = status_path+'/sampling_by_weekday_by_hour.csv'
status_by_weekday_by_hour.to_csv(file_path, index=False, encoding='UTF-8')
print(f'finished sampleing by weekday, cost {time.time() - stime} secs.')


# agg by weekday by period
# 每小時無必要，合併為數個時段
hour_to_period = generate_hour_to_period_dict()
status_by_weekday_by_hour['period'] = status_by_weekday_by_hour['hour'].map(
    hour_to_period)
status_by_weekday_by_period = status_by_weekday_by_hour.groupby(
    ['stop_id', 'weekday', 'period']
).agg({
    'raw_data_count': 'sum',
    'raw_data_disabled_count': 'sum',
    'total_minutes': 'sum',
    'disabled_count_by1m': 'sum',
    'median_available_rent': 'mean',
    'median_available_return': 'mean',
    'empty_minutes': 'sum',
    'full_minutes': 'sum',
    'continuous_empty_minutes': 'sum',
    'continuous_full_minutes': 'sum'
}).reset_index()
status_by_weekday_by_period = status_by_weekday_by_period.rename(columns={
    'median_available_rent': 'mean_available_rent',
    'median_available_return': 'mean_available_return'
})
# 沒車沒位合併計算太複雜了，分開兩個結果(考慮不要分群了，直接用機率篩選)
# 見車率/見位率 = (總分鐘-缺車分鐘)/總分鐘 = 1-(缺車分鐘/總分鐘)
status_by_weekday_by_period['available_rent_prob'] = 1 - (
    status_by_weekday_by_period['empty_minutes'] / status_by_weekday_by_period['total_minutes']
)
status_by_weekday_by_period['available_return_prob'] = 1 - (
    status_by_weekday_by_period['full_minutes'] / status_by_weekday_by_period['total_minutes']
)
# add index for powerbi present
is_weekend = status_by_weekday_by_period['weekday'] > 5
status_by_weekday_by_period['weekday_type'] = 'weekday'
status_by_weekday_by_period.loc[is_weekend, 'weekday_type'] = 'weekend'
status_by_weekday_by_period['wp'] = (
    status_by_weekday_by_period['weekday'].astype(str)
    + status_by_weekday_by_period['period'].astype(str)
)
weekday_period_mapping = generate_time_sequence_index((1, 8), (0, 5))
status_by_weekday_by_period['wp_index'] = (
    status_by_weekday_by_period['wp'].str.slice(0, 2).map(weekday_period_mapping)
)
# save
file_path = status_path+'/aggregate_by_weekday_by_period.csv'
status_by_weekday_by_period.to_csv(file_path, index=False, encoding='UTF-8')
print(f'finished by weekday by period, cost {time.time() - stime} secs.')


# agg by weekdaytype by hour
weekday_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
    6: 'weekend', 7: 'weekend'
}
status_by_weekday_by_hour['weekday_type'] = status_by_weekday_by_hour['weekday'].map(weekday_map)
status_by_weekdaytype_by_hour = status_by_weekday_by_hour.groupby(
    ['stop_id', 'weekday_type', 'hour']
).agg(
    raw_data_count=pd.NamedAgg(column='raw_data_count', aggfunc='sum'),
    mean_disabled_minute=pd.NamedAgg(column='disabled_count_by1m', aggfunc='mean'),
    mean_available_rent=pd.NamedAgg(column='median_available_rent', aggfunc='mean'),
    mean_available_return=pd.NamedAgg(column='median_available_return', aggfunc='mean'),
    available_rent_prob=pd.NamedAgg(column='available_rent_prob', aggfunc='mean')
).reset_index()

# save
file_path = status_path+'/aggregate_by_weekdaytype_by_hour.csv'
status_by_weekdaytype_by_hour.to_csv(file_path, index=False, encoding='utf-8')
print(f'finished by weekdaytype by hour, cost {time.time() - stime} secs.')
