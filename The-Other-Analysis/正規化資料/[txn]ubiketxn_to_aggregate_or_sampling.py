import sys
import pandas as pd
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import generate_hour_to_period_dict, generate_time_sequence_index

root_path = r'D:\iima\ubike分析'
ym = '202403'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'

# load
txn = pd.read_csv(f'{txn_path}/txn_only_ubike.csv')

# extract time info
txn['on_time'] = pd.to_datetime(txn['on_time'])
txn['off_time'] = pd.to_datetime(txn['off_time'])
txn['on_hour'] = txn['on_time'].dt.hour
txn['off_hour'] = txn['off_time'].dt.hour
txn['weekday'] = txn['on_time'].dt.weekday + 1
txn['date'] = txn['on_time'].dt.date


# agg by date by hour 
txn_rent = txn.groupby(['date', 'on_stop_id', 'on_hour']).agg({
    'weekday': 'first',
    'on_stop': 'first',
    'card_id': 'count'
    }).reset_index()
txn_rent.columns = ['date', 'stop_id', 'hour', 'weekday', 'stop', 'rent']
txn_return = txn.groupby(['date', 'off_stop_id', 'off_hour']).agg({
    'weekday': 'first',
    'off_stop': 'first',
    'card_id': 'count'
    }).reset_index()
txn_return.columns = ['date', 'stop_id', 'hour', 'weekday', 'stop', 'return']
txn_bydate = txn_rent.merge(
    txn_return, on=['date', 'stop_id', 'hour', 'weekday', 'stop'], how='outer'
)
txn_bydate = txn_bydate.fillna(0)
# save
file_path = txn_path+'/aggregate_by_date_by_hour.csv'
txn_bydate.to_csv(file_path, index=False, encoding='utf8')


# sampleing by stop by weekday by hour
txn_by_weekday_by_hour = txn_bydate.groupby(['stop_id', 'stop', 'weekday', 'hour']).agg({
    'rent': ['sum', 'median'],
    'return': ['sum', 'median']
    }).reset_index()
txn_by_weekday_by_hour.columns = [
    'stop_id', 'stop', 'weekday', 'hour',
    'raw_rent_count', 'rent',
    'raw_return_count', 'return'
]
txn_by_weekday_by_hour['net_profit'] = (
    txn_by_weekday_by_hour['return'] - txn_by_weekday_by_hour['rent']
)
# for plot, convert by weekday and hour to a seq
txn_by_weekday_by_hour['wh'] = (
    'W' + txn_by_weekday_by_hour['weekday'].astype(str) 
    + 'H' + txn_by_weekday_by_hour['hour'].astype(str).str.zfill(2)
)
weekday_hour_mapping = generate_time_sequence_index((1, 8), (0, 24))
txn_by_weekday_by_hour['wh_index'] = txn_by_weekday_by_hour['wh'].map(weekday_hour_mapping)
# save
file_path = txn_path+'/sampling_by_weekday_by_hour.csv'
txn_by_weekday_by_hour.to_csv(file_path, index=False, encoding='utf8')


# agg by weekday by period
hour_to_period = generate_hour_to_period_dict()
txn_by_weekday_by_hour['period'] = txn_by_weekday_by_hour['hour'].map(hour_to_period)
txn_by_weekday_by_period = txn_by_weekday_by_hour.groupby(
    ['stop_id', 'stop', 'weekday', 'period']
).agg({
    'raw_rent_count': 'sum',
    'raw_return_count': 'sum',
    'rent': 'sum',
    'return': 'sum',
    'net_profit': 'sum'
}).reset_index()
# add index for powerbi present
is_weekend = txn_by_weekday_by_period['weekday'] > 5
txn_by_weekday_by_period['weekday_type'] = 'weekday'
txn_by_weekday_by_period.loc[is_weekend, 'weekday_type'] = 'weekend'
txn_by_weekday_by_period['wp'] = (
    txn_by_weekday_by_period['weekday'].astype(str)
    + txn_by_weekday_by_period['period'].astype(str)
)
weekday_period_mapping = generate_time_sequence_index((1, 8), (0, 5))
txn_by_weekday_by_period['wp_index'] = (
    txn_by_weekday_by_period['wp'].str.slice(0, 2).map(weekday_period_mapping)
)
# save
file_path = txn_path+'/aggregate_by_weekday_by_period.csv'
txn_by_weekday_by_period.to_csv(file_path, index=False, encoding='utf8')


# agg by weekday_type by hour
weekday_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
    6: 'weekend', 7: 'weekend'
}
txn_by_weekday_by_hour['weekday_type'] = txn_by_weekday_by_hour['weekday'].map(weekday_map)
txn_by_weekdaytype_by_hour = txn_by_weekday_by_hour.groupby(
    ['stop_id', 'weekday_type', 'hour']
).agg({
    'raw_rent_count': 'sum',
    'raw_return_count': 'sum',
    'rent': 'mean',
    'return': 'mean',
    'net_profit': 'mean'
}).reset_index()
# save
file_path = txn_path+'/aggregate_by_weekdaytype_by_hour.csv'
txn_by_weekdaytype_by_hour.to_csv(file_path, index=False, encoding='utf8')
