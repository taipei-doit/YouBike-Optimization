'''
找出擋柱綁最嚴重的點
-根據API回傳資料， 車柱 - (可借+可還)
-計算原始檔板數
-agg最大、持續時間
'''

import pandas as pd
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import pivot_table_by_hour, make_sure_folder_exist

def sampling_by_5min(status_data, is_merge_all_time=False):
    '''
    原則上每5分鐘抽樣一筆資料，為了確保每個站每個時間點都有資料，應
        1.先產生所有時間*所有站的資料
        2.merge
        3.NA填前一筆
    但現實中有些站不會整個月都有營運，所以不能這樣做
    單純以產生所有時間再merge，202311只差了1300比左右，很少
    所以先不這麼做
    
    Parameters
    ----------
    status_data : raw status data
    is_merge_all_time : Boolean, optional, default False
        if True, generate full 5 minute range and merge with data
    '''
    
    # sampling by 5 min
    is_5min = (status_data['data_time'].dt.minute % 5) == 0
    sampling_by_5min = status_data[is_5min]
    
    if is_merge_all_time:
        # gererate full time range
        start_time = status_data['data_time'].min()
        end_time = status_data['data_time'].max()
        full_time_range = pd.date_range(start_time, end_time, freq='5min')
        full_time_range = full_time_range.to_series(name='time_index').reset_index(drop=True)
        # merge
        sampling_by_5min = pd.merge(
            full_time_range, status_data, how='left',
            left_on='time_index', right_on='data_time'
        )
        # fill na with previous value
        sampling_by_5min = sampling_by_5min.fillna(method='ffill')
    return sampling_by_5min


def show_unavailable_distribution(status_data):
    '''
    show bike_on_dock distribution
    '''
    print(status_data['unavailable'].max())
    print(status_data['unavailable'].mean())
    # show bike_on_dock where not zero
    selected_column = [
        'stop_name', 'data_time', 'capacity',
        'unavailable', 'total_available',
        'available_rent_bikes', 'available_return_bikes'
    ]
    return status_data.loc[status_data['unavailable']!=0, selected_column]


# Config
ym = '202311'
root_path = r'D:\iima\ubike分析'
status_path = root_path+f'/DM/{ym}/prepared_data/status'
output_path = root_path+f'/DM/{ym}/擋柱綁'

# Load
raw = pd.read_csv(status_path+'/unique_raw.csv')

# Sampling by 5 min
raw['data_time'] = pd.to_datetime(raw['data_time'])
data = sampling_by_5min(raw, is_merge_all_time=False)

# Add time info
data['date'] = data['data_time'].dt.date
data['hour'] = data['data_time'].dt.hour
data['weekday'] = data['data_time'].dt.weekday + 1
data['weekday_type'] = data['weekday'].apply(lambda x: 'weekday' if x <= 5 else 'weekend')

# Calculate bike tie on dock
# However, the results could be due to the malfunctioning bike, so I call it as unavailable.
data['total_available'] = data['available_rent_bikes'] + data['available_return_bikes']
data['unavailable'] = data['capacity'] - data['total_available']
show_unavailable_distribution(data)

# Aggreate by hour
# by_date_by_hour_by_stop
agg_by_date_by_hour_by_stop = data.groupby(
    ['stop_id', 'date', 'hour']
).agg({
    'weekday': 'first',
    'weekday_type': 'first',
    'stop_name': 'first',
    'capacity': 'max',
    'unavailable': 'mean',
    'total_available': 'min',
    'available_rent_bikes': 'mean',
    'available_return_bikes': 'mean'
}).reset_index()
# by_weekday_type_by_hour_by_stop
agg_by_weekday_type_by_hour_by_stop = agg_by_date_by_hour_by_stop.groupby(
    ['stop_id', 'weekday_type', 'hour']
).agg({
    'stop_name': 'first',
    'capacity': 'max',
    'unavailable': 'mean',
    'total_available': 'min',
    'available_rent_bikes': 'mean',
    'available_return_bikes': 'mean'
}).reset_index()
agg_by_weekday_type_by_hour_by_stop['unavailable'] = agg_by_weekday_type_by_hour_by_stop['unavailable'].round(1)

# Pivot table by hour
unavailable_pivot_by_hour = pivot_table_by_hour(
    long_data=agg_by_weekday_type_by_hour_by_stop,
    piovt_value='unavailable',
    index_col=['stop_id', 'stop_name', 'weekday_type', 'capacity'],
    is_round=True,
    is_add_sum_col=True
)

# Save agg data
make_sure_folder_exist(output_path)
agg_by_date_by_hour_by_stop.to_csv(
    output_path+'/unavailable_agg_by_date_by_hour_by_stop.csv', index=False
)
agg_by_weekday_type_by_hour_by_stop.to_csv(
    output_path+'/unavailable_agg_by_weekday_type_by_hour_by_stop.csv', index=False
)
unavailable_pivot_by_hour.to_csv(output_path+'/unavailable_pivot_by_hour.csv', index=False)
