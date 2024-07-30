# -*- coding: utf-8 -*-
"""
Created on Wed May 17 18:19:34 2023

@author: rz3881
"""

import pandas as pd

def find_most_need_bike_case(gdata):
    # 定位當日最缺車時間
    min_accu_net_txn_index = gdata['min_cum_txn'].idxmin()
    most_need_bike_row = gdata.loc[min_accu_net_txn_index]
    # 最缺車時的情況
    mnb_time = most_need_bike_row['adjust_api_time']
    mnb_available_rent = most_need_bike_row['available_rent_bikes_in_worst_case']
    mnb_number = most_need_bike_row['min_cum_txn']
    return mnb_time, mnb_available_rent, mnb_number

def find_most_need_dock_case(gdata):
    # 定位當日最缺位時間
    max_accu_net_txn_index = gdata['max_cum_txn'].idxmax()
    most_need_dock_row = gdata.loc[max_accu_net_txn_index]
    # 最缺位時的情況
    mnd_time = most_need_dock_row['adjust_api_time']
    mnd_available_rent = most_need_dock_row['available_rent_bikes_in_worst_case']
    mnd_number = most_need_dock_row['max_cum_txn']
    return mnd_time, mnd_available_rent, mnd_number

def find_empty_situation(gdata):
    # 當該次API回傳可借車 <= 1即認定為無車
    # 若更嚴謹一點，應無車同時無借車交易，畢竟沒車就不可能被借
    # 但現實是交易與回傳時間有時間落差，無法太精確地u反映真實狀況
    # 換句話說，此定義可能與現實有所出入，但時間較長的站很大機率是真的缺車比較久
    is_empty = (gdata['available_rent_bikes']<=1)
    next_adjust_api_time = gdata['adjust_api_time'].shift(-1)
    empty_seconds = (next_adjust_api_time.loc[is_empty] - gdata['adjust_api_time'].loc[is_empty]).dt.seconds
    sum_empty_minutes = empty_seconds.sum() / 60
    return_empty_counts = len(empty_seconds)
    return sum_empty_minutes, return_empty_counts
    
def find_full_situation(gdata):
    # 當該次API回傳(總柱數 - 可借車) <= 1即認定為無位
    is_full = (gdata['capacity'] - gdata['available_rent_bikes'])<=1
    next_adjust_api_time = gdata['adjust_api_time'].shift(-1)
    full_seconds = (next_adjust_api_time.loc[is_full] - gdata['adjust_api_time'].loc[is_full]).dt.seconds
    sum_full_minutes = full_seconds.sum() / 60
    return_full_counts = len(full_seconds)
    return sum_full_minutes, return_full_counts

def find_six_am_bike(gdata):
    # 找到最接近06:00的在站車數
    # 因已篩選時間是>=6點，第一筆資料一定是最接近的
    six_am_row = gdata.iloc[0]
    closest_am6_time = six_am_row['adjust_api_time']
    am6_available_rent = six_am_row['available_rent_bikes']
    return closest_am6_time, am6_available_rent

# config
root_path = r'D:\iima\ubike分析'
idle_path = root_path+r'\DM\202303\閒置車'

# load
data = pd.read_csv(idle_path+'/status_return_time_merge_txn_and_dispatch.csv')
data['adjust_api_time'] = pd.to_datetime(data['adjust_api_time'])
data['api_hour'] = data['adjust_api_time'].dt.hour

# filter 凌晨調度時間(06:00~23:59)
is_between_0600to2359 = data['api_hour'] >= 6
fdata = data.loc[is_between_0600to2359]

# 辨識最缺車、缺位狀態
# 6點後 最小交易淨值時的水位
results = []
for (date, stop_id), gdata in fdata.groupby(['date', 'stop_id']):
    # break
    temp = [date, stop_id]
    # 當日最需車資訊
    mnb_time, mnb_available_rent, mnb_number = find_most_need_bike_case(gdata)
    temp.extend([mnb_time, round(mnb_available_rent), mnb_number])
    # 當日最需位資訊
    mnd_time, mnd_available_rent, mnd_number = find_most_need_dock_case(gdata)
    temp.extend([mnd_time, round(mnd_available_rent), mnd_number])
    # 當日無車狀況
    sum_empty_minutes, return_empty_counts = find_empty_situation(gdata)
    temp.extend([round(sum_empty_minutes, 1), return_empty_counts])
    # 當日無位狀況
    sum_full_minutes, return_full_counts = find_full_situation(gdata)
    temp.extend([round(sum_full_minutes, 1), return_full_counts])
    # 6點車數
    closest_am6_time, am6_available_rent = find_six_am_bike(gdata)
    temp.extend([closest_am6_time, am6_available_rent])
    results.append(temp)
# cost time < 60 seconds
col_names = ['date', 'stop_id',
             'mnb_time', 'mnb_bikes', 'mnb_number',
             'mnd_time', 'mnd_bikes', 'mnd_number',
             'empty_minutes', 'empty_counts',
             'full_minutes', 'full_counts',
             'closest_am6_time', 'am6_available_rent']
max_demand_agg = pd.DataFrame(results, columns=col_names)


# 6點後 全域最低水位、調度量等
agg = fdata.groupby(['date', 'stop_id']).agg({
        'weekday': 'first',
        'stop_name': 'first',
        'capacity': 'first',
        'service_status': 'min',
        'available_rent_bikes': 'max',
        'txn_on': 'sum',
        'txn_off': 'sum',
        'txn_delta': 'sum',
        'min_cum_txn': 'min',
        'max_cum_txn': 'max',
        'other_delta': 'sum',
        'dispatch_delta': 'sum',
        'available_rent_bikes_in_worst_case': 'min'
    }).reset_index()
agg.columns = ['date', 'stop_id', 'weekday', 'stop_name', 'capacity',
               'service_status', 'max_available_rent_bikes',
               'sum_rent', 'sum_return', 'sum_txn_delta',
               'min_cum_txn', 'max_cum_txn',
               'sum_other_delta', 'sum_dispatch_delta',
               'min_available_rent_bikes']
# merge
agg = agg.merge(max_demand_agg, how='outer', on=['date', 'stop_id'])

# correct invalid value
is_nagative = (agg['min_available_rent_bikes']<0)
agg.loc[is_nagative, 'min_available_rent_bikes'] = 0
is_higher_than_capacity = (agg['max_available_rent_bikes']>agg['capacity'])
agg.loc[is_higher_than_capacity, 'max_available_rent_bikes'] = agg.loc[is_higher_than_capacity, 'capacity']

# reshape
service_status_map = {0: 'malfunctioned ', 1: 'good'}
agg['service_status'] = agg['service_status'].map(service_status_map)
agg = agg[['stop_id', 'stop_name', 'date', 'weekday', 'capacity', 'service_status',
           'closest_am6_time', 'am6_available_rent',
           'mnb_time', 'mnb_bikes', 'mnb_number',
           'mnd_time', 'mnd_bikes', 'mnd_number',
           'empty_minutes', 'empty_counts',
           'full_minutes', 'full_counts',
           'min_available_rent_bikes', 'max_available_rent_bikes',
           'sum_rent', 'sum_return','sum_txn_delta',
           'min_cum_txn', 'max_cum_txn',
           'sum_other_delta', 'sum_dispatch_delta']]

# save
agg.to_excel(idle_path+'/redundancy_bike.xlsx', sheet_name='redundancy_bike')
