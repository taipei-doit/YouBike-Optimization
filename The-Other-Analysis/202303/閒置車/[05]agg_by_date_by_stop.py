# -*- coding: utf-8 -*-
"""
Created on Wed May 17 18:19:34 2023

@author: rz3881
"""

import pandas as pd
import datetime
# 所有的描述都以車衡量，而非車柱

def find_min_cum_txn_case(gdata):
    '''
    (min_cum_txn 縮寫為 minct)
    定位並回傳交易淨值最小的交易淨值(min_cum_txn)、時間(minct_time)、在站車數(minct_available_bike)
    交易淨值最小 = 一天累積借車最多 = 理論上最需要車的時間
    '''
    min_accu_net_txn_index = gdata['min_cum_txn'].idxmin()
    most_need_bike_row = gdata.loc[min_accu_net_txn_index]
    minct_time = most_need_bike_row['adjust_api_time']
    minct_available_bike = most_need_bike_row['available_rent_bikes']
    return minct_time, minct_available_bike


def find_max_cum_txn_case(gdata):
    '''
    (max_cum_txn 縮寫為 maxct)
    定位並回傳交易淨值最大的交易淨值(max_cum_txn)、時間(maxct_time)、在站車數(maxct_available_bike)
    交易淨值最大 = 一天累積還車最多 = 理論上最需要車柱的時間
    '''
    max_accu_net_txn_index = gdata['max_cum_txn'].idxmax()
    most_need_dock_row = gdata.loc[max_accu_net_txn_index]
    maxct_time = most_need_dock_row['adjust_api_time']
    maxct_available_bike = most_need_dock_row['available_rent_bikes']
    return maxct_time, maxct_available_bike


def find_empty_situation(udata):
    '''
    找到空車的狀況
    當該次API回傳可借車 <= 1即認定為無車
    若更嚴謹一點，應無車同時無借車交易，畢竟沒車就不可能被借
    但現實是交易與回傳時間有時間落差，無法太精確地反映真實狀況
    換句話說，此定義可能與現實有所出入，但時間較長的站很大機率是真的缺車比較久
    '''
    is_empty = (udata['available_rent_bikes'] <= warning_threshould)
    next_adjust_api_time = udata['adjust_api_time'].shift(-1)
    empty_seconds = (next_adjust_api_time.loc[is_empty] - udata['adjust_api_time'].loc[is_empty]).dt.seconds
    sum_empty_minutes = empty_seconds.sum() / 60
    return_empty_counts = len(empty_seconds)
    return sum_empty_minutes, return_empty_counts
    

def find_full_situation(udata):
    '找到滿車的狀況，當該次API回傳(總柱數 - 可借車) <= 1即認定為無位'
    is_full = (udata['capacity']-udata['available_rent_bikes']) <= warning_threshould
    next_adjust_api_time = udata['adjust_api_time'].shift(-1)
    full_seconds = (next_adjust_api_time.loc[is_full] - udata['adjust_api_time'].loc[is_full]).dt.seconds
    sum_full_minutes = full_seconds.sum() / 60
    return_full_counts = len(full_seconds)
    return sum_full_minutes, return_full_counts


def find_init_hour_bike(gdata):
    '''
    找到最接近{init_hour}:00 +8的在站車數，因date是-{init_hour}tz
    每天初始車數 = {init_hour}點後第一筆資料的在站車數
    '''
    init_row = gdata.iloc[0]
    closest_init_hour_time = init_row['adjust_api_time']
    init_hour_available_bike = init_row['available_rent_bikes']
    return closest_init_hour_time, init_hour_available_bike


def find_afternoon_hour_bike(gdata, date_m6h):
    '''
    找到最接近{afternoon_hour}:00 +8的在站車數
      = {afternoon_hour}點後第一筆資料的在站車數
    '''
    afternoon_time = datetime.datetime(
        date_m6h.year, date_m6h.month, date_m6h.day, afternoon_hour, 0, 0)
    afternoon_gdata = gdata.loc[gdata['adjust_api_time']>=afternoon_time]
    is_empty = afternoon_gdata.shape[0]==0
    if is_empty:
        last_available_bike = gdata['available_rent_bikes'].iloc[-1]
        return afternoon_time, last_available_bike
    else:
        afternoon_time_row = afternoon_gdata.iloc[0]
        closest_init_hour_time = afternoon_time_row['adjust_api_time']
        init_hour_available_bike = afternoon_time_row['available_rent_bikes']
        return closest_init_hour_time, init_hour_available_bike


# config
ym = '202303'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'
init_hour = 6
afternoon_hour = 16
exclude_date = ['2023-02-28']
warning_threshould = 0
    
# load
data = pd.read_csv(idle_path+'/status_return_time_merge_txn_and_dispatch.csv')
data['adjust_api_time'] = pd.to_datetime(data['adjust_api_time'])
# 為符合使用者習慣，將init_hour視為每日起點
data['adjust_api_time_m6h'] = data['adjust_api_time'] - pd.Timedelta(hours=init_hour)
data['api_m6h_hour'] = data['adjust_api_time_m6h'].dt.hour 
data['date_m6h'] = data['adjust_api_time_m6h'].dt.date 
fdata = data.loc[~data['date_m6h'].isin(exclude_date)].copy()

# 全域最低水位、調度量等可以直接agg的變數
agg = fdata.groupby(['date_m6h', 'stop_id']).agg({
        'weekday': 'first',
        'stop_name': 'first',
        'capacity': 'first',
        'service_status': 'min',
        'available_rent_bikes': ['min', 'max'],
        'txn_on': 'sum',
        'txn_off': 'sum',
        'txn_delta': 'sum',
        'min_cum_txn': 'min',
        'max_cum_txn': 'max',
        'other_delta': 'sum',
        'in': 'sum',
        'out': 'sum',
        'dispatch_delta': 'sum'
        # 'available_rent_bikes_in_worst_case': 'min'
    }).reset_index()
agg.columns = ['date', 'stop_id', 'weekday',
               'stop_name', 'capacity', 'is_work_today',
               'min_available_rent_bikes', 'max_available_rent_bikes',
               'sum_rent', 'sum_return', 'sum_txn_delta',
               'min_cum_txn', 'max_cum_txn', 'sum_other_delta',
               'sum_in', 'sum_out', 'sum_dispatch_delta']
agg['sum_dispatch_bikes'] = agg['sum_in'] + agg['sum_out']

# 尋找實際最缺車、缺位狀態、6點後 最小交易淨值時的水位
results = []
for (date_m6h, stop_id), gdata in fdata.groupby(['date_m6h', 'stop_id']):
    if str(date_m6h) in exclude_date:
        continue
    
    # break
    # 0~6點是特殊時間，不列入空爆計算
    udata = gdata.loc[gdata['adjust_api_time'].dt.hour >= init_hour]
    # 因一天的開始是6點，0~6點是明日，計入下午調度
    is_morning = (gdata['adjust_api_time'].dt.hour < afternoon_hour) & (gdata['adjust_api_time'].dt.hour >= init_hour)
    is_afternoon = ~is_morning
    # 交易淨值最小資訊
    minct_time, minct_available_bike = find_min_cum_txn_case(gdata)
    # 交易淨值最大資訊
    maxct_time, maxct_available_bike = find_max_cum_txn_case(gdata)
    # 當日缺車狀況
    sum_empty_minutes, return_empty_counts = find_empty_situation(udata)
    # 當日滿站狀況
    sum_full_minutes, return_full_counts = find_full_situation(udata)
    # 6點車數
    closest_init_hour_time, init_hour_available_bike = find_init_hour_bike(gdata)
    # 16點車數
    closest_afternoon_hour_time, afternoon_hour_available_bike = find_afternoon_hour_bike(gdata, date_m6h)
    # 上午調度 in/out
    morning_in = gdata.loc[is_morning, 'in'].sum()
    morning_out = gdata.loc[is_morning, 'out'].sum()
    # 下午調度 in/out
    afternoon_in = gdata.loc[is_afternoon, 'in'].sum()
    afternoon_out = gdata.loc[is_afternoon, 'out'].sum()
    # save
    temp = [date_m6h, stop_id]
    temp.extend([minct_time, round(minct_available_bike)])
    temp.extend([maxct_time, round(maxct_available_bike)])
    temp.extend([round(sum_empty_minutes, 1), return_empty_counts])
    temp.extend([round(sum_full_minutes, 1), return_full_counts])
    temp.extend([closest_init_hour_time, init_hour_available_bike,
                 closest_afternoon_hour_time, afternoon_hour_available_bike,
                 morning_in, morning_out,
                 afternoon_in, afternoon_out])
    results.append(temp)
# cost time < 60 seconds
col_names = ['date', 'stop_id',
             'minct_time', 'minct_available_bike',
             'maxct_time', 'maxct_available_bike',
             'empty_minutes', 'empty_counts',
             'full_minutes', 'full_counts',
             'closest_init_hour_time', 'init_hour_available_bike',
             'closest_afternoon_hour_time', 'afternoon_hour_available_bike',
             'morning_in', 'morning_out',
             'afternoon_in', 'afternoon_out']
max_demand_agg = pd.DataFrame(results, columns=col_names)

# merge
agg = agg.merge(max_demand_agg, how='outer', on=['date', 'stop_id'])

# min_cum_txn最大最大值應是0，因為每天從0開始，但實際資料回傳通常是第一筆交易起算
# 導致得到1之類的>0正值，
is_min_cum_txn_positive = (agg['min_cum_txn'] > 0)
agg.loc[is_min_cum_txn_positive, 'min_cum_txn'] = 0
# max_cum_txn同理，最小值應是0
is_max_cum_txn_positive = (agg['max_cum_txn'] < 0)
agg.loc[is_max_cum_txn_positive, 'max_cum_txn'] = 0
# min_available_rent_bikes不確定原因，會有負值，但因為最小是-1，懶得debug，直接0
is_nagative = (agg['min_available_rent_bikes'] < 0)
agg.loc[is_nagative, 'min_available_rent_bikes'] = 0
is_higher_than_capacity = (agg['max_available_rent_bikes']>agg['capacity'])
agg.loc[is_higher_than_capacity, 'max_available_rent_bikes'] = agg.loc[is_higher_than_capacity, 'capacity']

# reshape
service_status_map = {0: False, 1: True}
agg['is_work_today'] = agg['is_work_today'].map(service_status_map)
agg = agg[['stop_id', 'stop_name', 'date', 'weekday', 'capacity', 'is_work_today',
           'closest_init_hour_time', 'init_hour_available_bike',
           'closest_afternoon_hour_time', 'afternoon_hour_available_bike',
           'min_cum_txn', 'minct_time', 'minct_available_bike',
           'max_cum_txn', 'maxct_time', 'maxct_available_bike',
           'empty_minutes', 'empty_counts',
           'full_minutes', 'full_counts',
           'min_available_rent_bikes', 'max_available_rent_bikes',
           'sum_rent', 'sum_return','sum_txn_delta', 'sum_other_delta',
           'morning_in', 'morning_out',
           'afternoon_in', 'afternoon_out',
           'sum_in', 'sum_out', 'sum_dispatch_delta', 'sum_dispatch_bikes']]

# save
agg.to_excel(idle_path+'/redundancy_bike.xlsx', sheet_name='redundancy_bike')
