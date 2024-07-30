# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 14:28:33 2023

@author: rz3881
"""

import pandas as pd
import datetime
from numpy import nan

def find_empty_situation(gdata):
    '''
    找到空車的狀況
    當該次API回傳可借車 <= 1即認定為無車
    若更嚴謹一點，應無車同時無借車交易，畢竟沒車就不可能被借
    但現實是交易與回傳時間有時間落差，無法太精確地反映真實狀況
    換句話說，此定義可能與現實有所出入，但時間較長的站很大機率是真的缺車比較久
    '''
    is_empty = (gdata['simulat_best_result'] <= 1)
    next_adjust_api_time = gdata['adjust_api_time'].shift(-1)
    empty_seconds = (next_adjust_api_time.loc[is_empty] - gdata['adjust_api_time'].loc[is_empty]).dt.seconds
    sum_empty_minutes = empty_seconds.sum() / 60
    return_empty_counts = len(empty_seconds)
    return sum_empty_minutes, return_empty_counts
    

def find_full_situation(gdata):
    '找到滿車的狀況，當該次API回傳(總柱數 - 可借車) <= 1即認定為無位'
    is_full = (gdata['capacity']-gdata['simulat_best_result']) <= 1
    next_adjust_api_time = gdata['adjust_api_time'].shift(-1)
    full_seconds = (next_adjust_api_time.loc[is_full] - gdata['adjust_api_time'].loc[is_full]).dt.seconds
    sum_full_minutes = full_seconds.sum() / 60
    return_full_counts = len(full_seconds)
    return sum_full_minutes, return_full_counts


def generate_end_time(target_hour):
    target_time = datetime.datetime.strptime(str(target_hour), "%H")
    end_time = target_time - datetime.timedelta(seconds=1)
    end_time = end_time.strftime("%H:%M:%S")
    return end_time


# Config
ym = '202304'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'
date_last_month = '2023-03-31'
init_hour = 6
afternoon_hour = 16
dispatch_reserve_number = 2
daily_end_time = generate_end_time(init_hour)
morning_end_time = generate_end_time(afternoon_hour)

# Load
static = pd.read_csv(idle_path+'/simulation_results_static_part.csv')
static['date_m6h'] = static['date'].astype(str)
detail = pd.read_csv(idle_path+'/simulation_results_detail_part.csv')
detail['adjust_api_time'] = pd.to_datetime(detail['adjust_api_time'])
agg = pd.read_excel(idle_path+'/redundancy_bike.xlsx', sheet_name='redundancy_bike')
agg['date_m6h'] = agg['date'].astype(str)

# Filter
agg = agg.loc[agg['is_work_today']]
is_this_month = (agg['date']!=date_last_month)
agg = agg.loc[is_this_month]

# Extract
# 實際
data_real = agg[['stop_id', 'stop_name', 'date_m6h', 'capacity', 
     'closest_init_hour_time', 'init_hour_available_bike',
     'closest_afternoon_hour_time', 'afternoon_hour_available_bike',
     'min_cum_txn', 'minct_time', 'minct_available_bike',
     'max_cum_txn', 'maxct_time', 'maxct_available_bike',
     'empty_minutes', 'empty_counts', 'full_minutes', 'full_counts',
     'min_available_rent_bikes', 'max_available_rent_bikes',
     'sum_rent', 'sum_return', 'sum_txn_delta',
     'morning_in', 'morning_out', 'afternoon_in', 'afternoon_out',
     'sum_in', 'sum_out', 'sum_dispatch_delta', 
     'sum_other_delta']]
data_real['txn_range'] = data_real['max_cum_txn'] - data_real['min_cum_txn']
data_real['sum_abs_dispatch'] = (data_real['sum_in'] + data_real['sum_out'])
data_real['morning_net_dis'] = (data_real['morning_in'] - data_real['morning_out'])
data_real['afternoon_net_dis'] = (data_real['afternoon_in'] - data_real['afternoon_out'])
# 模擬
# static
simu_static = static[['stop_id', 'date_m6h',
                      'best_init_bikes', 'best_afternoon_bikes',
                      'morning_final_bikes', 'afternoon_final_bikes',
                      'tomorrow_best_init_bikes',
                      'morning_in', 'morning_out',
                      'afternoon_in', 'afternoon_out',
                      'sum_dispatch_bikes']]
simu_static['simu_morning_net_dis'] = (simu_static['morning_in'] - simu_static['morning_out'])
simu_static['simu_afternoon_net_dis'] = (simu_static['afternoon_in'] - simu_static['afternoon_out'])
simu_static = simu_static.rename(
    columns={'morning_in': 'simu_morning_in',
             'afternoon_in': 'simu_afternoon_in',
             'morning_out': 'simu_morning_out',
             'afternoon_out': 'simu_afternoon_out'})
# detail
detail['abs_dis_suggest'] = detail['dis_suggest'].abs()
simu_detail = detail.groupby(['date_m6h', 'stop_id']).agg({
    'weekday_m6h': 'first',
    'abs_dis_suggest': 'sum',
    'dis_suggest': 'sum',
    'available_rent_bikes': 'min',  # 6點後最小在站車
    }).reset_index()
simu_detail = simu_detail.rename(columns={'available_rent_bikes': 'min_bike_after6'})
simu_detail_p2 = []
for (date, stop_id), gdata in detail.groupby(['date_m6h', 'stop_id']):
    # break
    temp = [date, stop_id]
    # 當日缺車狀況
    sum_empty_minutes, return_empty_counts = find_empty_situation(gdata)
    temp.extend([round(sum_empty_minutes, 1), return_empty_counts])
    # 當日滿站狀況
    sum_full_minutes, return_full_counts = find_full_situation(gdata)
    temp.extend([round(sum_full_minutes, 1), return_full_counts])
    # 當日上午結束在站車
    is_morning = (gdata['adjust_api_time'].dt.hour < afternoon_hour)
    mor_gdata = gdata.loc[is_morning]
    if mor_gdata.shape[0] == 0:
        is_target = (detail['stop_id'] == stop_id) & (detail['date_m6h'] == date)
        last_day_end_cum_txn = detail.loc[is_target, 'min_cum_txn'].iloc[-1]
        temp.append(last_day_end_cum_txn)
    else:
        morning_end_cum_txn = mor_gdata['min_cum_txn'].iloc[-1]
        temp.append(morning_end_cum_txn)
    # 當日下午結束在站車
    if gdata.shape[0] == 0:
        temp.append(nan)
    else:
        end_cum_txn = gdata['min_cum_txn'].iloc[-1]
        temp.append(end_cum_txn)
    # 為下午的調度
    is_dispatch_for_afternoon = (gdata['time'] == morning_end_time)
    dis_for_afternoon_count = sum(is_dispatch_for_afternoon)
    if dis_for_afternoon_count > 1:
        raise ValueError(f'{morning_end_time}的資料{dis_for_afternoon_count}筆, 不合理')
    if dis_for_afternoon_count == 0:
        temp.append(0)
    else:
        dispatch_for_afternoon = gdata.loc[is_dispatch_for_afternoon, 'dis_suggest'].iloc[0]
        temp.append(round(dispatch_for_afternoon))
    # 為隔日的調度
    is_dispatch_for_tomorrow = (gdata['time'] == daily_end_time)
    dis_for_tomorrow_count = sum(is_dispatch_for_tomorrow)
    if dis_for_tomorrow_count > 1:
        raise ValueError(f'{daily_end_time}的資料{dis_for_tomorrow_count}筆, 不合理')
    if dis_for_tomorrow_count == 0:
        temp.append(0)
    else:
        dispatch_for_tomorrow = gdata.loc[is_dispatch_for_tomorrow, 'dis_suggest'].iloc[0]
        temp.append(round(dispatch_for_tomorrow))
    # save
    simu_detail_p2.append(temp)
simu_detail_p2_col = ['date_m6h', 'stop_id',
                      'simu_empty_minutes', 'simu_empty_counts',
                      'simu_full_minutes', 'simu_full_counts',
                      'morning_end_cum_txn', 'end_cum_txn',
                      'dispatch_for_tomorrow', 'dispatch_for_afternoon']
simu_detail_p2 = pd.DataFrame(simu_detail_p2, columns=simu_detail_p2_col)

# merge
simu_detail = simu_detail.merge(simu_detail_p2, how='outer', on=['date_m6h', 'stop_id'])
simu_static = simu_static.merge(simu_detail, how='outer', on=['date_m6h', 'stop_id'])
compare_detail = data_real.merge(simu_static, how='left', on=['date_m6h', 'stop_id'])

# 計算docker-free所需的初始車數、夜間調度
# 要滿足累積交易淨值曲線，不用調度，=0-累積交易淨值最小值
compare_detail['best_init_if_docker_free'] = -compare_detail['min_cum_txn']
compare_detail['end_bike_if_docker_free'] = compare_detail['best_init_if_docker_free'] + compare_detail['end_cum_txn']
# 應補上最低車數
compare_detail['best_init_if_docker_free'] = compare_detail['best_init_if_docker_free'] + dispatch_reserve_number
compare_detail['end_bike_if_docker_free'] = compare_detail['end_bike_if_docker_free'] + dispatch_reserve_number

# 計算調度閒置車
# 閒置車 = (初始車數差 + 調度車數差) = ((實際初始-模擬初始) + (實際調度-模擬調度))
compare_detail['capacity_diff'] = compare_detail['capacity'] - compare_detail['txn_range']
compare_detail['init_diff'] = compare_detail['init_hour_available_bike'] - compare_detail['best_init_bikes']
compare_detail['dispatch_diff'] = compare_detail['sum_abs_dispatch'] - compare_detail['abs_dis_suggest']
compare_detail['idle'] = compare_detail['init_diff'] + compare_detail['dispatch_diff']

# save
file_path = idle_path + '/compare_detail.csv'
compare_detail.to_csv(file_path, index=False, encoding='utf-8')

