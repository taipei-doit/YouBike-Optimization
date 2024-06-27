import pandas as pd
import geopandas as gpd

import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import load_txn_data


def calcu_distance(data):
    on_points = gpd.points_from_xy(data['on_lng'], data['on_lat'], crs='EPSG:4326')
    on_points = on_points.to_crs(epsg='3826')
    off_points = gpd.points_from_xy(data['off_lng'], data['off_lat'], crs='EPSG:4326')
    off_points = off_points.to_crs(epsg='3826')
    distance_m = on_points.distance(off_points)
    return distance_m.round()


def calcu_diff_time(data):
    time_diff_min = (data['off_time'] - data['on_time']).dt.total_seconds() / 60
    return time_diff_min.round()


def daily_agg(data):
    agg = data.groupby(['date']).agg(
        weekday_type=pd.NamedAgg(column='weekday_type', aggfunc='first'),
        txn_count=pd.NamedAgg(column='card_id', aggfunc='count'),
        unique_card=pd.NamedAgg(column='card_id', aggfunc='nunique'),
        unique_bike=pd.NamedAgg(column='route_name', aggfunc='nunique'),
        mean_distance_m=pd.NamedAgg(column='distance_m', aggfunc='mean'),
        mean_time_diff_min=pd.NamedAgg(column='time_diff_min', aggfunc='mean')
    ).reset_index()
    return agg


# Constants
ROOT_PATH = r'D:\iima\ubike分析'
OUTPUT_PATH = r'D:\iima\ubike分析\DM\202403\7th_meeting'
# ref: https://www.cwa.gov.tw/V8/C/D/DailyPrecipitation.html
# 並與交易數量比對
rainny_days = ['2024-03-01', '2024-03-02', '2024-03-11', '2024-03-31']
festival_days = [
    '2024-02-08', '2024-02-09', '2024-02-10', '2024-02-11',
    '2024-02-12', '2024-02-13', '2024-02-14'
]
weekend_workdays = ['2024-02-17']

# load txn
txn02 = load_txn_data(f'{ROOT_PATH}/DM/202402/prepared_data/txn/txn_only_ubike.csv')
txn03 = load_txn_data(f'{ROOT_PATH}/DM/202403/prepared_data/txn/txn_only_ubike.csv')

# add distance_m, time_diff_min
# 原本的distance_m, time_diff_mins是為了計算轉乘，是與上一筆交易的距離與時間
txn02['distance_m'] = calcu_distance(txn02)
txn02['time_diff_min'] = calcu_diff_time(txn02)
txn03['distance_m'] = calcu_distance(txn03)
txn03['time_diff_min'] = calcu_diff_time(txn03)

# summarize txn with segment of weekday_type 
daily_txn_agg02 = daily_agg(txn02)
daily_txn_agg03 = daily_agg(txn03)
daily_txn_agg = pd.concat([daily_txn_agg02, daily_txn_agg03]).reset_index()

# adjust columns
daily_txn_agg['date'] = daily_txn_agg['date'].astype(str)
daily_txn_agg['month'] = daily_txn_agg['date'].str[5:7].astype(int)

# adjust 補班日
is_weekend_work = daily_txn_agg['date'].isin(weekend_workdays)
daily_txn_agg.loc[is_weekend_work, 'weekday_type'] = 'weekday'

# mark special days
daily_txn_agg['tag'] = daily_txn_agg['weekday_type']
# rainny days
is_rainny = daily_txn_agg['date'].isin(rainny_days)
daily_txn_agg.loc[is_rainny, 'tag'] = 'rainny'
# festival days
is_festival = daily_txn_agg['date'].isin(festival_days)
daily_txn_agg.loc[is_festival, 'tag'] = 'festival'

# save daily agg
daily_txn_agg.to_csv(f'{OUTPUT_PATH}/daily_txn_agg.csv')

# 兩月份比較
is_normal_day = (daily_txn_agg['tag'] == 'weekday') | (daily_txn_agg['tag'] == 'weekend')
normal_daily_txn_agg = daily_txn_agg.loc[is_normal_day]
monthly_agg = normal_daily_txn_agg.groupby(['month', 'tag']).agg(
    mean_txn_count=pd.NamedAgg(column='txn_count', aggfunc='mean'),
    mean_unique_card=pd.NamedAgg(column='unique_card', aggfunc='mean'),
    mean_unique_bike=pd.NamedAgg(column='unique_bike', aggfunc='mean'),
    mean_distance_m=pd.NamedAgg(column='mean_distance_m', aggfunc='mean'),
    mean_time_diff_mins=pd.NamedAgg(column='mean_time_diff_min', aggfunc='mean')
).reset_index()

# 畫不同距離與時間的分布圖
# 距離
distance_bins = list(range(0, 10000, 250))
txn02['bin_distance'] = pd.cut(txn02['distance_m'].tolist(), bins=distance_bins)
txn03['bin_distance'] = pd.cut(txn03['distance_m'].tolist(), bins=distance_bins)
distance_distribution = pd.concat([
    txn02['bin_distance'].value_counts().sort_index(),
    txn03['bin_distance'].value_counts().sort_index()
], axis=1)
distance_distribution.to_csv(f'{OUTPUT_PATH}/distance_distribution.csv')
# 時間
time_bins = list(range(0, 120, 10))
txn02['bin_time'] = pd.cut(txn02['time_diff_min'].tolist(), bins=time_bins)
txn03['bin_time'] = pd.cut(txn03['time_diff_min'].tolist(), bins=time_bins)
time_distribution = pd.concat([
    txn02['bin_time'].value_counts().sort_index(),
    txn03['bin_time'].value_counts().sort_index()
], axis=1)
time_distribution.to_csv(f'{OUTPUT_PATH}/time_distribution.csv')