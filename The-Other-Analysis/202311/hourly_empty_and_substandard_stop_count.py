import pandas as  pd
import time


def find_empty_situation(udata):
    '''
    (取自D:/iima/ubike分析/CODE/202309/閒置車/[05]agg_by_date_by_stop.py。)
    (為了保證跑出來的結果一致，單獨複製function，避免被修改。)
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
    '''
    (取自D:/iima/ubike分析/CODE/202309/閒置車/[05]agg_by_date_by_stop.py。)
    (為了保證跑出來的結果一致，單獨複製function，避免被修改。)
    找到滿車的狀況，當該次API回傳(總柱數 - 可借車) <= 1即認定為無位
    '''
    is_full = (udata['capacity']-udata['available_rent_bikes']) <= warning_threshould
    next_adjust_api_time = udata['adjust_api_time'].shift(-1)
    full_seconds = (next_adjust_api_time.loc[is_full] - udata['adjust_api_time'].loc[is_full]).dt.seconds
    sum_full_minutes = full_seconds.sum() / 60
    return_full_counts = len(full_seconds)
    return sum_full_minutes, return_full_counts


# Config
root_path = 'D:/iima/ubike分析'
output_path = root_path+'/DM/202311/市長會'
ym = '202307'
exclude_date = ['2023-06-30']
warning_threshould = 0  # 無車或無位的閾值
min_delay_secs = 4  # 交易發生後，站點最快也要4秒才會回傳紀錄
init_hour = 6   # 起始小時

# Load
status = pd.read_csv(f'{root_path}/DM/202307/prepared_data/status/unique_raw.csv')
# Add new columns
status['data_time'] = pd.to_datetime(status['data_time'])
status['adjust_api_time'] = status['data_time'] - pd.Timedelta(seconds=min_delay_secs)
status['hour'] = status['adjust_api_time'].dt.hour 
status['date'] = status['adjust_api_time'].dt.date
status['weekday'] = status['adjust_api_time'].dt.weekday + 1
status['weekday_type'] = status['weekday'].apply(lambda x: 'weekday' if x <= 5 else 'weekend')
# Filter
date_m6h = (status['adjust_api_time'] - pd.Timedelta(hours=init_hour)).dt.date.astype(str)
fdata = status.loc[~date_m6h.isin(exclude_date)].copy()

# Extract empty and full situation
strat_time = time.time()
results = []
total_loop = len(fdata.groupby(['stop_id', 'weekday_type', 'date', 'hour']))
a = 0
for (stop_id, weekday_type, date, hour), gdata in fdata.groupby(['stop_id', 'weekday_type', 'date', 'hour']):
    # break
    
    # process
    sum_empty_minutes, return_empty_counts = find_empty_situation(gdata)
    sum_full_minutes, return_full_counts = find_full_situation(gdata)

    # append
    temp = [stop_id, weekday_type, date, hour]
    temp.extend([round(sum_empty_minutes), return_empty_counts])
    temp.extend([round(sum_full_minutes), return_full_counts])
    results.append(temp)
    
    if (a % 10000) == 0:
        print(f'{a}/{total_loop} cost time {round(time.time() - strat_time)} seconds')
    a += 1
col_names = [
    'stop_id', 'weekday_type', 'date', 'hour',
    'empty_minutes', 'empty_counts',
    'full_minutes', 'full_counts'
]
agg_detail = pd.DataFrame(results, columns=col_names)

# Add new columns
agg_detail['見車率'] = 1 - (agg_detail['empty_minutes']/60)
agg_detail['見位率'] = 1 - (agg_detail['full_minutes']/60)
agg_detail['見車率未達標'] = agg_detail['見車率'] < 0.9
agg_detail['見位率未達標'] = agg_detail['見位率'] < 0.9
# Save raw extract data
# agg_detail.to_csv(f'{output_path}/hourly_agg.csv', index=False)

# Agg by weekday by date by hour
daily_agg = agg_detail.groupby(['date', 'weekday_type', 'hour']).agg(
    stop_count = pd.NamedAgg(column='stop_id', aggfunc='nunique'),
    daily_mean_ab_prop = pd.NamedAgg(column='見車率', aggfunc='mean'),  # ab: available bike rate
    daily_mean_ad_prop = pd.NamedAgg(column='見位率', aggfunc='mean'),  # ad: available dock rate
    ab_low_stop = pd.NamedAgg(column='見車率未達標', aggfunc='sum'),
    ad_low_stop = pd.NamedAgg(column='見位率未達標', aggfunc='sum')
).reset_index()
# Save daily agg data
# daily_agg.to_csv(f'{output_path}/daily_agg.csv', index=False)

# Agg by weekday by hour
weekday_type_agg = daily_agg.groupby(['weekday_type', 'hour']).agg(
    day_count = pd.NamedAgg(column='date', aggfunc='count'),
    stop_count = pd.NamedAgg(column='stop_count', aggfunc='mean'),
    daily_mean_ab_prop = pd.NamedAgg(column='daily_mean_ab_prop', aggfunc='mean'),
    daily_mean_ad_prop = pd.NamedAgg(column='daily_mean_ad_prop', aggfunc='mean'),
    ab_low_stop = pd.NamedAgg(column='ab_low_stop', aggfunc='mean'),
    ad_low_stop = pd.NamedAgg(column='ad_low_stop', aggfunc='mean')
).reset_index()
weekday_type_agg_pivot = weekday_type_agg.pivot_table(
    index='weekday_type', columns='hour', values='ab_low_stop'
).reset_index()
# Save weekday agg data
# weekday_type_agg_pivot.to_csv(f'{output_path}/by_weekday_pivot.csv', index=False)

# Pivot table
weekday_type_agg['low_stop'] = weekday_type_agg['ab_low_stop'] + weekday_type_agg['ad_low_stop']
ab_pivot = weekday_type_agg.pivot_table(
    index='weekday_type', columns='hour', values='ab_low_stop'
).reset_index()
ab_pivot['type'] = '日均見車率未達標站數'
ad_pivot = weekday_type_agg.pivot_table(
    index='weekday_type', columns='hour', values='ad_low_stop'
).reset_index()
ad_pivot['type'] = '日均見位率未達標站數'
low_pivot = weekday_type_agg.pivot_table(
    index='weekday_type', columns='hour', values='low_stop'
).reset_index()
low_pivot['type'] = '日均見車率或見位率未達標站數'
# Concat
final_pivot = pd.concat([ab_pivot, ad_pivot, low_pivot])
# Save final result
final_pivot = [[
    'weekday_type', 'type', 
    0, 1, 2, 3, 4, 5,
    6, 7, 8, 9, 10, 11,
    12, 13, 14, 15, 16, 17,
    18, 19, 20, 21, 22, 23
]]
# final_pivot.to_excel(f'{output_path}/週間週末分時_日均未達90%站點數.xlsx', index=False)