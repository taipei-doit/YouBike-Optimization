import pandas as pd
import datetime
import time


def generate_time_sequence(start_year, start_month, end_year, end_month, time_step_minutes=1):
    start_time = datetime.datetime(start_year, start_month, 1, 0, 0)
    end_time = datetime.datetime(end_year, end_month+1, 1, 0, 0)
    time_step = datetime.timedelta(minutes=time_step_minutes)

    time_sequence = []
    current_time = start_time
    while current_time < end_time:
        time_sequence.append(current_time)
        current_time += time_step
    return pd.to_datetime(time_sequence)


def fill_empty_data(wild_data, start_year, start_month, end_year, end_month, step_minutes=1):
    '''
    為了讓每個站每個時間區間都有資料，生成完整時間序列作為範本。
    缺值補上上個valid value, 沒有上個合法值的只有第一天的凌晨，
    但不重要就不找真實的值了，直接回填下個合法值
    '''
    try:
        ts
    except NameError:
        print(
            f'''
            Generate time sequence from {start_year}, {start_month} to {end_year},
            {end_month} by step {step_minutes} minutes
            '''
        )
        ts = pd.DataFrame(
            generate_time_sequence(start_year, start_month, end_year, end_month, step_minutes)
        )
        ts.columns = ['data_time']

    results = []
    for stop_id, group_data in wild_data.groupby('stop_id'):
        ts_complete_data = ts.merge(group_data, how='left', on='data_time')
        # 若有前值，則補前值，若無前值則補後值
        bf_cols = ['available_rent_bikes', 'available_return_bikes', 'is_disabled']
        for col in bf_cols:
            ts_complete_data[col] = ts_complete_data[col].ffill(axis='index')
            ts_complete_data[col] = ts_complete_data[col].bfill(axis='index')
        # 統一值
        ts_complete_data['stop_id'] = stop_id
        first_cols = [
            'stop_name', 'service_status', 'capacity', 'dist', 'lng', 'lat', 'service_type'
        ]
        for col in first_cols:
            ts_complete_data[col] = group_data[col].iloc[-1]
        # 補0
        is_na = ts_complete_data['raw_data_count'].isna()
        ts_complete_data.loc[is_na, 'raw_data_count'] = 0
        ts_complete_data.loc[is_na, 'raw_data_disabled_count'] = 0
        results.append(ts_complete_data)
    results = pd.concat(results)
    results = results.reset_index(drop=True)
    return results


# Config
root_path = r'D:\iima\ubike分析'
ym = '202403'
status_path = f'{root_path}/DM/{ym}/prepared_data/status'
step_minute = 5
stime = time.time()

# load
ubike_status = pd.read_csv(f'{status_path}/unique_raw.csv')

# Aggregate by step_minute
ubike_status['data_time'] = pd.to_datetime(ubike_status['data_time'])
if step_minute == 1:
    ubike_status['data_time'] = ubike_status['data_time'].dt.round('minute').copy()
else:
    ubike_status['data_time'] = ubike_status['data_time'].dt.round(f'{step_minute}T').copy()
# 2024/04/16 該區間有多筆時取第一筆(也就是抽樣，若使用min, median會有 rent+reurn != capacity的情況)
# (2024/04/16之前是使用min)
ubike_status_by_5m = ubike_status.groupby(
    ['stop_id', 'data_time']
).agg(
    stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
    service_status=pd.NamedAgg(column='service_status', aggfunc='first'),
    raw_data_count=pd.NamedAgg(column='stop_name', aggfunc='count'),
    is_disabled=pd.NamedAgg(column='service_status', aggfunc='min'),
    raw_data_disabled_count=pd.NamedAgg(column='service_status', aggfunc='sum'),
    available_rent_bikes=pd.NamedAgg(column='available_rent_bikes', aggfunc='first'),
    available_return_bikes=pd.NamedAgg(column='available_return_bikes', aggfunc='first'),
    capacity=pd.NamedAgg(column='capacity', aggfunc='first'),
    dist=pd.NamedAgg(column='dist', aggfunc='first'),
    lng=pd.NamedAgg(column='lng', aggfunc='first'),
    lat=pd.NamedAgg(column='lat', aggfunc='first'),
    service_type=pd.NamedAgg(column='service_type', aggfunc='first')
).reset_index()
ubike_status_by_5m['is_disabled'] = (ubike_status_by_5m['is_disabled'] == 0)
ubike_status_by_5m['raw_data_disabled_count'] = (
    ubike_status_by_5m['raw_data_count'] - ubike_status_by_5m['raw_data_disabled_count']
)
# save
file_path = status_path+'/unique_by_5minute.csv'
ubike_status_by_5m.to_csv(file_path, index=False, encoding='UTF-8')
print(f'Finished generate info, cost {time.time() - stime} secs.')

# 處理missing value
y = int(ym[0:4])
m = int(ym[4:6])
ubike_status_by_5m_filled = fill_empty_data(
    ubike_status_by_5m,
    start_year=y, start_month=m,
    end_year=y, end_month=m,
    step_minutes=step_minute
)
ubike_status_by_5m_filled['date'] = ubike_status_by_5m_filled['data_time'].dt.date
ubike_status_by_5m_filled['hour'] = ubike_status_by_5m_filled['data_time'].dt.hour
# 計算沒車滿站
ubike_status_by_5m_filled['is_empty'] = (ubike_status_by_5m_filled['available_rent_bikes'] == 0)
ubike_status_by_5m_filled['is_full'] = (ubike_status_by_5m_filled['available_return_bikes'] == 0)
# 連續幾分鐘沒車 or 滿站 = 某個站未有新交易，維持空/爆狀態
# = 同樣stop_id & 這次無回傳 & 上次爆 & 這次爆
# 同樣stop_id
is_continuous_id = (
    ubike_status_by_5m_filled['stop_id'] == ubike_status_by_5m_filled['stop_id'].shift(1)
)
is_continuous_date = (
    ubike_status_by_5m_filled['date'] == ubike_status_by_5m_filled['date'].shift(1)
)
is_current_no_return = (ubike_status_by_5m_filled['raw_data_count'] == 0)
is_continuous_status = is_continuous_id & is_current_no_return & is_continuous_date
# 這次無回傳
is_disabled = ubike_status_by_5m_filled['is_disabled']
is_target_time = (ubike_status_by_5m_filled['hour'] >= 6)
is_to_keep = (~is_disabled) & is_target_time
# 上次爆 & 這次爆
is_still_empty = (
    ubike_status_by_5m_filled['is_empty'].shift(1) & ubike_status_by_5m_filled['is_empty']
)
is_still_full = (
    ubike_status_by_5m_filled['is_full'].shift(1) & ubike_status_by_5m_filled['is_full']
)
is_continuous_empty = is_continuous_status & is_to_keep & is_still_empty
is_continuous_full = is_continuous_status & is_to_keep & is_still_full
# add to dataframe
ubike_status_by_5m_filled['is_continuous_empty'] = is_continuous_empty
ubike_status_by_5m_filled['is_continuous_full'] = is_continuous_full

# check
# temp = ubike_status_by_5m_filled.loc[ubike_status_by_5m_filled['stop_id']=='U113065']
# temp1 = ubike_status_by_5m_filled.loc[ubike_status_by_5m_filled['stop_id']=='U113021']

# save
file_path = f'{status_path}/filled_missing_value_by_5minute.csv'
ubike_status_by_5m_filled.to_csv(file_path, index=False, encoding='UTF-8')
print(f'Finished generate info, cost {time.time() - stime} secs.')
