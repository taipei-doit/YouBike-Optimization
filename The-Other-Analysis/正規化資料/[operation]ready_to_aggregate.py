import pandas as pd
import numpy as np

# Config
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'
DISPATCH_TYPES = ['調入', '調出', '綁車', '解綁車']

# Load
operation = pd.read_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_operation_log.csv')
operation = operation.loc[operation['工作狀態'].isin(DISPATCH_TYPES)]
operation.rename(
    columns={
        '城市': 'city',
        '責任區': 'dist',
        '責任群': 'cluster',
        '場站代號': 'stop_id',
        '場站名稱': 'stop_name',
        '平板裝置碼': 'device_id',
        '車輛號碼': 'truck_id',
        '工作性質': 'role',
        '人員編號': 'id',
        '人員姓名': 'name',
        '工作狀態': 'operation_type',
        '車輛數': 'bike_count',
        '備註': 'ps',
        '更新時間': 'update_time',
        '抵達時間': 'arrive_time',
    },
    inplace=True
)

# Add new columns
operation['data_time'] = pd.to_datetime(operation['update_time'])
operation['date'] = operation['data_time'].dt.date
operation['hour'] = operation['data_time'].dt.hour
operation['tie_bike'] = np.where(
    operation['operation_type'] == '綁車', operation['bike_count'], 0
)
operation['untie_bike'] = np.where(
    operation['operation_type'] == '解綁車', operation['bike_count'], 0
)
operation['load_bike'] = np.where(
    operation['operation_type'] == '調出', operation['bike_count'], 0
)
operation['unload_bike'] = np.where(
    operation['operation_type'] == '調入', operation['bike_count'], 0
)

# Agg output by date by hour by position
agg_by_date_by_hour_by_role = operation.groupby(
    ['stop_id', 'date', 'hour', 'role']
).agg(
    stop_name = pd.NamedAgg(column='stop_name', aggfunc='first'),
    operation_count = pd.NamedAgg(column='device_id', aggfunc='count'),
    dispatcher_count = pd.NamedAgg(column='name', aggfunc='nunique'),
    operation_bike_sum = pd.NamedAgg(column='bike_count', aggfunc='sum'),
    tie_bike_sum = pd.NamedAgg(column='tie_bike', aggfunc='sum'),
    untie_bike_sum = pd.NamedAgg(column='untie_bike', aggfunc='sum'),
    load_bike_sum = pd.NamedAgg(column='load_bike', aggfunc='sum'),
    unload_bike_sum = pd.NamedAgg(column='unload_bike', aggfunc='sum')
).reset_index()
# Add full time and part time count
agg_by_date_by_hour_by_role['full_time_count'] = np.where(
    agg_by_date_by_hour_by_role['role'] == '正職',
    agg_by_date_by_hour_by_role['dispatcher_count'],
    0
)
agg_by_date_by_hour_by_role['part_time_count'] = np.where(
    agg_by_date_by_hour_by_role['role'] == '工讀',
    agg_by_date_by_hour_by_role['dispatcher_count'],
    0
)

# Agg output by date by hour
agg_by_date_by_hour = agg_by_date_by_hour_by_role.groupby(
    ['stop_id', 'date', 'hour']
).agg(
    stop_name = pd.NamedAgg(column='stop_name', aggfunc='first'),
    ft_dispatcher = pd.NamedAgg(column='full_time_count', aggfunc='sum'),
    pt_dispatcher = pd.NamedAgg(column='part_time_count', aggfunc='sum'),
    operation_count = pd.NamedAgg(column='operation_count', aggfunc='sum'),
    operation_bike = pd.NamedAgg(column='operation_bike_sum', aggfunc='sum'),
    tie_bike = pd.NamedAgg(column='tie_bike_sum', aggfunc='sum'),
    untie_bike = pd.NamedAgg(column='untie_bike_sum', aggfunc='sum'),
    load_bike = pd.NamedAgg(column='load_bike_sum', aggfunc='sum'),
    unload_bike = pd.NamedAgg(column='unload_bike_sum', aggfunc='sum')
).reset_index()

# Save
agg_by_date_by_hour.to_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/operation_agg_by_stop_by_date_hour.csv',
    index=False
)
