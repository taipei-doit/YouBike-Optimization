import pandas as  pd
import numpy as np

# Config
root_path = 'D:/iima/ubike分析'
ym = '202307'
dispatch_types = ['調入', '調出', '綁車', '解綁車']

# Load
operation = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/dispatch/dispatch_operation_log.csv')
operation = operation.loc[operation['工作狀態'].isin(dispatch_types)]
operation.rename(columns={'工作性質': 'role', '人員姓名': 'name'}, inplace=True)

# Add new columns
operation['data_time'] = pd.to_datetime(operation['更新時間'])
operation['date'] = operation['data_time'].dt.date
operation['hour'] = operation['data_time'].dt.hour
operation['weekday'] = operation['data_time'].dt.weekday + 1
operation['weekday_type'] = np.where(operation['weekday']<=5, 'weekday', 'weekend')
operation['tie_bike'] = np.where(operation['工作狀態']=='綁車', operation['車輛數'], 0)
operation['untie_bike'] = np.where(operation['工作狀態']=='解綁車', operation['車輛數'], 0)
operation['load_bike'] = np.where(operation['工作狀態']=='調出', operation['車輛數'], 0)
operation['unload_bike'] = np.where(operation['工作狀態']=='調入', operation['車輛數'], 0)

# Agg output by date by hour by position by man
agg_detail = operation.groupby(
    ['weekday_type', 'date', 'hour', 'role', 'name']
).agg(
    operation_count = pd.NamedAgg(column='平板裝置碼', aggfunc='count'),
    dist_nunique = pd.NamedAgg(column='責任區', aggfunc='nunique'),
    cluster_unique = pd.NamedAgg(column='責任群', aggfunc='nunique'),
    stop_nunique = pd.NamedAgg(column='場站代號', aggfunc='nunique'),
    bike_sum = pd.NamedAgg(column='車輛數', aggfunc='sum'),
    tie_bike_sum = pd.NamedAgg(column='tie_bike', aggfunc='sum'),
    untie_bike_sum = pd.NamedAgg(column='untie_bike', aggfunc='sum'),
    load_bike_sum = pd.NamedAgg(column='load_bike', aggfunc='sum'),
    unload_bike_sum = pd.NamedAgg(column='unload_bike', aggfunc='sum')
).reset_index()

# Agg mean output by hour by position by man
agg_by_date_by_hour_by_role = agg_detail.groupby(
    ['date', 'hour', 'role']
).agg(
    dispatcher_count = pd.NamedAgg(column='name', aggfunc='nunique'),
    operations_hourly = pd.NamedAgg(column='operation_count', aggfunc='mean'),
    unique_dist_hourly = pd.NamedAgg(column='dist_nunique', aggfunc='mean'),
    unique_cluster_hourly = pd.NamedAgg(column='cluster_unique', aggfunc='mean'),
    unique_stop_hourly = pd.NamedAgg(column='stop_nunique', aggfunc='mean'),
    bike_hourly = pd.NamedAgg(column='bike_sum', aggfunc='mean'),
    tie_bike_hourly = pd.NamedAgg(column='tie_bike_sum', aggfunc='mean'),
    untie_bike_hourly = pd.NamedAgg(column='untie_bike_sum', aggfunc='mean'),
    load_bike_hourly = pd.NamedAgg(column='load_bike_sum', aggfunc='mean'),
    unload_bike_hourly = pd.NamedAgg(column='unload_bike_sum', aggfunc='mean')
).reset_index()

# Agg mean output by hour by position
agg_by_hour_by_role = agg_by_date_by_hour_by_role.groupby(
    ['hour', 'role']
).agg(
    dispatcher_hourly = pd.NamedAgg(column='dispatcher_count', aggfunc='mean'),
    operations_hourly = pd.NamedAgg(column='operations_hourly', aggfunc='mean'),
    unique_dist_hourly = pd.NamedAgg(column='unique_dist_hourly', aggfunc='mean'),
    unique_cluster_hourly = pd.NamedAgg(column='unique_cluster_hourly', aggfunc='mean'),
    unique_stop_hourly = pd.NamedAgg(column='unique_stop_hourly', aggfunc='mean'),
    bike_hourly = pd.NamedAgg(column='bike_hourly', aggfunc='mean'),
    tie_bike_hourly = pd.NamedAgg(column='tie_bike_hourly', aggfunc='mean'),
    untie_bike_hourly = pd.NamedAgg(column='untie_bike_hourly', aggfunc='mean'),
    load_bike_hourly = pd.NamedAgg(column='load_bike_hourly', aggfunc='mean'),
    unload_bike_hourly = pd.NamedAgg(column='unload_bike_hourly', aggfunc='mean')
).reset_index()

# Pivot to wide format
wide_agg_table = agg_by_hour_by_role.pivot_table(
    index=['role'],
    columns=['hour'],
    values=['unique_stop_hourly']
).reset_index()
wide_agg_table['type'] = '日均調度站數'

# Save
#! Do not save to overwrite 分時_日均調度人員調度站數.xlsx.
# 此py的code是遺失後，試著reproduce的。但試了許久，還是與之前的產出有些微差距。
# 因此僅用來大概趨近之前的做法，實際產出以原本的產出檔案為準
# wide_agg_table.to_excel(
#     f'{root_path}/DM/202311/市長會/分時_日均調度人員調度站數_test.xlsx',
#     index=False
# )