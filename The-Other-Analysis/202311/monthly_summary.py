import pandas as  pd
import time

# Config
root_path = 'D:/iima/ubike分析'
yms = ['202303', '202304', '202307', '202309', '202311']
day_count = {
    '202303': {'weekday': 23, 'weekend': 10},
    '202304': {'weekday': 20, 'weekend': 10},
    '202307': {'weekday': 21, 'weekend': 10},
    '202309': {'weekday': 21, 'weekend': 9},
    '202311': {'weekday': 22, 'weekend': 8},
}

# Process
results = {
    'ym': [], 'txn_count': [], 'weekday_count': [], 'weekend_count': [],
    'stop_count': [], 'dock_count': [],
    'daily_txn_bike_mean': [], 'inout_bike_count': [],
    'dis_operation_count': [], 'daily_operation_mean': [],
}
start_time = time.time()
for ym in yms:
    # break
    # Basic
    results['ym'].append(ym)
    weekday_count = day_count[ym]['weekday']
    results['weekday_count'].append(weekday_count)
    weekend_count = day_count[ym]['weekend']
    results['weekend_count'].append(weekend_count)
    
    # Transaction
    txn = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/txn/txn_only_ubike.csv')
    txn_count = txn.shape[0]
    results['txn_count'].append(txn_count)
    # Trabnsaction daily
    daily_txn_bike_mean = txn.groupby('data_date')['route_name'].nunique().mean()
    results['daily_txn_bike_mean'].append(daily_txn_bike_mean)
    
    # Station
    stop = pd.read_csv(f'{root_path}/DIM/ubike_stops_from_api_{ym}.csv')
    stop_count = stop.shape[0]
    results['stop_count'].append(stop_count)
    dock_count = stop['capacity'].sum()
    results['dock_count'].append(dock_count)
    
    # Dispatch Card
    dispatch_card = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/dispatch/cleaned_raw.csv')
    inout_bike_count = dispatch_card.shape[0]
    results['inout_bike_count'].append(inout_bike_count)
    
    # Operation
    if ym == '202304':
        results['dis_operation_count'].append(None)
        results['daily_operation_mean'].append(None)
    else:
        operation = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/dispatch/dispatch_operation_log.csv')
        dis_operation_count = operation.shape[0]
        results['dis_operation_count'].append(dis_operation_count)
        operation['date'] = pd.to_datetime(operation['抵達時間']).dt.date
        daily_operation_mean = operation.groupby('date')['人員姓名'].nunique().mean()
        results['daily_operation_mean'].append(daily_operation_mean)
    
    print(f'Done {ym}, cost {time.time() - start_time} secs.')
data = pd.DataFrame(results)

# Save
data.to_excel(f'{root_path}/DM/202311/市長會/各月份統計資料.xlsx', index=False)