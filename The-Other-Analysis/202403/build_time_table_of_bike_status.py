import sys
import pandas as pd
import time
import pickle

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
sys.path.append(f'{ROOT_PATH}/CODE')
from udf_function import make_sure_folder_exist
OUTPUT_PATH =  f'{ROOT_PATH}/DM/202403/bike_status'
make_sure_folder_exist(OUTPUT_PATH)
YM = '202311'
TIME_INTERVAL = 10  # minutes
INTERVALS_A_DAY = 24 * 60 / TIME_INTERVAL

# Read
txn = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/txn/txn_only_ubike.csv')

# Transform
txn['on_time'] = pd.to_datetime(txn['on_time'])
txn['off_time'] = pd.to_datetime(txn['off_time'])
txn['date'] = txn['on_time'].dt.date
# tuncate time to 10 minutes
txn['on_time_10m_trunc'] = txn['on_time'].dt.round('10T')
txn['on_time_10m_trunc_only_time'] = txn['on_time_10m_trunc'].dt.time.astype(str)
txn['off_time_10m_trunc'] = txn['off_time'].dt.round('10T')
txn['off_time_10m_trunc_only_time'] = txn['off_time_10m_trunc'].dt.time.astype(str)
print(txn.iloc[0])

# Record the status of the bike for each date and time split into 10 minutes
# 0: not occupied, 1: occupied
not_occurred_whole_day = pd.Series(
    [0] * int(INTERVALS_A_DAY),
    index=[f'{i:02d}:{j:02d}:00' for i in range(24) for j in range(0, 60, TIME_INTERVAL)]
)
# record occupied time
start_time = time.time()
records = {}
for date, daily_txn in txn.groupby('date'):
    daily_records = {}
    total_bike = len(set(daily_txn['route_name']))
    # print(f'Processing {date} with {total_bike} bikes..')
    bike_count = 0
    for bike_id, a_bike_txn in daily_txn.groupby('route_name'):
        bike_record = not_occurred_whole_day.copy()
        # mark the time bike been rented or returned
        bike_record.loc[a_bike_txn['on_time_10m_trunc_only_time']] = 1
        bike_record.loc[a_bike_txn['off_time_10m_trunc_only_time']] = 2
        # mark the time between rented and returned in pandas
        in_range = False
        for i, record in enumerate(bike_record):
            if record == 1:  # rented
                in_range = True
            elif record == 2 and in_range:  # returned
                in_range = False
            elif in_range:
                bike_record.iloc[i] = 1
        daily_records[bike_id] = bike_record
        # print progress
        bike_count += 1
        # if bike_count % 1000 == 0:
        #     print(f'Processing {bike_count}/{total_bike} bikes, cost {time.time()-start_time:.2f}s..')
    records[str(date)] = pd.concat(daily_records, axis=1).T
    print(f'Processing {date} done, cost {time.time()-start_time:.2f}s..')

# Save whole records to pickle
with open(f'{OUTPUT_PATH}/bike_status.pkl', 'wb') as f:
    pickle.dump(records, f)
# to csv for each date
for date, daily_records in records.items():
    daily_records.to_csv(f'{OUTPUT_PATH}/bike_status_{date}.csv')
