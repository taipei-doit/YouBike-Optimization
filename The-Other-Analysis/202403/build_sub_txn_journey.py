import sys
import pandas as pd
import networkx as nx
import time

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
sys.path.append(f'{ROOT_PATH}/CODE')
from udf_function import make_sure_folder_exist
OUTPUT_PATH =  f'{ROOT_PATH}/DM/202403/journey'
make_sure_folder_exist(OUTPUT_PATH)
YM = '202311'

# Read data
station = pd.read_csv(f'{ROOT_PATH}/DIM/ubike_stop_{YM}.csv')
txn = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/txn/txn_only_ubike.csv')

# Transform data
# station
station['service_type'] = station['service_type'].str.extract('([12])').astype(int)
station['lng'] = pd.to_numeric(station['lng'], errors='coerce')
station['lat'] = pd.to_numeric(station['lat'], errors='coerce')
# txn
txn['on_time'] = pd.to_datetime(txn['on_time'])
txn['on_hour'] = txn['on_time'].dt.hour
txn['date'] = txn['on_time'].dt.date
txn['weekday'] = txn['on_time'].dt.weekday + 1
txn['weekday_type'] = txn['weekday'].apply(lambda x: 'weekday' if x < 6 else 'weekend')
txn['off_time'] = pd.to_datetime(txn['off_time'])
txn['off_hour'] = txn['off_time'].dt.hour

def build_direction_graph(weekday_type='all', on_hour=[], lower_bound=0):
    print(f'Condition: weekday_type={weekday_type}, on_hour={on_hour}')
    sub_txn = txn.copy()

    # Filter txn
    if weekday_type != 'all':
        sub_txn = sub_txn.loc[sub_txn['weekday_type'] == weekday_type]
    if len(on_hour) > 0:
        sub_txn = sub_txn.loc[sub_txn['on_hour'].isin(on_hour)]

    # Count txn numbers between each station
    txn_count = sub_txn.groupby(['on_stop_id', 'off_stop_id']).size().reset_index(name='count')
    txn_count = txn_count.loc[txn_count['count'] > lower_bound]

    # Insert to directed graph
    dg = nx.DiGraph()
    # add nodes (= station)
    for _, row in station.iterrows():
        dg.add_node(
            # node id
            row['stop_name'],
            # node attributes
            id = row['stop_id'],
            version = row['service_type'],
            capacity = row['bike_capacity'],
            county = row['county'],
            address = row['addr'],
            latitude=row['lat'], longtitude=row['lng']
        )
    # add edges (= transaction)
    total_loop = txn_count.shape[0]
    start_time = time.time()
    for _, row in txn_count.iterrows():
        dg.add_edge(
            row['on_stop_id'], row['off_stop_id'], weight=row['count'],
        )
        # print progress
        if (_ % 10000) == 0:
            print(f'{_}/{total_loop}({_ / total_loop:.2%}) cost {time.time() - start_time:.2f} sec')

    # Save
    on_hour_str = ''.join(map(str, on_hour))
    nx.write_gpickle(dg, f'{OUTPUT_PATH}/journey_graph_{weekday_type}_{on_hour_str}.gpickle')
    print(f'Saved to {OUTPUT_PATH}/journey_graph_{weekday_type}_{on_hour_str}.gpickle')

# build full txn graph
build_direction_graph(weekday_type='all', on_hour=[], lower_bound=0)
# build weekday txn graph
build_direction_graph(weekday_type='weekday', on_hour=[], lower_bound=0)
# build weekend txn graph
build_direction_graph(weekday_type='weekend', on_hour=[], lower_bound=0)