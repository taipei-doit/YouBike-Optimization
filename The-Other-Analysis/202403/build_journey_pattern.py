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

# Insert to directed graph
DG = nx.DiGraph()
# add nodes (= station)
for _, row in station.iterrows():
    DG.add_node(
        # node id
        row['stop_id'],
        # node attributes
        name = row['stop_name'],
        version = row['service_type'],
        capacity = row['bike_capacity'],
        county = row['county'],
        address = row['addr'],
        latitude=row['lat'], longtitude=row['lng']
    )
# add edges (= transaction)
total_loop = txn.shape[0]
start_time = time.time()
for _, row in txn.iterrows():
    DG.add_edge(
        # necessary
        row['on_stop_id'], row['off_stop_id'], weight=1,
        # edge attributes
        edge_type = 'txn',
        card_id = row['card_id'],
        bike_id = row['route_name'],
        rent_time = row['on_time'],
        return_time = row['off_time'],
        date = row['date'],
        weekday = row['weekday'],
        weekday_type = row['weekday_type']
    )
    # print progress
    if (_ % 100000) == 0:
        print(f'{_}/{total_loop}({_ / total_loop:.2%}) cost {time.time() - start_time:.2f} sec')

# Save
nx.write_gpickle(DG, OUTPUT_PATH+'/journey_graph.gpickle')
