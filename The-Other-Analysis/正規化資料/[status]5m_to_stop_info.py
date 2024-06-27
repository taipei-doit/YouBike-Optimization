import pandas as pd

# config
root_path = r'D:\iima\ubike分析'
ym = '202402'
status_path = root_path + f'/DM/{ym}/prepared_data/status'

# load
ubike_status_unique = pd.read_csv(f'{status_path}/unique_raw.csv')

# extract stop info
ubike_stops = ubike_status_unique.groupby('stop_id').agg({
    'stop_name': 'first',
    'dist': 'first',
    'capacity': 'first',
    'lat': 'first',
    'lng': 'first',
    'service_type': 'first'
}).reset_index()
file_path = f'{root_path}/DIM/ubike_stops_from_api_{ym}.csv'
ubike_stops.to_csv(file_path, index=False, encoding='utf8')
