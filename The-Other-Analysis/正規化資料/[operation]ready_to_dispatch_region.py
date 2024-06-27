import pandas as pd

# Config
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'

# Load
operation = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_operation_log.csv')

# Extract dispatch region
region = operation.groupby(['場站代號', '場站名稱']).agg(
    region=pd.NamedAgg(column='責任區', aggfunc='first'),
    region_count=pd.NamedAgg(column='責任區', aggfunc='nunique'),
    group=pd.NamedAgg(column='責任群', aggfunc='first'),
    group_count=pd.NamedAgg(column='責任群', aggfunc='nunique')
).reset_index()
# validate that one stop has only one dispatch region
assert  (region['region_count']>1).sum() == 0, 'Stop has multiple dispatch regions'
assert  (region['group_count']>1).sum() == 0, 'Stop has multiple dispatch groups'

# Cleansing
region.drop(columns=['region_count', 'group_count'], inplace=True)
region.rename(columns={'場站代號': 'stop_id', '場站名稱': 'stop_name'}, inplace=True)

# Save
region.to_csv(f'{ROOT_PATH}/DIM/dispatch_region_{YM}.csv', index=False)