import pandas as pd
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import (
    load_ubike_stop,
    load_stop_mapping_table,
    convert_str_to_time_format,
    make_sure_folder_exist
)

# Config
ym = '202403'
root_path = r'D:\iima\ubike分析'
raw_dispatch_path = f'{root_path}/DW/raw_dispatch/{ym}/dispatch_card_{ym}.xlsx'
dispatch_path = f'{root_path}/DM/{ym}/prepared_data/dispatch'
dim_path = f'{root_path}/DIM'

# Load
# dispatch card
try:
    raw = pd.read_excel(raw_dispatch_path, sheet_name='工作表1')
except ValueError:
    raw = pd.read_excel(raw_dispatch_path, sheet_name='Sheet1')
if raw.empty:
    raise ValueError(f'No data in {raw_dispatch_path}')

# Process
# cleansing
is_no_off_stop = raw['還車場站'].isna()
is_no_on_stop = raw['借車場站'].isna()
is_stop_empty = is_no_off_stop | is_no_on_stop
raw_filter = raw.loc[~is_stop_empty]
# reshape
col_map = {
    '借車時間': 'on_time',
    '借車場站': 'on_stop',
    '借車代號': 'on_stop_id',
    '還車時間': 'off_time',
    '還車場站': 'off_stop', 
    '還車代號': 'off_stop_id',
    '車號': 'bike_id'
}
raw_filter = raw_filter.rename(columns=col_map)

# The dispatch data for the new ym seems to not require some of the code from
# previous years. However, to avoid affecting the old years, the new ym will 
# be executed independently.
if ym in ['202311']:
    in_dispatch = raw_filter[['bike_id', 'off_stop', 'off_time', 'off_stop_id']]
    in_dispatch.columns = ['bike_id', 'stop_name', 'txn_time', 'stop_id']
    in_dispatch['dispatch_type'] = 'in'
    out_dispatch = raw_filter[['bike_id', 'on_stop', 'on_time', 'on_stop_id']]
    out_dispatch.columns = ['bike_id', 'stop_name', 'txn_time', 'stop_id']
    out_dispatch['dispatch_type'] = 'out'
    dispatch = pd.concat([in_dispatch, out_dispatch])
    # time 
    dispatch['txn_time'] = convert_str_to_time_format(dispatch['txn_time'])
    # stop_id
    dispatch['stop_id'] = dispatch['stop_id'].astype(str)
    is_start_with_500 = dispatch['stop_id'].str.startswith('500')
    dispatch.loc[is_start_with_500, 'stop_id'] = (
        'U' + dispatch.loc[is_start_with_500, 'stop_id'].str.slice(3, )
    )
else:
    in_dispatch = raw_filter[['bike_id', 'off_stop', 'off_time']]
    in_dispatch.columns = ['bike_id', 'stop_name', 'txn_time']
    in_dispatch['dispatch_type'] = 'in'
    out_dispatch = raw_filter[['bike_id', 'on_stop', 'on_time']]
    out_dispatch.columns = ['bike_id', 'stop_name', 'txn_time']
    out_dispatch['dispatch_type'] = 'out'
    dispatch = pd.concat([in_dispatch, out_dispatch])
    # time 
    dispatch['txn_time'] = convert_str_to_time_format(dispatch['txn_time'])
    # mapping id
    dispatch['stop_name'] = dispatch['stop_name'].str.replace('臺北流行', '台北流行')
    ubike_stop = load_ubike_stop(ym, dim_path)
    # mapping id table
    mapping_table = load_stop_mapping_table(dim_path)
    # 站名->ID，以ubike提供mapping表為主，但因為沒有新北，所以另外加其他的來源
    mapping_dict = {row['stop_name']: row['stop_id'] for _, row in ubike_stop.iterrows()}
    for _, row in mapping_table.iterrows():
        mapping_dict[row['stop_name']] = row['stop_id']
    dispatch['stop_id'] = dispatch['stop_name'].map(mapping_dict)

# Drop missing data
is_mapping = ~dispatch['stop_id'].isna()
dispatch = dispatch.loc[is_mapping]
# dispatch.loc[(dispatch['stop_id']=='U112064') & (dispatch['txn_time'].dt.date.astype(str)=='2023-11-06')]

# Save
make_sure_folder_exist(dispatch_path)
file_path = dispatch_path+'/cleaned_raw.csv'
is_ym = dispatch['txn_time'].dt.month == int(ym[-2:])
dispatch.loc[is_ym].to_csv(file_path, index=False, encoding='utf-8')
