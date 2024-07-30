import os
import sys
import pandas as pd
import xlrd
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import make_sure_folder_exist

# Config
YM = '202403'
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH = f'{ROOT_PATH}/DW/raw_tie/{YM}'
OUTPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/tie'

# Load data
file_names = os.listdir(INPUT_PATH)
data = []
for file_name in file_names:
    wb = xlrd.open_workbook(os.path.join(INPUT_PATH, file_name), ignore_workbook_corruption=True)
    temp = pd.read_excel(wb)
    data.append(temp)
tie = pd.concat(data, ignore_index=True)

# Transform data
# date
try:
    tie['date'] = pd.to_datetime(tie['日期'], format='%Y年%m月%d日')
except ValueError:
    tie['date'] = pd.to_datetime(tie['日期'])
tie['date'] = tie['date'].dt.date.astype(str)
# stop_id
tie['場站代號'] = tie['場站代號'].astype(str)
is_start_with_500 = tie['場站代號'].str.startswith('500')
tie.loc[is_start_with_500, '場站代號'] = 'U' + tie.loc[is_start_with_500, '場站代號'].str.slice(3,)

# Reshape data
tie.drop(columns=['日期', '城市'], inplace=True)
tie_long = tie.melt(
    id_vars=['責任區', '責任群', '場站代號', '場站名稱', 'date'],
    var_name='hour',
    value_name='tie_bike'
)

# Rename columns
tie_long.rename(
    columns={
        '責任區': 'dist',
        '責任群': 'group',
        '場站代號': 'stop_id',
        '場站名稱': 'stop_name',
        'date': 'date',
        'hour': 'hour',
        'tie_bike': 'tie_bike',
    },
    inplace=True
)

# Save data
make_sure_folder_exist(OUTPUT_PATH)
tie_long.to_csv(f'{OUTPUT_PATH}/tie_by_stop_by_date_by_hour.csv', index=False)
