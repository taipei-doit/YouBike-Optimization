import os
import sys
import pandas as pd
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import make_sure_folder_exist

# Config
YM = '202311'
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH = f'{ROOT_PATH}/DW/raw_availability/{YM}'
OUTPUT_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data/availability'

# Load data
file_name = os.listdir(INPUT_PATH)
file_type = file_name[0].split('.')[-1]
if file_type == 'csv':
    availability = pd.read_csv(
        f'{INPUT_PATH}/{file_name[0]}', encoding='utf-8')
elif file_type == 'xlsx':
    availability = pd.read_excel(f'{INPUT_PATH}/{file_name[0]}')
else:
    raise Exception('File type not supported!')
# nickname
ava = availability

# Filter data
ava = ava.loc[ava['項目'] == '分鐘']
ava.drop(columns=['總計', '項目'], inplace=True)

# Transform data
# date
try:
    ava['date'] = pd.to_datetime(ava['date'], format='%Y年%m月%d日')
except ValueError:
    ava['date'] = pd.to_datetime(ava['date'])
ava['date'] = ava['date'].dt.date.astype(str)
# stop_id
ava['場站代號'] = ava['場站代號'].astype(str)
is_start_with_500 = ava['場站代號'].str.startswith('500')
ava.loc[is_start_with_500, '場站代號'] = 'U' + ava.loc[is_start_with_500, '場站代號'].str.slice(3,)
# status
ava['狀態'] = ava['狀態'].map({'無車': 'empty', '無位': 'full'})

# Reshape data
ava = ava.melt(
    id_vars=['責任區', '場站代號', '站名', 'date', '狀態'],
    var_name='hour',
    value_name='minutes'
)
ava = ava.pivot_table(
    index=['責任區', '場站代號', '站名', 'date', 'hour'],
    columns='狀態',
    values='minutes'
).reset_index()

# Rename columns
ava.rename(
    columns={
        '責任區': 'dist',
        '場站代號': 'stop_id',
        '站名': 'stop_name',
        'date': 'date',
        'hour': 'hour',
        'empty': 'empty_minute',
        'full': 'full_minute'
    },
    inplace=True
)

# Save data
make_sure_folder_exist(OUTPUT_PATH)
ava.to_csv(f'{OUTPUT_PATH}/availability_by_stop_by_date_by_hour.csv', index=False)
