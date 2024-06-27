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
is_target_dist = (ava['責任區'] == 'ZH2')
is_target_status = (ava['狀態'] == '無車')
target_data = ava.loc[is_target_status]
total_empty_minute = target_data['9'].sum()
unique_stop_count = len(set(ava['場站代號']))
1 - (total_empty_minute / (60*unique_stop_count))