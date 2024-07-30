import pandas as pd

# Constants
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'
NTU_REGION = ['ZB1', 'ZB2', 'ZB3']

# Load
# dispatch region
region = pd.read_csv(f'{ROOT_PATH}/DIM/dispatch_region_{YM}.csv')
# status detail
status = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/閒置車/simulation_results_detail_part.csv')
# dock suggestion
dock = pd.read_excel(f'{ROOT_PATH}/DM/{YM}/全策略/理想站體與配置.xlsx')
# stop
stop = pd.read_csv(f'{ROOT_PATH}/DIM/ubike_stops_from_api_202403.csv')

# Transform
# keep NTU stop
target_stop_id = region.loc[region['region'].isin(NTU_REGION), 'stop_id']
target_status = status.loc[status['stop_id'].isin(target_stop_id)]
target_dock = dock.loc[dock['ID'].isin(target_stop_id)]
target_region = region.loc[region['stop_id'].isin(target_stop_id)]
# merge stop ingo to region
target_region = target_region.merge(stop[['stop_id', 'capacity', 'dist', 'lng', 'lat']], on='stop_id', how='left')

# Save
target_status.to_csv(
    f'{ROOT_PATH}/DM/{YM}/7th_meeting/ntu_simulation_results_detail_part.csv',
    index=False,
    encoding='UTF-8'
)
dock.to_csv(
    f'{ROOT_PATH}/DM/{YM}/7th_meeting/ntu_dock_suggestion.csv',
    index=False,
    encoding='UTF-8'
)
target_region.to_csv(
    f'{ROOT_PATH}/DM/{YM}/7th_meeting/ntu_stop.csv',
    index=False,
    encoding='UTF-8'
)
