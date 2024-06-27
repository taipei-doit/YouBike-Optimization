# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 21:01:45 2023

@author: rz3881
"""

import pandas as pd
# import datetime

ym = '202307'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'
weekday_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
    6: 'weekend', 7: 'weekend'
}

# Load
compare_detail = pd.read_csv(idle_path + '/compare_detail.csv')
dispatch_operate = pd.read_csv(
    root_path+f'/DM/{ym}/prepared_data/dispatch/dispatch_operation_log.csv'
)

# define 
dispatch_operate = dispatch_operate.loc[dispatch_operate['工作狀態'].isin(['綁車', '解綁車'])]
dispatch_operate['更新時間'] = pd.to_datetime(dispatch_operate['更新時間'])
dispatch_operate['date'] = dispatch_operate['更新時間'].dt.date
# dispatch_operate['date_m6h'] = (dispatch_operate['更新時間'] - pd.Timedelta(hours=6)).dt.date
# dispatch_operate = dispatch_operate.loc[dispatch_operate['date_m6h']!=datetime.date(2023, 6, 30)]
dispatch_operate['weekday'] = dispatch_operate['更新時間'].dt.weekday + 1
dispatch_operate['weekday_type'] = dispatch_operate['weekday'].map(weekday_map)
compare_detail['weekday_type'] = compare_detail['weekday_m6h'].map(weekday_map)
compare_detail['is_dispatch'] = compare_detail['sum_abs_dispatch'] > 0

# agg by stop_id, weekday_type
empty_dispatch = compare_detail.groupby(['stop_id', 'weekday_type']).agg({
    'stop_name': 'first',
    'date_m6h': 'nunique',  # day count
    'capacity': 'first',
    'empty_minutes': 'median',  #  空車分鐘
    'is_dispatch': 'sum',  # 實際調度車數加總
    'sum_abs_dispatch': 'mean'
}).reset_index()
empty_dispatch = empty_dispatch.rename(
    columns={'date_m6h': 'day_count',
             'is_dispatch': 'card_dis_day_count'}
)
dis_agg = dispatch_operate.groupby(['場站代號', 'weekday_type']).agg({
    '場站名稱': 'first',
    'date': 'nunique'
}).reset_index()
dis_agg.columns = ['stop_id', 'weekday_type', 'stop_name', 'tie_day_count']

# add columns
empty_dispatch['見車率'] = 1 - (empty_dispatch['empty_minutes'] / (18*60))
bins = [0.0, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0]
labels = [
    '60%以下', '60%~65%', '65%~70%', '70%~75%', '75%~80%',
    '80%~85%', '85%~90%', '90%~95%', '95%以上'
]
empty_dispatch['見車率_bin'] = pd.cut(
    empty_dispatch['見車率'], bins, labels=labels
)

# merge
empty_dispatch = empty_dispatch.merge(
    dis_agg, how='left',
    on=['stop_id', 'weekday_type', 'stop_name']
)
empty_dispatch['is_no_set'] = empty_dispatch['tie_day_count'].isna()

# 不常調度 = 調度卡天數<50%
is_weekend = (empty_dispatch['weekday_type'] == 'weekend')
empty_dispatch['is_seldom_card_dispatch'] = 0
empty_dispatch.loc[is_weekend, 'is_seldom_card_dispatch'] = (
    empty_dispatch.loc[is_weekend, 'card_dis_day_count'] <= 4
)
empty_dispatch.loc[~is_weekend, 'is_seldom_card_dispatch'] = (
    empty_dispatch.loc[~is_weekend, 'card_dis_day_count'] <= 10
)
# empty_dispatch['is_seldom_tie_untie'] = 0
# empty_dispatch.loc[is_weekend, 'is_seldom_tie_untie'] = (
#     empty_dispatch.loc[is_weekend, 'tie_day_count'] <= 5
# )
# empty_dispatch.loc[~is_weekend, 'is_seldom_tie_untie'] = (
#     empty_dispatch.loc[~is_weekend, 'tie_day_count'] <= 10
# )
# 

# save raw
empty_dispatch.to_csv(
    r'D:\iima\ubike分析\DM\202309\張副秘會議\empty_dispatch.csv',
    index=False, encoding='UTF-8'
)

# reshape
output = empty_dispatch.groupby(['見車率_bin', 'weekday_type']).agg({
    'stop_id': 'count',
    'is_seldom_card_dispatch': 'sum',
    'is_no_set': 'sum'
}).reset_index()
output.columns = ['112年7月見車率', '週間週末', '站數', '少調度站數', '未配置站數']
output1 = output.pivot(
    columns='週間週末', index='112年7月見車率', values=['站數', '少調度站數', '未配置站數']
).reset_index()
output1 = output1.T
output1.index = [
    '112年7月見車率', '週間站數', '週末站數',
    '週間少調度站數', '週末少調度站數',
    '週間未配置站數', '週末未配置站數',
]

# save sheet
# output1.to_excel(r'D:\iima\ubike分析\DM\202309\張副秘會議\YouBike7月見車率.xlsx',)
