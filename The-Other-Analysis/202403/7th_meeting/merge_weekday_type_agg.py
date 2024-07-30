'''
為了在PBI中畫出見車率與調度天數的矩陣
merge兩個資料
'''

import pandas as pd

weekday_type_dispatch = pd.read_csv(r'D:\iima\ubike分析\DM\202403\prepared_data\dispatch\dispatch_card_agg_by_stop_by_weekday_type.csv')
weekday_type_txn = pd.read_csv(r'D:\iima\ubike分析\DM\202403\閒置車\compare_agg.csv')

weekday_type_agg = pd.merge(
    weekday_type_dispatch,
    weekday_type_txn,
    left_on=['stop_id', 'weekday_type'],
    right_on=['ID', '週間週末'],
    how='right'
)

weekday_type_agg.to_csv(
    r'D:\iima\ubike分析\DM\202403\7th_meeting\weekday_type_agg.csv', index=False, encoding='utf-8'
)