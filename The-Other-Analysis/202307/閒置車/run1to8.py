# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 17:41:45 2023

@author: rz3881
"""

# config
ym = '202307'
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'
dispatch_path = root_path+f'/DM/{ym}/prepared_data/dispatch'
status_path = root_path+f'/DM/{ym}/prepared_data/status'
idle_path = root_path+f'/DM/{ym}/閒置車'
init_hour = 6  # 一天初始時間(6 = 06:00)
afternoon_hour = 16
min_delay_secs = 4  # 交易發生後，站點最快也要4秒才會回傳紀錄
is_add_txn_when_empty_or_full = False  # 是否啟用 觸底或觸頂需求加1
reserve_number = 2  # 保留預警閾值
dispatch_capacity = 14  # 每次調度的最大車數
date_last_month = '2023-06-30'
exclude_date = ['2023-06-30']
warning_threshould = 0  # empty = (在站車 <= warning_threshould)
target_percentile = 1
dispatch_reserve_number = 2  # 調度車數與站點柱數至少要有{dispatch_reserve_number}個

# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[01]combine_txn_and_status.py", encoding='UTF-8').read())
# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[02]simulate_to_find_best_init_bikes.py", encoding='UTF-8').read())
# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[03]simulate_to_find_best_dispatch.py", encoding='UTF-8').read())
# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[04]extract_df_from_simulation.py", encoding='UTF-8').read())
# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[05]agg_by_date_by_stop.py", encoding='UTF-8').read())
# exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[06]建立站點profile.py", encoding='UTF-8').read())
exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[07]產出結論p1.py", encoding='UTF-8').read())
exec(open(f'D:\\iima/ubike分析/CODE/{ym}/閒置車'+"/[08]產出結論p2.py", encoding='UTF-8').read())

