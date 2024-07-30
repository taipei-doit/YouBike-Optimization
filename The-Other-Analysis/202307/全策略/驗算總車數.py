# -*- coding: utf-8 -*-
"""
Created on Tue Sep 12 10:00:56 2023

@author: rz3881
"""

import pandas as pd

ym = '202307'
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
idle_path = root_path+f'/DM/{ym}/閒置車'
strategy_path = root_path+f'/DM/{ym}/全策略'
n_th = 2
init_hour = 6

# rich setup
rich_setup = pd.read_excel(strategy_path+'/rich_setup.xlsx')
rich_setup.loc[rich_setup['weekday_type']=='weekday', 'morning_total_bike'].sum()
rich_setup.loc[rich_setup['weekday_type']=='weekday', 'afternoon_total_bike'].sum()


# median setup
ref_yms = ['202303', '202304']
valid_ym = '202307'
yms = ref_yms + [valid_ym]

dispatch_clean = pd.read_csv(
    strategy_path+'/strategy_filter_abnormal_add_idle_docker_dispatch.csv')

dispatch_clean['實際上午合計'] = (
    dispatch_clean['實際6點在站車_202307'] 
    + dispatch_clean['實際上午調入_202307'])

dispatch_clean['實際下午合計'] = (
    dispatch_clean['實際16點在站車_202307'] 
    + dispatch_clean['實際下午調入_202307'])

dispatch_clean['模擬上午合計'] = (
    dispatch_clean['建議6點在站車_202307'] 
    + dispatch_clean['模擬上午調入_202307'])

dispatch_clean['模擬下午合計'] = (
    dispatch_clean['建議16點在站車_202307'] 
    + dispatch_clean['模擬下午調入_202307'])
    # - dispatch_clean['模擬夜間淨調度_202307']

dispatch_clean.loc[dispatch_clean['週間週末']=='weekday', '實際上午合計'].sum()
dispatch_clean.loc[dispatch_clean['週間週末']=='weekday', '模擬上午合計'].sum()
dispatch_clean.loc[dispatch_clean['週間週末']=='weekday', '實際下午合計'].sum()
dispatch_clean.loc[dispatch_clean['週間週末']=='weekday', '模擬下午合計'].sum()
