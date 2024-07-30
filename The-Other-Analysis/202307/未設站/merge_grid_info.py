# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 01:30:36 2023

@author: rz3881
"""

import pandas as pd

PATH = r'D:\iima\ubike分析\DM\202307\未設站'

info = pd.read_csv(PATH+'/GridInfo.csv')
suggest = pd.read_csv(PATH+'/FullRecommend.csv')

info.rename(columns={'gridid': 'GridID'}, inplace=True)
suggest.rename(columns={'Num_Space': 'suggest_space'}, inplace=True)
suggest.rename(columns={'Num_Car': 'suggest_bike'}, inplace=True)

info['GridID'] = info['GridID'].astype(str)
suggest['GridID'] = suggest['GridID'].astype(str)

grid_all_info = info.merge(
    suggest[['GridID', 'suggest_bike', 'suggest_space']],
    on='GridID', how='left'
)

grid_all_info.to_csv(PATH+'/grid_all_info.csv', index=False, encoding='UTF-8')
