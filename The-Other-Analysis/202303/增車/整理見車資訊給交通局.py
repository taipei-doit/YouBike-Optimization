# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 17:04:05 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
addbike_path = root_path+r'\DM\202303\增車'

# load
whole_agg = pd.read_excel(addbike_path+'/[summary]empty_whole_view_agg_by_stopid.xlsx')
output_whole = pd.read_excel(addbike_path+'/[summary]bike_empty_whole_view.xlsx')

# pivot hour
arp_hourly = output_whole.pivot_table(index='ID', columns='時間', values='見車率').reset_index()

# merge
result = whole_agg.merge(arp_hourly, how='inner', on='ID')

# rename
result.columns = ['ID', '站名', '類別', '日均見車率', '車柱數',
                  '總API回傳筆數', '總交易數',
                  '6點見車率', '7點見車率', '8點見車率', '9點見車率',
                  '10點見車率', '11點見車率', '12點見車率', '13點見車率',
                  '14點見車率', '15點見車率', '16點見車率', '17點見車率',
                  '18點見車率', '19點見車率', '20點見車率', '21點見車率',
                  '22點見車率', '23點見車率']
result = result[['ID', '站名', '類別', '車柱數',
                 '總API回傳筆數', '總交易數', '日均見車率',
                 '6點見車率', '7點見車率', '8點見車率', '9點見車率',
                 '10點見車率', '11點見車率', '12點見車率', '13點見車率',
                 '14點見車率', '15點見車率', '16點見車率', '17點見車率',
                 '18點見車率', '19點見車率', '20點見車率', '21點見車率',
                 '22點見車率', '23點見車率']]

# save
file_path = addbike_path+'/[summary]output_available_rent_prob_to_transportation_bureau.xlsx'
result.to_excel(file_path, index=False)
