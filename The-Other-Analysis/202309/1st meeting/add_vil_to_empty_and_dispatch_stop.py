# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 13:44:54 2023

@author: rz3881
"""

import pandas as pd
import geopandas as gpd


def add_geometry(dispatch):
    stop_info = pd.read_excel(root_dir+'/DIM/場站設定_20231114164008.xlsx')
    stop_info['場站編號'] = 'U' + stop_info['場站編號'].astype(str).str.slice(3,)
    stop_info['lng'] = stop_info['座標'].str.split(',').str[1].astype(float)
    stop_info['lat'] = stop_info['座標'].str.split(',').str[0].astype(float)
    stop_info = stop_info[['場站編號', 'lng', 'lat']]
    dispatch = dispatch.merge(stop_info, left_on='stop_id', right_on='場站編號', how='left')
    return dispatch


def add_village(dispatch):
    village = gpd.read_file('D:/iima/opendata/行政界線圖/台北市里界圖/tpe_vil.geojson')
    village = village[['TVNAME', 'geometry']]
    village.rename(columns={'TVNAME': 'village'}, inplace=True)
    dispatch = gpd.sjoin(dispatch, village, how='left', op='intersects')
    return dispatch

# Config
root_dir = 'D:/iima/ubike分析'

# Load data
dispatch = pd.read_csv(root_dir+r'\DM\202309\張副秘會議\見車率與調度天數清單.csv')

# Add geometry
dispatch = add_geometry(dispatch)
dispatch = gpd.GeoDataFrame(
    dispatch, geometry=gpd.points_from_xy(dispatch['lng'], dispatch['lat']), crs='EPSG:4326'
)
dispatch = add_village(dispatch)

# Save data
dispatch = dispatch[[
    'stop_id', 'stop_name', 'weekday_type', '日曆天數', '柱數',
    '日均空車分鐘', '見車率', '見車率_bin',
    '日均滿車分鐘', '見位率',
    '有任意調度卡紀錄的天數', '日均調度車次(補車+拉車)',
    'village', 'lat', 'lng', 'geometry'
]]
dispatch.to_csv(
    root_dir+r'\DM\202309\張副秘會議\見車率與調度天數清單_加上里_test.csv',
    index=False
)