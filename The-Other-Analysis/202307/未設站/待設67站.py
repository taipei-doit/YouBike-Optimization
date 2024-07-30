# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 17:43:06 2023

@author: rz3881
"""

import pandas as pd
import geopandas as gpd
from shapely import wkt

# Config
ROOT_PATH = r'D:\iima\ubike分析'
BUFFER_RADIUS = 100  # meter

# Load
not_set_stop = pd.read_csv(ROOT_PATH+'/DM/202307/未設站/grid_all_info.csv')
not_set_stop = not_set_stop.query(
    'category == "wo" and TooHigh == 0 and Forbidden == 0 and Surrounding == 0 and Manual == 0'
)
ready_set_stop = pd.read_csv(ROOT_PATH+'/DW/待建置67站_20231011.csv')

# Define
not_set_stop = gpd.GeoDataFrame(
    not_set_stop,
    geometry=not_set_stop['geometry'].apply(wkt.loads),
    crs=4326
)
ready_set_stop = gpd.GeoDataFrame(
    ready_set_stop,
    geometry=gpd.points_from_xy(ready_set_stop['lng'], ready_set_stop['lat']),
    crs=4326
)

# Intersect 
ready_set_stop1 = ready_set_stop.to_crs(crs=3826)
ready_set_stop1['geometry'] = ready_set_stop1.buffer(BUFFER_RADIUS)
ready_set_stop = ready_set_stop1.to_crs(crs=4326)
intersections = gpd.overlay(ready_set_stop, not_set_stop, how='intersection')
intersections_id = set(intersections['編號'])
ready_set_stop['is_intersection'] = ready_set_stop['編號'].isin(intersections_id)

# Save
ready_set_stop.to_csv(
    ROOT_PATH+'/DM/202307/未設站/待建置67站_20231011.csv',
    index=False,
    encoding="utf8"
)
