# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 11:52:16 2023

@author: rz3881
"""

import geopandas as gpd
import pandas as pd
import os

input_path = r'D:\iima\ubike分析\DM\202307\未設站'
output_path = r'D:\iima\ubike分析\DM\202307\未設站'
input_file_format = 'json'
output_file_format = 'csv'

files = os.listdir(input_path)

for file in files:
    if file.endswith(input_file_format):
        data =  gpd.read_file(f'{input_path}/{file}', encoding='utf8')
        csv_file = file.replace(input_file_format, output_file_format)
        data.to_csv(f'{output_path}/{csv_file}', index=False)
