import pandas as pd
import geopandas as gpd

data = pd.read_json(r'D:\iima\ubike分析\DM\202406\park_area\臺北市公園基本資料.json')

is_park = data['pm_type'] == '公園'
park = data.loc[is_park]

park['pm_LandPublicArea'] = pd.to_numeric(park['pm_LandPublicArea'], errors='coerce')

park['pm_LandPublicArea'].plot(kind='hist', bins=100, edgecolor='black')

pr20 = park['pm_LandPublicArea'].quantile(0.20)

is_top_20_percent = park['pm_LandPublicArea'] >= pr20
target_park = park.loc[is_top_20_percent]

target_park.to_excel(r'D:\iima\ubike分析\DM\202406\park_area\排除最小20比例的公園.xlsx', index=False)

geo_target_park =gpd.GeoDataFrame(
    target_park[['SeqNo', 'pm_name', 'pm_Longitude', 'pm_Latitude']],
    geometry=gpd.points_from_xy(target_park.pm_Longitude, target_park.pm_Latitude)
)

