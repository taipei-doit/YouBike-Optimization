'''
此分析旨在將user的單點行為，轉換成一片區域，藉此交疊出需求(O&D)地圖。
其中可考慮的是需不需要區分星期1~7，或僅分為工作日與假日。
以及若無固定pattern，需要列入需求計算嗎?(因為是隨機行為)
一片區域多大比較適合?
借不到車的地方永遠沒有需求?
排除奇怪的天?
'''

import pandas as pd
import geopandas as gpd
from sklearn.cluster import AgglomerativeClustering
from shapely.geometry import Point, Polygon
import time

import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import load_txn_data


def cluster_points(traning_data):
    '''Ward hierarchical clustering on twd97 coordinates'''
    ward = AgglomerativeClustering(
        n_clusters=None, linkage='ward', distance_threshold=MIN_DISTANCE_THRESHOULD
    )
    ward = ward.fit(traning_data)
    return ward.labels_


def calculate_probability(weekday, unit_data):
    '''Calculate the repeat probability of the OD in same weekday.'''
    unique_days_in_this_weekday = len(set(unit_data['date']))
    if unique_days_in_this_weekday == 0:
        print(unit_data)
        raise RuntimeError(f'Probability is zero in {weekday}, check data above.')
    elif unique_days_in_this_weekday <= RANDOM_BEHAVIOR_THRESHOLD:
        return 0

    total_days_in_this_weekday = DAYS_IN_WEEKDAY[weekday]
    if unique_days_in_this_weekday > total_days_in_this_weekday:
        print(unit_data)
        raise RuntimeError(f'Probability is greater than 1 in {weekday}, check data above.')

    if total_days_in_this_weekday - unique_days_in_this_weekday <= SAME_WEEKDAY_TOLERANCE_COUNT:
        unique_days_in_this_weekday = total_days_in_this_weekday
    return unique_days_in_this_weekday / total_days_in_this_weekday


def _calculate_midpoint(point1, point2):
    x_mid = (point1.x + point2.x) / 2
    y_mid = (point1.y + point2.y) / 2
    return Point(x_mid, y_mid)


def calculate_polygon(weekday, unit_data):
    '''
    Return a polygon that cover from points in the unit_data.
    if there is only one unique point, return a circle with radius MIN_DISTANCE_THRESHOULD/2
    if there are two unique points, return a circle in the middle of two points with radius MIN_DISTANCE_THRESHOULD/2
    if there are more than two unique points, return a polygon that cover all points
    '''
    unique_point = gpd.GeoSeries(unit_data['on_point'].drop_duplicates())
    point_count = len(unique_point)
    if point_count == 1:
        poly = unique_point.buffer(MIN_DISTANCE_THRESHOULD/2, resolution=2).iloc[0]
    elif point_count == 2:
        center_point = _calculate_midpoint(unique_point.iloc[0], unique_point.iloc[1])
        poly = center_point.buffer(MIN_DISTANCE_THRESHOULD/2, resolution=2)
    elif point_count > MAXIMUM_POINT_COUNT_IN_A_WEEKDAY:
        print(unit_data)
        raise RuntimeError(f'Too much points in {weekday}, check data above.')
    else:
        poly = Polygon([p for p in unique_point])
        if not poly.is_valid:
            print(unique_point)
            print('Above points are invalid.')
    return poly


def generate_key_column(data):
    '''Generate a key column that is the concatenation of KEY_COLUMNS.'''
    key = None
    for kcol in KEY_COLUMNS:
        if key is None:
            key = data[kcol].astype(str)
        else:
            key += data[kcol].astype(str)
    return key


# Constants
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'
MIN_TXN_MINUTE = 2
NTU_REGION = ['ZB1', 'ZB2', 'ZB3']
HOUR_INTERVAL = [
    (0, 5.5), (6, 7.5),  # 凌晨~早上
    (8, 9.5), (10, 11.5),  # 上午課
    (12, 12.5),  # 午休
    (13, 15), (15.5, 17),  # 下午課
    (17.5, 19.5), (20, 23.5)  # 晚上
]
USE_SAMPLE_USER = False
DAYS_IN_WEEKDAY = {1: 4, 2: 4, 3: 4, 4: 5, 5: 5, 6: 5, 7: 5}
MIN_DISTANCE_THRESHOULD = 100
MAXIMUM_POINT_COUNT_IN_A_WEEKDAY = 5
# 考慮到使用者有可能借不到車，就算有需求也不會每天都有一樣的OD
# 因此設定，如本月星期一共5天，有5-{SAME_WEEKDAY_TOLERANCE_COUNT}個星期一都有該OD
# 就視發生機率為1
SAME_WEEKDAY_TOLERANCE_COUNT = 1
# 同個星期的同一區間，若少於或等於{RANDOM_BEHAVIOR_THRESHOLD}，則視為隨機行為，機率調整為0
RANDOM_BEHAVIOR_THRESHOLD = 0
KEY_COLUMNS = ['card_id', 'weekday', 'on_hour_interval', 'label']

# Load
# dispatch region
region = pd.read_csv(f'{ROOT_PATH}/DIM/dispatch_region_{YM}.csv')
# transaction
raw_txn = load_txn_data(
    f'{ROOT_PATH}/DM/202403/prepared_data/txn/txn_only_ubike.csv',
    is_add_hour=True,
    is_add_date=True,
    is_add_weekday=True,
    is_filter_invalid_stop=True,
    is_only_taipei=False,
    is_only_user_stop=True,
    date_col='on_time'
)

# Transform
# cleansing txn
txn = raw_txn.loc[~raw_txn['card_id'].isna()]
# 少於2分鐘視為異常
txn['time_diff_min'] = (txn['off_time'] - txn['on_time']).dt.total_seconds() / 60
txn = txn.loc[txn['time_diff_min'] > MIN_TXN_MINUTE]  # <=2共8萬筆，<=3共32萬筆
txn = txn[[
    'card_id',
    'on_stop_id', 'on_stop', 'on_lng', 'on_lat',
    'off_stop_id', 'off_stop', 'off_lng', 'off_lat',
    'on_time', 'on_hour', 'off_time', 'off_hour',
    'date', 'weekday', 'weekday_type'
]]
# 精細化時間
is_second_half_hour = txn['on_time'].dt.minute > 30
txn.loc[is_second_half_hour, 'on_hour'] = txn['on_hour'] + 0.5
is_second_half_hour = txn['off_time'].dt.minute > 30
txn.loc[is_second_half_hour, 'off_hour'] = txn['off_hour'] + 0.5
# mark hour interval
txn['on_hour_interval'] = None
txn['off_hour_interval'] = None
for start, end in HOUR_INTERVAL:
    hour_label = f'{start}-{end}'
    is_in_interval = txn['on_hour'].between(start, end, inclusive='both')
    txn.loc[is_in_interval, 'on_hour_interval'] = hour_label
    txn.loc[is_in_interval, 'off_hour_interval'] = hour_label
# convert geometry projection
on_points = gpd.points_from_xy(txn['on_lng'], txn['on_lat'], crs='EPSG:4326')
txn['on_point'] = on_points.to_crs(epsg='3826')
txn['on_twd97_lng'] = txn['on_point'].apply(lambda x: None if x.is_empty else x.x)
txn['on_twd97_lat'] = txn['on_point'].apply(lambda x: None if x.is_empty else x.y)
off_points = gpd.points_from_xy(txn['off_lng'], txn['off_lat'], crs='EPSG:4326')
txn['off_point'] = off_points.to_crs(epsg='3826')
txn['off_twd97_lng'] = txn['off_point'].apply(lambda x: None if x.is_empty else x.x)
txn['off_twd97_lat'] = txn['off_point'].apply(lambda x: None if x.is_empty else x.y)
# keep NTU stop
target_stop_id = region.loc[region['region'].isin(NTU_REGION), 'stop_id']

# 曾在NTU區域租借的user
rent_in_ntu_region = txn.loc[txn['on_stop_id'].isin(target_stop_id)]
rent_in_ntu_region = rent_in_ntu_region[[
    'card_id',
    'weekday', 'date', 'on_time', 'on_hour', 'on_hour_interval',
    'on_stop_id', 'on_stop',
    'on_lng', 'on_lat', 'off_lng', 'off_lat',
    'on_twd97_lng', 'on_twd97_lat', 'on_point'
]]
if USE_SAMPLE_USER:
    # filter user
    top10_active_user_in_ntu = rent_in_ntu_region['card_id'].value_counts().index[0:10000]
    model_user_txn = txn.loc[txn['card_id'].isin(top10_active_user_in_ntu)]
else:
    model_user_txn = rent_in_ntu_region

# 為了要辨識使用者行為是否有pattern，並且容忍行為的非精確性
# 比如甲星期一有早8課，借車時間可能分佈於7:00~7:40，可能也不會每天都借(或借不到)
# 理想上可根據甲的每筆交易，建立time window，匡列每筆交易是否在此window，但計算量不切實際
# 因此將時間切分為固定區間，並對該區間的點位進行DBSCAN分群
# 假設同一個星期的任一最小時間區間的任一群(weekday, hour_interval, cluster)，不應有超過一次的OD行為
# 也就是假設甲在每個星期一的{HOUR_INTERVAL[0]}的某一區域，應該只有一次OD(只有一次移動)
# 反過來說，當甲若在{HOUR_INTERVAL[0]}某區域內中多次移動，此方法會低估其需求
start_time = time.time()
total_loop = len(model_user_txn.groupby(['card_id', 'weekday', 'on_hour_interval']))
loop_count = 1

raw = []
results = {
    'card_id': [], 'weekday': [], 'on_hour_interval': [], 'label': [],
    'probability': [], 'polygon': []
}
for (cid, wk, ohi), group_data in model_user_txn.groupby(['card_id', 'weekday', 'on_hour_interval']):
    if group_data.shape[0] == 1:
        group_data['label'] = 0
    else:
        group_data['label'] = cluster_points(group_data[['on_twd97_lat', 'on_twd97_lat']])

    for label, ldata in group_data.groupby('label'):
        probability = calculate_probability(wk, ldata)
        if probability == 0:
            polygon = Polygon()
        else:
            polygon = calculate_polygon(wk, ldata)

        # save result
        raw.append(group_data)
        results['card_id'].append(cid)
        results['weekday'].append(wk)
        results['on_hour_interval'].append(ohi)
        results['label'].append(label)
        results['probability'].append(probability)
        results['polygon'].append(polygon)
        
        # print progress
        if loop_count % 5000 == 0:
            print(f'{loop_count}/{total_loop}, cost {time.time()-start_time:.2f} sec')
        loop_count += 1
print(f'Cost {time.time()-start_time:.2f} sec')


# output to Power BI
raw_data = pd.concat(raw)
raw_data['key'] = generate_key_column(raw_data)
raw_data.to_csv(f'{ROOT_PATH}/DM/{YM}/7th_meeting/probability_cloud/raw_user_txn.csv', index=False)

result_df = pd.DataFrame(results)
result_df['key'] = generate_key_column(result_df)
result_df = gpd.GeoDataFrame(result_df, geometry='polygon', crs='EPSG:3826')
result_df = result_df.to_crs(epsg='4326')
result_df.to_csv(f'{ROOT_PATH}/DM/{YM}/7th_meeting/probability_cloud/rent_probability_cloud.csv', index=False)

key_table = result_df.groupby(['card_id', 'weekday', 'on_hour_interval', 'label']).size()
key_table = key_table.reset_index()
key_table['key'] = generate_key_column(key_table)
key_table.rename(columns={0: 'count'}, inplace=True)
key_table.to_csv(f'{ROOT_PATH}/DM/{YM}/7th_meeting/probability_cloud/key_table.csv', index=False)
