''' pseduocode
給定擋柱綁篩選標準
    絕對數量、持續時間
篩選需要被檢討的擋住綁站

給定可配合站點標準
    距離、缺車、交易潮汐互補
挑選可配合站點
'''

import sys
import pandas as pd
import geopandas as gpd
from scipy.spatial import distance_matrix
import pickle
sys.path.append('D:/iima/ubike分析/CODE')
from udf_function import filter_invalid_stop_id


def clean_data(data, START_HOUR, END_HOUR, is_filter_hour=True, is_filter_stop_id=True):
    print('original data shape:', data.shape)
    
    # keep data after start_hour
    if is_filter_hour:
        data = data.loc[(data['hour']>=START_HOUR) & (data['hour']<=END_HOUR)]
        
    # drop invalid stop_id
    if is_filter_stop_id:
        data = filter_invalid_stop_id(data)
    
    print('cleaned data shape:', data.shape)
    return data


def add_twd97_geometry(station_data):
    station_data = gpd.GeoDataFrame(
        station_data,
        geometry=gpd.points_from_xy(station_data['lng'], station_data['lat']),
        crs='EPSG:4326'
    )
    station_data = station_data.to_crs('EPSG:3826')
    station_data['lng_twd97'] = station_data['geometry'].x
    station_data['lat_twd97'] = station_data['geometry'].y
    return station_data


def calculate_curve_slope(data, window_size=3):
    """
    Calculate the curve slope of the cumulative net profit for each stop and weekday type.
    The slope is calculated by the difference between the moving average of cumulative net profit.
    The moving average is calculated by the rolling `window_size` of the cumulative net profit.
    MA(t0) = (cumulative_net_profit(t0) + cumulative_net_profit(t-1) + cumulative_net_profit(t-`window_size`-1) + ...) / `window_size`

    Parameters:
    - data: DataFrame containing the data for calculation.
    - window_size: Size of the rolling window for calculating the moving average of cumulative net profit. Default is 3.

    Returns:
    - DataFrame: The input data with additional columns for moving average and slope of cumulative net profit.
    """
    results = []
    for _, gdata in data.groupby(['stop_id', 'weekday_type']):
        gdata['ma_cum_net_profit'] = gdata['cum_net_profit'].rolling(window_size).mean()
        gdata['slope_of_cum_net_profit'] = gdata['ma_cum_net_profit'].diff()
        results.append(gdata)
    return pd.concat(results)


def compute_distance_matrix(station_data):
    # compute distance matrix
    distance_ndarray = distance_matrix(
        station_data[['lng_twd97', 'lat_twd97']], station_data[['lng_twd97', 'lat_twd97']]
    )
    # add index to distance matrix
    distance_ndarray = pd.DataFrame(distance_ndarray)
    distance_ndarray.index = station_data['stop_id']
    distance_ndarray.columns = station_data['stop_id']
    return distance_ndarray


def export_distance_matrix(distance_ndarray, station_data, OUTPUT_PATH):
    '''
    Export distance dataframe to an Excel.
    And add multi-level index and multi-level columns.

    Parameters:
    - station_distance_martix (pd.DataFrame): Distance matrix between stations.
    - station (pd.DataFrame): Station data containing stop_id and stop_name.
    - OUTPUT_PATH (str): Path to save the distance dataframe Excel file.

    Returns:
    - None
    '''
    # station with raw stop_id
    temp_station = station_data.copy()
    temp_station['stop_id'] = temp_station['stop_id'].str.replace('U', '500')

    # Add multiindex
    distance_df = distance_ndarray.reset_index().copy()
    distance_df['stop_id'] = temp_station['stop_id']
    distance_df['stop_name'] = temp_station['stop_name']

    # Check if the index order is the same as station
    check_result = distance_ndarray.index.str.replace('U', '500') == distance_df['stop_id']
    print('Check if the index order is the same as station:', all(check_result))

    # Set index
    distance_df = distance_df.set_index(['stop_id', 'stop_name'])

    # Add multi-level columns
    columns = [(dist, stop_id, stop_name) for dist, stop_id, stop_name in temp_station[['dist', 'stop_id', 'stop_name']].values]
    distance_df.columns = pd.MultiIndex.from_tuples(columns)

    # Save to Excel
    distance_df.to_excel(f'{OUTPUT_PATH}/distance_df.xlsx')


def keep_tod_need_review_stops(tod_by_wt_data, ava_by_wt_data, DOCK_MORE_THAN, FULL_MINUTE_MORE_THAN):
    """
    Keep stops have tie on dock and need to be reviewed.
    Review condition:
    - (Number of tie on dock > DOCK_MORE_THAN) time consecutive positive > HOUR_DURATION_LONGER_THAN

    Args:
        tod_by_wt (pd.DataFrame): DataFrame containing tie on dock data.
        DOCK_MORE_THAN (int): Threshold for number of tie on dock occurrences.
        HOUR_DURATION_LONGER_THAN (int): Threshold for consecutive hours of tie on dock occurrences.

    Returns:
        pd.DataFrame: Filtered DataFrame containing complementary stops.

    """
    # Mark data that (tod dock too much) and (consecutive positive too long)
    # tod dock too much
    tod_by_wt_data['is_too_many_tod'] = tod_by_wt_data['unavailable'] > DOCK_MORE_THAN

    # Mark data that usually full when tod duration
    ava_by_wt_data['is_usually_full'] = ava_by_wt_data['full_minute'] > FULL_MINUTE_MORE_THAN
    tod_by_wt_data = tod_by_wt_data.merge(
        ava_by_wt_data[['stop_id', 'weekday_type', 'hour', 'full_minute', 'is_usually_full']],
        how='left', on=['stop_id', 'weekday_type', 'hour']
    )
    tod_by_wt_data['is_full_when_tod'] = (
        tod_by_wt_data['is_usually_full'].fillna(False) & tod_by_wt_data['is_too_many_tod']
    )

    # Unique stop_id and weekday_type that need to be reviewed
    index_tod_by_wt = tod_by_wt_data.groupby(['stop_id', 'weekday_type']).agg(
        stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
        full_when_tod_label=pd.NamedAgg(column='is_full_when_tod', aggfunc='max')
    ).reset_index()

    # Filter
    reviewd_tod_stop = index_tod_by_wt.loc[(index_tod_by_wt['full_when_tod_label'])]

    return reviewd_tod_stop, tod_by_wt_data


def find_optimize_hour(merge_tod_by_wt_data, target_stop_id, target_weekday_type, DELAY_HOUR_AFTER_DURATION):
    '''
    Find optimize hour for tie on dock duration
    tod_hour is hours that tie on dock can move to other stop, it is at least 1 hour before target need bike.
    pay_back_hour is hour that other stop should return the bike to target stop.
    So pay_back_hour can be at least tod_hour + 1, that is the hour complementary
    '''
    # locate hours that tie on dock should optimize
    is_target_stop = merge_tod_by_wt_data['stop_id'] == target_stop_id
    is_target_weekday_type = merge_tod_by_wt_data['weekday_type'] == target_weekday_type
    target_merge_tod_by_wt = merge_tod_by_wt_data.loc[is_target_stop & is_target_weekday_type]
    target_tod_hour = set()

    # usually full
    is_full_when_tod = target_merge_tod_by_wt['is_full_when_tod']
    if is_full_when_tod.sum() > 0:
        usually_full = target_merge_tod_by_wt.loc[is_full_when_tod, 'hour']
        target_tod_hour = target_tod_hour | set(usually_full.tolist())

    # hour to pay back bike
    end_of_target_tod_hour = max(target_tod_hour) + 1
    target_pay_back_hour = end_of_target_tod_hour + DELAY_HOUR_AFTER_DURATION
    # make sure pay back hour is not bigger than end hour
    target_pay_back_hour = min(target_pay_back_hour, END_HOUR)
    return target_tod_hour, target_pay_back_hour


def find_ava_complementary_stop(ava_by_wt_data, target_near_station_id, target_weekday_type, target_tod_hour, EMPTY_MINUTE_MORE_THAN, is_pivot=True):
    """
    Find empty complementary stops during the target stop's tie-on-dock duration.
    Which means the station need bike when the target station tie on dock too much.
    
    Args:
        ava_by_wt (pd.DataFrame): Availability data by weekday type and hour.
        near_station (pd.Index): Stations near the target stop.
        target_weekday_type (str): Target weekday type.
        tod_hour (set): Set of hours to optimize tie-on-dock duration.
        EMPTY_MINUTE_MORE_THAN (int): Minimum number of empty minutes.

    Returns:
        pd.DataFrame: Complementary stops with empty occurrences during tie-on-dock duration.
    """
    # keep near station and target weekday type
    ava_near = ava_by_wt_data.loc[ava_by_wt_data['stop_id'].isin(target_near_station_id)]
    ava_near = ava_near.loc[ava_near['weekday_type'] == target_weekday_type]
    # find out empty hour complementary station
    is_empty = ava_near['empty_minute'] > EMPTY_MINUTE_MORE_THAN
    is_tod_hour = ava_near['hour'].isin(target_tod_hour)
    matched_ava_data = ava_near.loc[is_empty & is_tod_hour]
    match_ava_stop_id = set(matched_ava_data['stop_id'])
    complementary_ava_near = ava_near.loc[ava_near['stop_id'].isin(match_ava_stop_id)]

    # output complementary empty data
    if complementary_ava_near.empty:
        return match_ava_stop_id, complementary_ava_near
    else:
        complementary_ava_near['empty_minute'] = complementary_ava_near['empty_minute'].round(1)
        if is_pivot:
            pivot = complementary_ava_near.pivot(
                index=['weekday_type', 'dist', 'stop_id', 'stop_name'],
                columns='hour',
                values='empty_minute'
            ).reset_index()
            pivot.insert(4, 'type', 'empty_minute')
            pivot['sum_empty_minute'] = pivot.iloc[:, 5:].sum(axis=1)
            pivot.sort_values(by='sum_empty_minute', ascending=False, inplace=True)
            return match_ava_stop_id, pivot
        else:
            return match_ava_stop_id, complementary_ava_near


def find_txn_complementary_stop(txn_by_wt_data, target_near_station_id, target_weekday_type, target_pay_back_hour, END_HOUR, is_pivot=True):
    """
    Find transaction tidal complementary stops for tie-on-dock duration.
    Which means the station can provide bike when the target stop need bike.

    Args:
        txn_by_wt (pd.DataFrame): Transaction data by weekday type and hour.
        near_station (pd.Index): Stations near the target stop.
        target_weekday_type (str): Target weekday type.
        optimize_hour (set): Set of hours to optimize tie-on-dock duration.
        DELAY_HOUR_AFTER_DURATION (int): Delay in hours after tie-on-dock duration.

    Returns:
        dict: Dictionary containing complementary stops categorized as 'both' and 'txn'.
    """
    # keep near station and target weekday type
    txn_near = txn_by_wt_data.loc[txn_by_wt_data['stop_id'].isin(target_near_station_id)]
    txn_near = txn_near.loc[txn_near['weekday_type'] == target_weekday_type]
    
    is_no_need_to_pay_back = target_pay_back_hour >= END_HOUR
    if is_no_need_to_pay_back:
        # no need to find txn complementary stop
        match_txn_stop_id = set(target_near_station_id)
        complementary_txn_near = txn_near
    else:
        # current hour complementary
        current_hour_txn_near = txn_near.loc[txn_near['hour'] == target_pay_back_hour]
        is_positive = current_hour_txn_near['cum_net_profit'] > 1
        is_increasing = current_hour_txn_near['slope_of_cum_net_profit'] > 1
        current_positive_and_increasing_stop_id = set(
            current_hour_txn_near.loc[is_positive & is_increasing, 'stop_id']
        )
        # make sure txn tidal is the same direction to the end
        after_hour_txn_near = txn_near.loc[txn_near['hour'] >= target_pay_back_hour]
        is_increasing_to_end = after_hour_txn_near.groupby(['stop_id'])['slope_of_cum_net_profit'].min() > 0
        increasing_to_end_stop_id = set(is_increasing_to_end.loc[is_increasing_to_end].index)
        # find out txn complementary data
        match_txn_stop_id = current_positive_and_increasing_stop_id & increasing_to_end_stop_id
        complementary_txn_near = txn_near.loc[txn_near['stop_id'].isin(match_txn_stop_id)]

    # output txn complementary data
    if complementary_txn_near.empty:
        return match_txn_stop_id, complementary_txn_near
    else:
        complementary_txn_near['cum_net_profit'] = complementary_txn_near['cum_net_profit'].round(1)
        if is_pivot:
            pivot = complementary_txn_near.pivot(
                index=['weekday_type', 'stop_id'],
                columns='hour',
                values='cum_net_profit'
            ).reset_index()
            pivot.insert(2, 'type', 'cum_net_profit')
            pivot.sort_values(by=pay_back_hour, ascending=False, inplace=True)
            return match_txn_stop_id, pivot
        else:
            return match_txn_stop_id, complementary_txn_near


def find_both_complementary_stops(complementary_ava_stop_id, complementary_txn_stop_id, complementary_ava_data, complementary_txn_data):
        """
        Find stations that fit both complementary conditions.
        """
        both_complementary_stop_id = complementary_ava_stop_id & complementary_txn_stop_id
        # txn
        both_complementary_txn_data = complementary_txn_data.loc[
            complementary_txn_data['stop_id'].isin(both_complementary_stop_id)
        ]
        # ava
        both_complementary_ava_data = complementary_ava_data.loc[
            complementary_ava_data['stop_id'].isin(both_complementary_stop_id)
        ]
        # both
        both_complementary_data = pd.concat([both_complementary_ava_data, both_complementary_txn_data])
        both_complementary_data = both_complementary_data.reset_index(drop=True)
        
        return both_complementary_data, both_complementary_ava_data, both_complementary_txn_data


def add_hour_info_to_targe(target_tod_by_wt_pivot_data, complementary_pair_data):
    tod_hour = []
    pay_back_hour = []
    for _, row in target_tod_by_wt_pivot_data.iterrows():
        target_stop_id = row['stop_id']
        target_weekday_type = row['weekday_type']
        tod_hour.append(
            complementary_pair_data[target_weekday_type][target_stop_id]['hour']['tod_hour']
        )
        pay_back_hour.append(
            complementary_pair_data[target_weekday_type][target_stop_id]['hour']['pay_back_hour']
        )
    return target_tod_by_wt_pivot_data


def extract_ava_df(complementary_pair, condition):
    complementary_pair_data = complementary_pair.copy()
    if condition == 'both':
        subset_key = 'both_ava'
    else:
        subset_key = 'ava'
    
    complementary_ava_df = []
    for weekday_type, temp_stop_subset in complementary_pair_data.items():
        for target_stop_id, temp_subset in temp_stop_subset.items():
            temp_ava_df = temp_subset[subset_key]
            target_tod_hour = str(temp_subset['hour']['tod_hour'])
            temp_ava_df.insert(0, 'tod_hour', target_tod_hour)
            temp_ava_df.insert(0, 'target_stop_id', target_stop_id)
            complementary_ava_df.append(temp_ava_df)
    complementary_ava_df = pd.concat(complementary_ava_df)
    return complementary_ava_df


def extract_txn_df(complementary_pair, condition):
    complementary_pair_data = complementary_pair.copy()
    if condition == 'both':
        subset_key = 'both_txn'
    else:
        subset_key = 'txn'
    
    complementary_txn_df = []
    for weekday_type, temp_stop_subset in complementary_pair_data.items():
        for target_stop_id, temp_subset in temp_stop_subset.items():
            temp_txn_df = temp_subset[subset_key]
            temp_txn_df.insert(0, 'pay_back_hour', temp_subset['hour']['pay_back_hour'])
            temp_txn_df.insert(0, 'target_stop_id', target_stop_id)
            complementary_txn_df.append(temp_txn_df)
    complementary_txn_df = pd.concat(complementary_txn_df)
    return complementary_txn_df


def generate_target_ava_txn_pair_geometry(complementary_pair, station_data, condition):
    complementary_pair_data = complementary_pair.copy()
    if condition == 'both':
        subset_key1 = 'both_ava'
        subset_key2 = 'both_txn'
    else:
        subset_key1 = 'ava'
        subset_key2 = 'txn'

    # concat data
    complementary_df = []
    for weekday_type, temp_stop_subset in complementary_pair_data.items():
        for target_stop_id, temp_subset in temp_stop_subset.items():
            # target
            temp_target_df = temp_subset['target'][['stop_id', 'weekday_type']]
            temp_target_df['type'] = 'target'
            temp_target_df['sum_empty_minute'] = -100
            # ava
            temp_ava_df = temp_subset[subset_key1]
            if temp_ava_df.empty:  # no station is empty when target tie on dock
                print(target_stop_id, 'ava empty')
                temp_ava_df = temp_subset[subset_key2][['stop_id', 'weekday_type']]
                temp_ava_df['sum_empty_minute'] = 0
            else:
                temp_ava_df = temp_ava_df[['stop_id', 'weekday_type', 'sum_empty_minute']]
            temp_ava_df['type'] = 'ava'
            # txn
            temp_txn_df = temp_subset[subset_key2][['stop_id', 'weekday_type']]
            temp_txn_df['type'] = 'txn'
            temp_txn_df['sum_empty_minute'] = 0
            # reshape
            temp_df = pd.concat([temp_target_df, temp_ava_df, temp_txn_df])
            temp_df.insert(0, 'target_stop_id', target_stop_id)
            # distance
            distance_df = temp_subset['near'].reset_index()
            distance_df.rename(columns={target_stop_id: 'distance'}, inplace=True)
            temp_df = temp_df.merge(distance_df, how='left', left_on='stop_id', right_on='stop_id')
            # save
            complementary_df.append(temp_df)
    complementary_df = pd.concat(complementary_df)

    # add geometry
    complementary_df = complementary_df.merge(
        station_data[['stop_id', 'stop_name', 'lng', 'lat']], how='left', on='stop_id'
    )
    
    complementary_df = complementary_df[[
        'weekday_type',
        'target_stop_id', 'stop_id', 'stop_name', 'distance',
        'type', 'sum_empty_minute',
        'lng', 'lat'
    ]]
    return complementary_df

# Set Config
pd.options.mode.chained_assignment = None
YM = '202311'
ROOT_PATH = r'D:\iima\ubike分析'
PREAPRED_PATH = f'{ROOT_PATH}/DM/{YM}/prepared_data'
OUTPUT_PATH = f'{ROOT_PATH}/DM/{YM}/擋柱綁'
START_HOUR = 9  # 09:00
END_HOUR = 15
# Condition for tie on dock
DOCK_MORE_THAN = 1
FULL_MINUTE_MORE_THAN = 0
# Condition for complementary stop
METER_SHORTER_THAN = 1000
METER_LONGER_THAN = 0
HOURLY_EMPTY_MINUTE_MORE_THAN = 9
DELAY_HOUR_AFTER_DURATION = 0


# Load data (wt = weekday_type, tod = tie_on_dock)
station = pd.read_csv(f'{ROOT_PATH}/DIM/ubike_stops_from_api_202311.csv')
ava_by_wt = pd.read_csv(f'{PREAPRED_PATH}/availability/availability_agg_by_weekday_type_by_hour_by_stop.csv')
txn_by_wt = pd.read_csv(f'{PREAPRED_PATH}/txn/aggregate_by_weekdaytype_by_hour.csv')
tod_by_wt = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/擋柱綁/unavailable_agg_by_weekday_type_by_hour_by_stop.csv')


# Preprocess data
ava_by_wt = clean_data(ava_by_wt, START_HOUR, END_HOUR)
ava_by_wt['full_minute'] = ava_by_wt['full_minute'].fillna(0)
txn_by_wt = clean_data(txn_by_wt, START_HOUR, END_HOUR)
txn_by_wt['cum_net_profit'] = txn_by_wt.groupby(['stop_id', 'weekday_type'])['net_profit'].cumsum()
txn_by_wt = calculate_curve_slope(txn_by_wt)
tod_by_wt = clean_data(tod_by_wt, START_HOUR, END_HOUR)
station = add_twd97_geometry(station)


# Find stations that tie on dock toot long or too much
target_tod_stop, merge_tod_by_wt = keep_tod_need_review_stops(
    tod_by_wt, ava_by_wt, DOCK_MORE_THAN, FULL_MINUTE_MORE_THAN
)
# save to review
target_tod_by_wt = target_tod_stop.merge(
    merge_tod_by_wt, how='left', on=['stop_id', 'weekday_type', 'stop_name']
)
target_tod_by_wt['unavailable'] = target_tod_by_wt['unavailable'].round(1)
# full minute

target_tod_by_wt.to_csv(f'{OUTPUT_PATH}/target_tod_by_wt.csv', index=False)

# pivot tie on dock data
target_tod_by_wt_pivot = target_tod_by_wt.pivot(
    index=['weekday_type', 'stop_id', 'stop_name'],
    columns='hour',
    values='unavailable'
).reset_index()


# Compute distance matrix between stations
station_distance_martix = compute_distance_matrix(station)
# export_distance_matrix(station_distance_martix, station, OUTPUT_PATH)

# target_tod_by_wt_pivot.loc[target_tod_by_wt_pivot['stop_id'] == 'U101160']
# Filter complementary stop
complementary_pair = {'weekday': {}, 'weekend': {}}
for _, taget in target_tod_by_wt_pivot.iterrows():
    # break
    subset_data = {}
    stop_id = taget['stop_id']
    weekday_type = taget['weekday_type']

    # keep station that is near target
    dis_between_station = station_distance_martix[stop_id]
    is_proper_distance = dis_between_station.between(
        METER_LONGER_THAN, METER_SHORTER_THAN, inclusive='neither'
    )
    near_station = dis_between_station.loc[is_proper_distance]
    near_station_id = set(near_station.index)
    
    # find hours that tie on dock and need to pay back
    tod_hour, pay_back_hour = find_optimize_hour(
        merge_tod_by_wt, stop_id, weekday_type, DELAY_HOUR_AFTER_DURATION
    )

    # find near stations that is empty when target tie on dock
    compl_ava_stop_id, compl_ava_data = find_ava_complementary_stop(
        ava_by_wt, near_station_id, weekday_type, tod_hour, HOURLY_EMPTY_MINUTE_MORE_THAN
    )

    # find near stations that can provide bike when target need bike to pay back
    compl_txn_stop_id, compl_txn_data = find_txn_complementary_stop(
        txn_by_wt, near_station_id, weekday_type, pay_back_hour, END_HOUR
    )

    # find stations that can fit both conditions above
    both_compl_data, both_compl_ava_data, both_compl_txn_data = find_both_complementary_stops(
        compl_ava_stop_id, compl_txn_stop_id, compl_ava_data, compl_txn_data
    )
    
    # save
    subset_data['target'] = pd.DataFrame([taget])
    subset_data['hour'] = {'tod_hour': tod_hour, 'pay_back_hour': pay_back_hour}
    subset_data['near'] = near_station
    subset_data['ava'] = compl_ava_data
    subset_data['both_ava'] = both_compl_ava_data
    subset_data['txn'] = compl_txn_data
    subset_data['both_txn'] = both_compl_txn_data
    subset_data['both'] = both_compl_data
    complementary_pair[weekday_type][stop_id] = subset_data


# Save result as pickle
with open(f'{OUTPUT_PATH}/complementary_pair.pickle', 'wb') as f:
    pickle.dump(complementary_pair, f)


# Save result as csv
# target
target_tod_by_wt_pivot = add_hour_info_to_targe(target_tod_by_wt_pivot, complementary_pair)
target_tod_by_wt_pivot.to_csv(f'{OUTPUT_PATH}/target_tod_by_wt_pivot.csv', index=False)
# complementary
# ava
compl_ava_data = extract_ava_df(complementary_pair, condition='not_both')
compl_ava_data.to_csv(f'{OUTPUT_PATH}/complementary_ava_data.csv', index=False)
both_compl_ava_data = extract_ava_df(complementary_pair, condition='both')
both_compl_ava_data.to_csv(f'{OUTPUT_PATH}/both_complementary_ava_data.csv', index=False)
# txn
compl_txn_data = extract_txn_df(complementary_pair, condition='not_both')
compl_txn_data.to_csv(f'{OUTPUT_PATH}/complementary_txn_data.csv', index=False)
both_compl_txn_data = extract_txn_df(complementary_pair, condition='both')
both_compl_txn_data.to_csv(f'{OUTPUT_PATH}/both_complementary_txn_data.csv', index=False)
# target ava txn pair
result_pair_geometry = generate_target_ava_txn_pair_geometry(
    complementary_pair, station, condition='not_both'
)
result_pair_geometry.to_csv(f'{OUTPUT_PATH}/result_pair_geometry.csv', index=False)