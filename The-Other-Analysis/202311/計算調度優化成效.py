import pandas as pd
import numpy as np
import sys
sys.path.append('D:/iima/ubike分析/CODE')
from udf_function import filter_invalid_stop_id


def load_dispacth_testing_target(root_path):
    '''
    Load dispatching testing target stations
    '''
    target = pd.read_csv(f'{root_path}/DM/202309/全策略/優化調度_202309.csv')
    target['ID'] = target['ID'].str.replace('^500', 'U', regex=True)
    target['is_target'] = 1
    target.rename(
        columns={
            'ID': 'stop_id',
            '站名': 'stop_name',
            '柱數': 'capacity',
            '週間週末': 'weekday_type'
        },
        inplace=True
    )
    return target[['stop_id', 'weekday_type', 'is_target']]


def load_availability_data(root_path, ym):
    '''
    Data source is from YB
    '''
    ava = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/availability/availability_by_stop_by_date_by_hour.csv')
    ava = filter_invalid_stop_id(ava)
    ava['hour'] = ava['hour'].astype(int)
    time = pd.to_datetime(ava['date'])
    ava['weekday'] = time.dt.weekday + 1
    ava['weekday_type'] = np.where(ava['weekday'] <= 5, 'weekday', 'weekend')
    ava = ava[[
        'dist', 'stop_id', 'stop_name', 'weekday_type', 'date', 'hour',
        'empty_minute', 'full_minute'
    ]]
    return ava


def load_txn_data(root_path, ym):
    '''
    Load transaction agg by date by hour data.
    Average rent and return to represent transaction of a station in a hour.
    '''
    txn = pd.read_csv(f'{root_path}/DM/{ym}/prepared_data/txn/aggregate_by_date_by_hour.csv')
    txn = filter_invalid_stop_id(txn)
    txn['hour'] = txn['hour'].astype(int)
    txn['txn_bike'] =  (txn['rent'] + txn['return'])
    txn = txn[['stop_id', 'date', 'hour', 'rent', 'return', 'txn_bike']]
    return txn


def load_dispatch_operation_data(root_path, ym):
    '''
    Load dispatch operation data.
    '''
    operation = pd.read_csv(
        f'{root_path}/DM/{ym}/prepared_data/dispatch/operation_agg_by_stop_by_date_hour.csv'
    )
    operation = filter_invalid_stop_id(operation)
    operation['hour'] = operation['hour'].astype(int)
    operation = operation[[
        'stop_id', 'date', 'hour', 'operation_bike', 'load_bike', 'unload_bike'
    ]]
    return operation


def load_dispatch_card_data(root_path, ym):
    '''
    Load dispatch operation data.
    '''
    dispatch_card = pd.read_csv(
        f'{root_path}/DM/{ym}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_date_hour.csv'
    )
    dispatch_card = filter_invalid_stop_id(dispatch_card)
    dispatch_card['hour'] = dispatch_card['hour'].astype(int)
    dispatch_card = dispatch_card[[
        'stop_id', 'date', 'hour', 'dispatch_bike', 'in_bike', 'out_bike'
    ]]
    return dispatch_card


def _fill_na(been_process_data, reference_data):
    # fill stop_name
    is_name_na = been_process_data['stop_name'].isna()
    stop_name_mapping = {
        id: name for _, (id, name) in reference_data[['stop_id', 'stop_name']].drop_duplicates().iterrows()
    }
    been_process_data.loc[is_name_na, 'stop_name'] = been_process_data.loc[is_name_na, 'stop_id'].map(stop_name_mapping)
    
    # fill weekday_type
    been_process_data['weekday_type'] = pd.to_datetime(been_process_data['date']).dt.weekday + 1
    been_process_data['weekday_type'] = np.where(been_process_data['weekday_type'] <= 5, 'weekday', 'weekend')
    
    # fill dist
    is_dist_na = been_process_data['dist'].isna()
    stop_dist_mapping = {
        id: dist for _, (id, dist) in reference_data[['stop_id', 'dist']].drop_duplicates().iterrows()
    }
    been_process_data.loc[is_dist_na, 'dist'] = been_process_data.loc[is_dist_na, 'stop_id'].map(stop_dist_mapping)
    
    # fill numeric columns
    na_columns = [
        'empty_minute', 'full_minute',
        'is_target',
        'dispatch_bike', 'in_bike', 'out_bike',
        'operation_bike', 'load_bike', 'unload_bike',
        'rent', 'return', 'txn_bike'
    ]
    been_process_data[na_columns] = been_process_data[na_columns].fillna(0)
    return been_process_data


def load_by_stop_date_hour_ym_data(root_path, start_hour, ym):
    '''
    Load, Merge, and Fill NA for data in given {ym}.
    Since a station with no empty/full minute will not be included in avaialbility data,
        merge data use outer join.
    '''
    # Load data
    target_stop = load_dispacth_testing_target(root_path)
    ava = load_availability_data(root_path, ym)
    txn = load_txn_data(root_path, ym)
    opa = load_dispatch_operation_data(root_path, ym)
    dis = load_dispatch_card_data(root_path, ym)

    # Merge data
    ava1 = ava.merge(target_stop, on=['stop_id', 'weekday_type'], how='left')
    agg = txn.merge(ava1, on=['stop_id', 'date', 'hour'], how='outer')
    agg = agg.merge(opa, on=['stop_id', 'date', 'hour'], how='outer')
    agg = agg.merge(dis, on=['stop_id', 'date', 'hour'], how='outer')

    # Filter data
    # Optimize dispatch strategy by maintaining bike availability ratio and minimizing dispatches.
    # Exclude pre-6am availability, dispatch cause we don't care about dispatches in midnight.
    # And exclude pre-6am transactions due to low activity.
    agg = agg.loc[agg['hour'] >= start_hour]
    
    # Fill NA
    agg = _fill_na(been_process_data=agg, reference_data=ava)
    
    # Drop invalid stop_id
    # There still some stop_id not in ava data, so we drop them here.
    agg = agg.loc[~agg['stop_name'].isna()]
    
    return agg


def agg_by_stop_date(by_stop_date_hour_data):
    by_stop_by_date = by_stop_date_hour_data.groupby(['stop_id', 'date']).agg(
        stop_name = pd.NamedAgg(column='stop_name', aggfunc='first'),
        hour_count = pd.NamedAgg(column='hour', aggfunc='count'),
        dist = pd.NamedAgg(column='dist', aggfunc='first'),
        weekday_type = pd.NamedAgg(column='weekday_type', aggfunc='first'),
        daily_empty_minute = pd.NamedAgg(column='empty_minute', aggfunc='sum'),
        daily_full_minute = pd.NamedAgg(column='full_minute', aggfunc='sum'),
        daily_dispatch_bike = pd.NamedAgg(column='dispatch_bike', aggfunc='sum'),
        daily_in_bike = pd.NamedAgg(column='in_bike', aggfunc='sum'),
        daily_out_bike = pd.NamedAgg(column='out_bike', aggfunc='sum'),
        daily_operation_bike = pd.NamedAgg(column='operation_bike', aggfunc='sum'),
        daily_load_bike = pd.NamedAgg(column='load_bike', aggfunc='sum'),
        daily_unload_bike = pd.NamedAgg(column='unload_bike', aggfunc='sum'),
        daily_rent = pd.NamedAgg(column='rent', aggfunc='sum'),
        daily_return = pd.NamedAgg(column='return', aggfunc='sum'),
        daily_txn_bike = pd.NamedAgg(column='txn_bike', aggfunc='sum'),
        is_target = pd.NamedAgg(column='is_target', aggfunc='max')
    ).reset_index()
    
    # available bike ratio/available dock ratio
    by_stop_by_date['available_bike_ratio'] = (
        1 - (by_stop_by_date['daily_empty_minute'] / MINUTE_IN_A_DAY)
    )
    by_stop_by_date['available_dock_ratio'] = (
        1 - (by_stop_by_date['daily_full_minute'] / MINUTE_IN_A_DAY)
    )
    
    # Add metrics
    # This metric can't be sum or mean by hourly data because it's a ratio
    by_stop_by_date['txn_per_diapatch'] = (
        by_stop_by_date['daily_txn_bike'] / by_stop_by_date['daily_dispatch_bike']
    )
    
    return by_stop_by_date


def agg_by_stop_weekdaytype(by_stop_date_data, weekday_type_days):
    """
    Since a station with no empty/full minute will not be included in avaialbility data,
        the computing of available_bike_ratio/available_dock_ratio 
        use total empty/full minute divided by 60*18*weekday_type_days.
    """
    by_stop_by_weekdaytype = by_stop_date_data.groupby(['stop_id', 'weekday_type']).agg(
        stop_name = pd.NamedAgg(column='stop_name', aggfunc='first'),
        dist = pd.NamedAgg(column='dist', aggfunc='first'),
        total_empty_minute = pd.NamedAgg(column='daily_empty_minute', aggfunc='sum'),
        total_full_minute = pd.NamedAgg(column='daily_full_minute', aggfunc='sum'),
        daily_dispatch_bike = pd.NamedAgg(column='daily_dispatch_bike', aggfunc='mean'),
        daily_in_bike = pd.NamedAgg(column='daily_in_bike', aggfunc='mean'),
        daily_out_bike = pd.NamedAgg(column='daily_out_bike', aggfunc='mean'),
        daily_operation_bike = pd.NamedAgg(column='daily_operation_bike', aggfunc='mean'),
        daily_load_bike = pd.NamedAgg(column='daily_load_bike', aggfunc='mean'),
        daily_unload_bike = pd.NamedAgg(column='daily_unload_bike', aggfunc='mean'),
        daily_rent = pd.NamedAgg(column='daily_rent', aggfunc='mean'),
        daily_return = pd.NamedAgg(column='daily_return', aggfunc='mean'),
        daily_txn_bike = pd.NamedAgg(column='daily_txn_bike', aggfunc='mean'),
        is_target = pd.NamedAgg(column='is_target', aggfunc='max')
    ).reset_index()
    
    # Add metrics
    by_stop_by_weekdaytype['txn_per_diapatch'] = (
        by_stop_by_weekdaytype['daily_txn_bike'] / by_stop_by_weekdaytype['daily_dispatch_bike']
    )
    
    # compute available_bike_ratio/available_dock_ratio
    # weekday
    is_weekday = (by_stop_by_weekdaytype['weekday_type']=='weekday')
    weekday_days = weekday_type_days['weekday']
    by_stop_by_weekdaytype.loc[is_weekday, 'available_bike_ratio'] = (
        1 - (by_stop_by_weekdaytype['total_empty_minute'] / (MINUTE_IN_A_DAY*weekday_days))
    )
    by_stop_by_weekdaytype.loc[is_weekday, 'available_dock_ratio'] = (
        1 - (by_stop_by_weekdaytype['total_full_minute'] / (MINUTE_IN_A_DAY*weekday_days))
    )
    # weekend
    is_weekend = ~is_weekday
    weekend_days = weekday_type_days['weekend']
    by_stop_by_weekdaytype.loc[is_weekend, 'available_bike_ratio'] = (
        1 - (by_stop_by_weekdaytype['total_empty_minute'] / (MINUTE_IN_A_DAY*weekend_days))
    )
    by_stop_by_weekdaytype.loc[is_weekend, 'available_dock_ratio'] = (
        1 - (by_stop_by_weekdaytype['total_full_minute'] / (MINUTE_IN_A_DAY*weekend_days))
    )
    
    # lable target stop by weekday_type
    by_stop_by_weekdaytype['is_weekday_target'] = np.where(
        by_stop_by_weekdaytype['weekday_type'] == 'weekday', by_stop_by_weekdaytype['is_target'], 0
    )
    by_stop_by_weekdaytype['is_weekend_target'] = np.where(
        by_stop_by_weekdaytype['weekday_type'] == 'weekend', by_stop_by_weekdaytype['is_target'], 0
    )
    by_stop_by_weekdaytype.drop(columns=['is_target'], inplace=True)
    
    return by_stop_by_weekdaytype


def agg_by_dist_weekdaytype(by_stop_weekdaytype_data):
    by_dist_by_weekdaytype = by_stop_weekdaytype_data.groupby(['dist', 'weekday_type']).agg(
        stop_count = pd.NamedAgg(column='stop_id', aggfunc='count'),
        weekday_targeted_stop_count = pd.NamedAgg(column='is_weekday_target', aggfunc='sum'),
        weekend_targeted_stop_count = pd.NamedAgg(column='is_weekend_target', aggfunc='sum'),
        total_empty_minute = pd.NamedAgg(column='total_empty_minute', aggfunc='sum'),
        total_full_minute = pd.NamedAgg(column='total_full_minute', aggfunc='sum'),
        available_bike_ratio = pd.NamedAgg(column='available_bike_ratio', aggfunc='mean'),
        available_dock_ratio = pd.NamedAgg(column='available_dock_ratio', aggfunc='mean'),
        daily_dispatch_bike = pd.NamedAgg(column='daily_dispatch_bike', aggfunc='mean'),
        daily_in_bike = pd.NamedAgg(column='daily_in_bike', aggfunc='mean'),
        daily_out_bike = pd.NamedAgg(column='daily_out_bike', aggfunc='mean'),
        daily_operation_bike = pd.NamedAgg(column='daily_operation_bike', aggfunc='mean'),
        daily_load_bike = pd.NamedAgg(column='daily_load_bike', aggfunc='mean'),
        daily_unload_bike = pd.NamedAgg(column='daily_unload_bike', aggfunc='mean'),
        daily_rent = pd.NamedAgg(column='daily_rent', aggfunc='mean'),
        daily_return = pd.NamedAgg(column='daily_return', aggfunc='mean'),
        daily_txn_bike = pd.NamedAgg(column='daily_txn_bike', aggfunc='mean')
    ).reset_index()
    
    # Add metrics
    by_dist_by_weekdaytype['txn_per_diapatch'] = (
        by_dist_by_weekdaytype['daily_txn_bike'] / by_dist_by_weekdaytype['daily_dispatch_bike']
    )
    
    # rename
    by_dist_by_weekdaytype.rename(
        columns={
            'stop_id': 'stop_count',
            'is_weekday_target': 'weekday_targeted_stop_count',
            'is_weekend_target': 'weekend_targeted_stop_count',
        },
        inplace=True
    )
    
    return by_dist_by_weekdaytype


def find_usually_dispatch_stop(by_stop_date_data, USUALLY_DISPATCH_THRESHOLD, weekday_type_to_keep):
    """
    Filter out stops that are not usually dispatched.

    Args:
        results (dict): Dictionary containing the results.
        YMS (list): List of year-month strings.
        USUALLY_DISPATCH_THRESHOLD (int): Minimum number of dispatch days to consider a stop as usually dispatched.
        weekday_type_to_keep (str): Weekday type to keep. Can be 'weekday', 'weekend', or None.

    Returns:
        set: Set of stop IDs that are usually dispatched.
    """
    # Count dispatch day
    by_stop_date_data['is_dispatch'] = by_stop_date_data['daily_dispatch_bike'] > 0

    if weekday_type_to_keep is not None:
        by_stop_date_data = by_stop_date_data.loc[by_stop_date_data['weekday_type'] == weekday_type_to_keep]

    by_stop_dispatch_day_sum = by_stop_date_data.groupby('stop_id')['is_dispatch'].sum().reset_index()
    by_stop_dispatch_day_sum.rename(columns={'is_dispatch': 'dispatch_day_count'}, inplace=True)

    # Keep stop that is usually dispatch
    is_usually_dispatch = by_stop_dispatch_day_sum['dispatch_day_count'] >= USUALLY_DISPATCH_THRESHOLD
    stop_to_keep = set(by_stop_dispatch_day_sum.loc[is_usually_dispatch, 'stop_id'])

    return stop_to_keep


def compute_difference(before_data, after_data, join_list, except_list):
    """
    Compute the difference between two datasets based on specified columns.

    Args:
        before_data (pandas.DataFrame): The dataframe containing the data before the changes.
        after_data (pandas.DataFrame): The dataframe containing the data after the changes.
        join_list (list): The list of columns used for joining the two datasets.
        except_list (list): The list of columns for which the difference should not be calculated.
            Columns in except_list will be kept from before_data,
            if the value in before_data is NaN, the value in after_data will be used instead.

    Returns:
        pandas.DataFrame: The dataframe containing the computed differences.

    """
    cols = before_data.columns
    full_data = before_data.merge(
        after_data, on=join_list, how='outer', suffixes=('_before', '_after')
    )
    
    results = []
    for col in cols:
        col_in_before = f'{col}_before'
        col_in_after = f'{col}_after'
        
        if col in join_list:
            results.append(full_data[col])
        elif col in except_list:
            new_col = np.where(
                full_data[col_in_before].isna(), full_data[col_in_after], full_data[col_in_before]
            )
            results.append(pd.Series(new_col, name=col))
        else:
            temp_col = full_data[col_in_after] - full_data[col_in_before]
            results.append(temp_col)
            
    results = pd.concat(results, axis=1)
    results.columns = cols
    return full_data, results


# Config
ROOT_PATH = r'D:\iima\ubike分析'
OUTPUT_PATH = ROOT_PATH + '/DM/202311/調度優化成效'
MINUTE_IN_A_DAY = 60 * 18
YMS = ['202309', '202311']
WEEKDAYS = {
    '202309': {'weekday': 21, 'weekend': 9},
    '202311': {'weekday': 22, 'weekend': 8}
}
START_HOUR = 6
# I thought other districts had more transactions per dispatch because some stations dispatch naturally.
# But after removing those with less than 10 days of dispatch, it's still the same."
IS_KEEP_ONLY_USUALLY_DISPATCH = False
USUALLY_DISPATCH_THRESHOLD = WEEKDAYS[YMS[0]]['weekday'] // 2


# Init
results = {}
for ym in YMS:
    results[ym] = {}

# Load data
for ym in YMS:
    by_stop_date_hour = load_by_stop_date_hour_ym_data(ROOT_PATH, START_HOUR, ym)
    results[ym]['by_stop_date_hour'] = by_stop_date_hour

# Agg data
for ym in YMS:
    # Agg and add additional columns
    by_stop_date = agg_by_stop_date(results[ym]['by_stop_date_hour'])
    if IS_KEEP_ONLY_USUALLY_DISPATCH:
        if ym == YMS[0]:  # Only run when first ym
            stop_to_keep = find_usually_dispatch_stop(
                by_stop_date, USUALLY_DISPATCH_THRESHOLD, 'weekday'
            )
        by_stop_date = by_stop_date.loc[by_stop_date['stop_id'].isin(stop_to_keep)]
    by_stop_weekdaytype = agg_by_stop_weekdaytype(by_stop_date, WEEKDAYS[ym])
    by_dist_weekdaytype = agg_by_dist_weekdaytype(by_stop_weekdaytype)
    # Save
    results[ym]['by_stop_by_date'] = by_stop_date
    results[ym]['by_stop_by_weekdaytype'] = by_stop_weekdaytype
    results[ym]['by_dist_by_weekdaytype'] = by_dist_weekdaytype


# Compute difference
# by stop by weekdaytype
join_cols = ['stop_id', 'weekday_type']
except_cols = join_cols + ['stop_name', 'dist', 'is_weekday_target', 'is_weekend_target']
by_stop_by_weekdaytype_join, by_stop_by_weekdaytype_diff = compute_difference(
    before_data=results['202309']['by_stop_by_weekdaytype'],
    after_data=results['202311']['by_stop_by_weekdaytype'],
    join_list=join_cols,
    except_list=except_cols
)
# by dist
join_cols = ['dist', 'weekday_type']
except_cols = join_cols + ['weekday_targeted_stop_count', 'weekend_targeted_stop_count']
by_dist_by_weekdaytype_join, by_dist_by_weekdaytype_diff = compute_difference(
    before_data=results['202309']['by_dist_by_weekdaytype'],
    after_data=results['202311']['by_dist_by_weekdaytype'],
    join_list=join_cols,
    except_list=except_cols
)

# Save data
results['202309']['by_stop_by_date'].to_csv(
    OUTPUT_PATH+'/by_stop_by_date_202309.csv',
    index=False
)
results['202311']['by_stop_by_date'].to_csv(
    OUTPUT_PATH+'/by_stop_by_date_202311.csv',
    index=False
)
by_stop_by_weekdaytype_join.to_excel(
    OUTPUT_PATH+'/by_stop_by_weekdaytype_join.xlsx',
    sheet_name='by_stop_by_weekdaytype_join',
    index=False
)
by_dist_by_weekdaytype_join.to_excel(
    OUTPUT_PATH+'/by_dist_by_weekdaytype_join.xlsx',
    sheet_name='by_dist_by_weekdaytype_join',
    index=False
)
by_stop_by_weekdaytype_diff.to_excel(
    OUTPUT_PATH+'/by_stop_by_weekdaytype_diff.xlsx',
    sheet_name='by_stop_by_weekdaytype_diff',
    index=False
)
by_dist_by_weekdaytype_diff.to_excel(
    OUTPUT_PATH+'/by_dist_by_weekdaytype_diff.xlsx',
    sheet_name='by_dist_by_weekdaytype_diff',
    index=False
)