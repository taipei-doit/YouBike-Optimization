# 方便調用code
# import sys
# sys.path.append(r'D:\iima\ubike分析\code')
# from udf_function import make_sure_folder_exist

import os
import datetime
import pandas as pd
from numpy import nan
import numpy as np
from shapely.geometry import multilinestring, MultiLineString
import re
import json
import requests
import pytz
taipei_tz = pytz.timezone('Asia/Taipei')

def to_time_contain_chinese_string(x):
    '''
    處理時間欄位裡包含上午/下午，甚至後面附帶.000
    
    Example
    ----------
    to_time_contain_chinese_string(None)
    to_time_contain_chinese_string("2022/7/14 上午 12:00:00")
    to_time_contain_chinese_string("2022/7/14 下午 12:00:00")
    to_time_contain_chinese_string("2022/7/14 下午 12:00:00.000")
    '''
    if x:
        x = x.replace('.000', '')
        x = x.replace('  ', ' ')
        split_x = x.split(' ')
        if split_x[1] == '上午':
            hour = int(split_x[2][0:2])
            if hour == 12:  # 上午12=00點
                fine_x = split_x[0] + ' ' + '00'+ split_x[2][2:]
            else:  # 不用轉換
                fine_x = split_x[0] + ' ' + split_x[2]
        elif split_x[1] == '下午':
            hour = int(split_x[2][0:2])+12  # 下午 = +12
            if hour == 24:  # 下午12點=12點
                hour = 12
            fine_x = split_x[0] + ' ' + str(hour) + split_x[2][2:]
        else:
            # print(x)
            pattern = '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'
            if re.match(pattern, x)[0]:
                fine_x = x
            else:
                fine_x = re.findall(pattern, x)[0]
        return fine_x
    else:
        return None


def _parse_from_format(from_format):
    '''
    解析from_format以做後續利用
    
    Example
    ----------
    time_column = pd.Series(['111/12/31', '110/12/31'])
    pattern, items = _parse_from_format(from_format='cy/m/d')
    
    time_column = pd.Series(['111-12-31', '110-12-31'])
    pattern, items = _parse_from_format(from_format='cy-m-d')
    
    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    pattern, items = _parse_from_format(from_format='y/m/d')
    
    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    pattern, items = _parse_from_format(from_format='cy/m/d H:M:S')
    '''
    from_format += ';'
    sep_list = [':', ' ', ',', '/', '-']
    
    items = []
    pattern = ''
    temp = ''
    for char in from_format:
        if char in sep_list:
            sep = char
            items.append(temp)
            pattern += f'([0-9]+){sep}'
            temp = ''
        elif char == ';':
            pattern += '([0-9]+)'
            items.append(temp)
        else:
            temp += char
    
    return pattern, items


def _standardize_time_string(column, from_format):
    '''
    根據提供的form_format，將input處理成標準時間格式
    
    Example
    ----------
    time_column = pd.Series(['111/12/31', '110/12/31'])
    datetime_str = _standardize_time_string(time_column, from_format='cy/m/d')
    
    time_column = pd.Series(['111-12-31', '110-12-31'])
    datetime_str = _standardize_time_string(time_column, from_format='cy-m-d')
    
    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    datetime_str = _standardize_time_string(time_column, from_format='y/m/d')
    
    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    datetime_str = _standardize_time_string(time_column, from_format='cy/m/d H:M:S')
    '''
    
    pattern, items = _parse_from_format(from_format)
    splited_column = column.str.extract(pattern)
    splited_column.columns = items
    
    for item in items:
        if item == 'cy':
            temp_column = splited_column[item].copy()
            temp_column = temp_column.astype(float) + 1911
            splited_column['y'] = temp_column.astype(int).astype(str)
    
    datetime_col = pd.Series(['']*splited_column.shape[0])
    item_founded = ''
    pre_time_item = ''
    for time_item in ['y', 'm', 'd', 'H', 'M', 'S']:
        try:
            if pre_time_item == '':
                pass
            elif pre_time_item == 'y':
                datetime_col += '-'
            elif pre_time_item == 'm':
                datetime_col += '-'
            elif pre_time_item == 'd':
                datetime_col += ' '
            elif pre_time_item == 'H':
                datetime_col += ':'
            elif pre_time_item == 'M':
                datetime_col += ':'
            else:
                raise ValueError(f'Not valid previous time format code *{pre_time_item}*!')
            datetime_col += splited_column[time_item]
            item_founded += time_item
            # print(splited_column[time_item])
        except KeyError:
            print(f'*{time_item}* not found, only *{item_founded}*')
            break
        pre_time_item = time_item
    return datetime_col


def convert_str_to_time_format(column: pd.Series, from_format=None,
                               output_level='datetime', output_type='time',
                               is_utc=False, from_timezone='Asia/Taipei',
                               on_error='raise'
                               ) -> pd.Series:
    '''
    時間處理 function
    Input should be pd.Series with string.
    Output type depending on para output_level and output_type.
    
    Parameters
    ----------
    output_level: "date" or "datetime", default "datetime".
    output_type: "str" or "time", default "time".
    from_format: defalut None, means format were common, let function automatically parse.
        Or, you can given string like "ty/m/d" or "y-m-d",
        function will split input string by "/" then convert to time format.
        Format "ty" is taiwan year, ty will +1911 to western year.
        All allowed code is [y, m, d, H, M, S].
    is_utc: defalut False, which means input is not UTC timezone.
    from_timezone: defalut "Asia/Taipei", if is_utc=False, from_timezone must be given.
        if is_utc=True, from_timezone will be ignored.
        
    Example
    ----------
    t1 = to_time_contain_chinese_string("2022/7/14 上午 12:00:00")
    t2 = to_time_contain_chinese_string("2022/7/14 下午 12:00:00.000")
    time_column = pd.Series([t1, t2])
    datetime_col = convert_str_to_time_format(time_column, output_level='date')
    
    time_column = pd.Series(['111/12/31', '110/12/31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d')
    
    time_column = pd.Series(['111-12-31', '110-12-31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy-m-d')
    
    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='y/m/d')
    datetime_col = convert_str_to_time_format(time_column, from_format='y/m/d', is_utc=True)
    
    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S')
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S', output_level='date')
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S', output_type='str')
    '''
    if from_format:
        column = _standardize_time_string(column, from_format)
    
    if is_utc:
        column = pd.to_datetime(column, errors=on_error)
    else:
        try:
            column = pd.to_datetime(column, utc=is_utc, errors=on_error).dt.tz_localize(from_timezone)
        except TypeError:
            column = column.astype(str).str.replace('\\+08:00', '')
            column = pd.to_datetime(column, utc=is_utc, errors=on_error).dt.tz_localize(from_timezone)
    
    if output_level == 'date':
        column = column.dt.date
        
    if output_type == 'str':
        column = column.astype(str)
    
    return column


def convert_to_float(column):
    '''
    無論原本欄位的格式，轉成float格式
    
    Example
    ----------
    data = pd.DataFrame({'name': ['a', 'b', 'c', 'd'],
                         'type': ['A', 'B', 'C', 'D']})
    x = pd.Series([121.123, 123.321, '', None])
    y = pd.Series([25.123, 26.321, None, ''])
    xx = convert_to_float(x)
    gdf = add_point_wkbgeometry_column_to_df(data, x, y, from_crs=4326)
    
    x = pd.Series([262403.2367, 481753.6091, '', None])
    y = pd.Series([2779407.0527, 2914189.1837, None, ''])
    convert_to_float(x)
    convert_to_float(y)
    '''
    try:
        column = column.astype(float)
    except ValueError:
        is_empty = (column=='')
        is_na = pd.isna(column)
        column.loc[is_empty|is_na] = nan
        column = column.astype(float)
    return column


def get_tpe_now_time_str():
    '''
    Get now time with tz = 'Asia/Taipei'.
    Output is a string truncate to seconds.
    output Example: '2022-09-21 17:56:18'
    
    Example
    ----------
    get_tpe_now_time_str()
    '''
    now_time = str(datetime.datetime.now(tz=taipei_tz)).split('.')[0]
    return now_time


def get_datataipei_data_updatetime(url):
    '''
    Request lastest update time of given data.taipei url.
    Output is a string truncate to seconds.
    output Example: '2022-09-21 17:56:18'
    
    Example
    ----------
    url = 'https://data.taipei/api/frontstage/tpeod/dataset/change-history.list?id=4fefd1b3-58b9-4dab-af00-724c715b0c58'
    get_datataipei_data_updatetime(url)
    '''
    # 抓data.taipei的更新時間
    res = requests.get(url)
    update_history = json.loads(res.text)
    lastest_update = update_history['payload'][0]
    lastest_update_time = lastest_update.split('更新於')[-1]
    return lastest_update_time.strip()

def mapping_category_ignore_number(string, cate):
    try:
        return cate[string]
    except KeyError:
        return string


def get_datataipei_data_file_last_modeified_time(url, rank=0):
    '''
    Request lastest modeified time of given data.taipei url.
    Output is a string truncate to seconds.
    The json can contain more than one data last modifytime, "rank" para chose which one.
    output Example: '2022-09-21 17:56:18'
    
    Example
    ----------
    '''
    # 抓data.taipei的更新時間
    res = requests.get(url)
    data_info = json.loads(res.text)
    lastest_modeified_time = data_info['payload']['resources'][rank]['last_modified']
    return lastest_modeified_time

def linestring_to_multilinestring(geo):
    '''
    將LineString轉換為MultiLineString
    
    Example
    ----------
    line_a = LineString([[0,0], [1,1]])
    line_b = LineString([[1,1], [1,0]])
    multi_line = MultiLineString([line_a, line_b])
    linestring_to_multilinestring(None)
    type(linestring_to_multilinestring(multi_line))
    type(linestring_to_multilinestring(line_a))
    '''
    is_multistring = (type(geo)==multilinestring.MultiLineString)
    is_na = pd.isna(geo)
    if (is_multistring) or (is_na):
        return geo
    else:
        return MultiLineString([geo])
    
def taipei_opendata_offset_function(rid):
    '''
    Get Data.taipei API，自動遍歷所有資料。
    (data.taipei的API單次return最多1000筆，因此需利用offset定位，取得所有資料)
    '''
    url = f"""https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire"""
    response = requests.request("GET", url)
    data_dict = response.json()
    count = data_dict['result']['count']
    res = list()
    offset_count = int(count/1000)
    for i in range(offset_count+1):
        i = i* 1000
        url = f"""https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire&offset={i}&limit=1000"""
        response = requests.request("GET", url)
        get_json = response.json()
        res.extend(get_json['result']['results'])
    return res

    
def given_string_to_none(input_str, given_str, mode='start'):
    '''
    輸入任意string，若符合指定文字，則轉成None，不符合則保持原樣
    此funciton能igonre data type的問題
    
    Example
    ----------
    given_string_to_none('-990.00', '-99')
    given_string_to_none('-90.00', '-99')
    given_string_to_none('-990.00', '-99', mode='end')
    given_string_to_none('-990.00', '-99', mode='test')
    '''
    if mode == 'start':
        try:
            is_target = input_str.startswith(given_str)
        except:
            is_target = False
    elif mode == 'end':
        try:
            is_target = input_str.endswith(given_str)
        except:
            is_target = False
    else:
        is_target = False
    
    if is_target:
        return None
    else:
        return input_str
    
 
def get_tpe_now_time_timestamp(minutes_delta=None):
    '''
    Get now time with tz = 'Asia/Taipei'.
    '''
    from datetime import datetime, timedelta
    if minutes_delta:
        now_timestamp = (datetime.now(tz=taipei_tz)+timedelta(minutes=minutes_delta)).timestamp() * 1e3
    else:
        now_timestamp = datetime.now(tz=taipei_tz).timestamp() * 1e3
    return now_timestamp
    

def delete_input_data(input_file_path: str):
    '''
    因空間不足，處理完的資料，刪掉原始檔案
    
    Test
    ---------
    delete_input_data('D:\test.csv')
    '''
    os.remove(input_file_path)
    print(f'Deleted {input_file_path}.')
    
    
def make_sure_folder_exist(output_folder_path: str):
    '''
    若輸出資料夾不存在，則建立資料夾
    
    Test
    ---------
    check_folder_exist('D:\\test')
    '''
    if not os.path.isdir(output_folder_path):
        os.makedirs(output_folder_path)
        print(f'Create folder {output_folder_path}.')
        
        
def generate_next_month_firstday(date:datetime.date, n=1):
    '''
    根據input時間，return n個月的第一天
    
    Test code
    ------------
    date = datetime.date(2022, 2, 1)
    timedelta_one_month(date)
    '''
    m = date.month
    mp1 = m + 1
    if mp1 == 13:
        mp1 = 1

    date_after_n_month = date + datetime.timedelta(n)
    while date_after_n_month.month != mp1:
        date_after_n_month = date + datetime.timedelta(n)
        # print(delta, date_after_n_month)
        if n > 31:
            raise ValueError('Eroor at generate_next_month_firstday, timedelta > 31!')
        n += 1
    return date_after_n_month


def zero_to_twentyfour(hour):
    '''
    將數字0轉換成24
    主要用途是把0點變成24點，畫圖的時序會比較好看
    
    Test code
    ------------
    zero_to_twentyfour(0)
    zero_to_twentyfour(1)
    '''
    if hour == 0:
        return 24
    else:
        return hour
    

def generate_hour_to_period_dict():
    '''
    產生小時->時段的字典
    
    Test code
    ------------
    generate_hour_to_period_dict()
    '''
    hour_to_period = {}
    hour_to_period.update({h: '0twilight' for h in range(0, 6)})
    hour_to_period.update({h: '1morning' for h in range(6, 10)})
    hour_to_period.update({h: '2noon' for h in range(10, 15)})
    hour_to_period.update({h: '3afternoon' for h in range(15, 20)})
    hour_to_period.update({h: '4evening' for h in range(20, 24)})
    return hour_to_period


def generate_time_sequence_index(seq_tuple1, seq_tuple2):
    '''
    為了powerbi呈現，需產生一連續數字來當作時間序列
    
    Test code
    ------------
    weekday_hour_mapping = generate_time_sequence_index((1, 8), (0, 24))
    '''
    s1, e1 = seq_tuple1
    s2, e2 = seq_tuple2
    time_to_index = {}
    _index = 0
    for i in range(1, 8):
        for j in range(0, 5):
            _i = str(i)
            _j = str(j)
            time_to_index[f'{_i}{_j}'] = _index
            _index += 1
    return time_to_index


def load_ubike_stop(ym, dim_path=r'D:\iima\ubike分析\DIM'):
    if ym=='202304':
        tpe_ubike_stop = pd.read_csv(dim_path+'/ubike_stops_from_api_202304.csv')
        tpe_ubike_stop = tpe_ubike_stop[['stop_id', 'stop_name', 'lng', 'lat']]
        ntpe_ubike_stop = pd.read_csv(dim_path+'/ntpe_ubike_stop_20230411.csv')
        ntpe_ubike_stop = ntpe_ubike_stop[['stop_id', 'stop_name', 'lng', 'lat']]
        ubike_stop = pd.concat([tpe_ubike_stop, ntpe_ubike_stop])
    else: 
        ubike_stop = pd.read_csv(dim_path+f'/ubike_stop_{ym}.csv')
    return ubike_stop[['stop_id', 'stop_name', 'lng', 'lat']]


def load_stop_mapping_table(dim_path = r'D:\iima\ubike分析\DIM'):
    ver1 = pd.read_excel(dim_path+'/mapping_table_name_and_id_202305.xlsx',
                         sheet_name='1.0')
    ver1.columns = ['stop_id', 'stop_name']
    ver1['stop_id'] = 'U' + ver1['stop_id'].astype(str)
    ver1['stop_name'] = ver1['stop_name'].str.replace('@', '')
    ver1['ver'] = '1.0'
    ver2 = pd.read_excel(dim_path+'/mapping_table_name_and_id_202305.xlsx',
                         sheet_name='2.0')
    ver2.columns = ['stop_id', 'stop_name']
    ver2['stop_id'] = ver2['stop_id'].astype(str)
    ver2['stop_id'] = 'U' + ver2['stop_id'].str.replace(r'^500', '', regex=True)
    ver2['ver'] = '2.0'
    mapping_table = pd.concat([ver1, ver2])
    return mapping_table


def pivot_table_by_hour(long_data, piovt_value, index_col, is_round=True, is_add_sum_col=True):
    """
    Generate a pivot table by hour from the given long_data.
    (A high-level function to call pd.pivot_table.)
    (Aggfunc is mean.)

    Parameters:
    - long_data (DataFrame): The DataFrame that need to be pivot from long to wide.
    - piovt_value (str): The column name to be used as the values in the pivot table.
    - index_col (list): The column names to be used as the index in the pivot table.
    - is_round (bool, optional): Whether to round(1) the values in the pivot table. Default is True.
    - is_add_sum_col (bool, optional): Whether to add sum columns to the pivot table. Default is True.
        sum columns include: sum_0_23, sum_6_23.

    Returns:
    - pivot_by_hour (DataFrame): The generated pivot table by hour.
    """
    pivot_by_hour = pd.pivot_table(
        long_data,
        values=piovt_value,
        index=index_col,
        columns='hour',
        aggfunc='mean'
    ).reset_index()
    
    index_col_len = len(index_col)
    wide = pivot_by_hour.shape[1]
    if is_round:
        pivot_by_hour.iloc[:, index_col_len:wide] = pivot_by_hour.iloc[:, index_col_len:wide].round(1)
    if is_add_sum_col:
        pivot_by_hour['sum_0_23'] = pivot_by_hour.iloc[:, index_col_len:wide].sum(axis=1)
        pivot_by_hour['sum_6_23'] = pivot_by_hour.iloc[:, (index_col_len+6):wide].sum(axis=1)
    pivot_by_hour.columns = pivot_by_hour.columns.astype(str)
    return pivot_by_hour


def filter_invalid_stop_id(data: pd.DataFrame, stop_id_col='stop_id', is_only_taipei=True, is_only_user_stop=True):
    """
    Filters out not Taipei and not user can use(e.g. service center) stop_id.
    stop_id column should be given since some data may have different column name, i.g. `stop_id` or `on_stop_id`.

    Args:
        data (DataFrame): The input DataFrame containing `stop_id` column.

    Returns:
        DataFrame: The filtered DataFrame with only valid stop_id.
        
    --------------------------------
    Test:
    txn = pd.read_csv(f'{ROOT_PATH}/DM/{ym}/prepared_data/txn/aggregate_by_date_by_hour.csv')
    txn = filter_invalid_stop_id(txn)
    """
    service_center_stop_id = [
        'U199001', 'U199002', 'U199003', 'U199004', 'U199005',
        'U199006', 'U199007', 'U199008', 'U199009', 'U199010'
    ]
    unknown_stop_id = ['U0', 'U999999']
    invalid_stop_id = unknown_stop_id + service_center_stop_id
    
    if is_only_taipei:
        is_taipei = data[stop_id_col].str.startswith('U1')
        data = data.loc[is_taipei]
    
    if is_only_user_stop:
        data = data.loc[~data[stop_id_col].isin(invalid_stop_id)]
    
    # print invalid stop_id
    print(f'Filter out data rows: {data.shape[0] - data.shape[0]}')
    print(f'Invalid stop_id: {set(data[stop_id_col]) - set(data[stop_id_col])}')
    return data


def load_txn_data(
    file_path,
    is_add_hour=True,
    is_add_date=True,
    is_add_weekday=True,
    is_filter_invalid_stop=True,
    is_only_taipei=False,
    is_only_user_stop=True,
    date_col='on_time'
    ):
    txn = pd.read_csv(
        file_path,
        dtype={
            'trans_type': str, 
            'route_name': str,
            'card_id': str, 
            'card_type': str, 
            'data_type': str,
            'on_stop_id': str,
            'on_stop': str,
            'on_lng': np.float64,
            'on_lat': np.float64,
            'on_time': str, 
            'off_stop_id': str,
            'off_stop': str,
            'off_lng': np.float64,
            'off_lat': np.float64,
            'off_time': str,
            'txn_amt': np.float64,
            'txn_disc': np.float64,
            'data_date': str,
            'distance_m': np.float64,
            'time_diff_mins': np.float64,
            'speed': np.float64,
            'is_transfer': bool,
            'journey_id': str,
            'transfer_mark': str,
        }
    )
    txn['on_time'] = pd.to_datetime(txn['on_time']).dt.tz_localize(None)
    txn['off_time'] = pd.to_datetime(txn['off_time']).dt.tz_localize(None)

    if is_filter_invalid_stop:
        txn = filter_invalid_stop_id(
            txn,
            stop_id_col='on_stop_id',
            is_only_taipei=is_only_taipei,
            is_only_user_stop=is_only_user_stop
        )
        txn = filter_invalid_stop_id(
            txn,
            stop_id_col='off_stop_id',
            is_only_taipei=is_only_taipei,
            is_only_user_stop=is_only_user_stop
        )

    if is_add_hour:
        txn['on_hour'] = txn['on_time'].dt.hour
        txn['off_hour'] = txn['off_time'].dt.hour

    if is_add_date:
        txn['date'] = txn[date_col].dt.date

    if is_add_weekday:
        txn['weekday'] = txn[date_col].dt.weekday + 1
        txn['weekday_type'] = txn['weekday'].apply(lambda x: 'weekend' if x>=5 else 'weekday')

    return txn