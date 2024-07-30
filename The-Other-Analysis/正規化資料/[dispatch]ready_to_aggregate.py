import pandas as pd
import numpy as np

# Config
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'

# Load
dispatch_card = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/cleaned_raw.csv')

# Add new columns
dispatch_card['data_time'] = pd.to_datetime(dispatch_card['txn_time'])
dispatch_card['date'] = dispatch_card['data_time'].dt.date
dispatch_card['hour'] = dispatch_card['data_time'].dt.hour
dispatch_card['in_bike'] = np.where(dispatch_card['dispatch_type'] == 'in', 1, 0)
dispatch_card['out_bike'] = np.where(dispatch_card['dispatch_type'] == 'out', 1, 0)

# Agg output by date by hour
agg_by_date_by_hour = dispatch_card.groupby(
    ['stop_id', 'date', 'hour']
).agg(
    stop_name = pd.NamedAgg(column='stop_name', aggfunc='first'),
    in_bike = pd.NamedAgg(column='in_bike', aggfunc='sum'),
    out_bike = pd.NamedAgg(column='out_bike', aggfunc='sum')
).reset_index()
agg_by_date_by_hour['dispatch_bike'] = (
    agg_by_date_by_hour['in_bike'] + agg_by_date_by_hour['out_bike']
)
# Save
agg_by_date_by_hour.to_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_date_hour.csv',
    index=False
)

# agg daily dispatch bikes
agg_by_date_by_hour['weekday'] = pd.to_datetime(agg_by_date_by_hour['date']).dt.weekday + 1
daily_agg_dispatch = agg_by_date_by_hour.groupby(
    ['stop_id', 'date']
).agg(
    stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
    weekday=pd.NamedAgg(column='weekday', aggfunc='first'),
    daily_dispatch_in=pd.NamedAgg(column='in_bike', aggfunc='sum'),
    daily_dispatch_out=pd.NamedAgg(column='out_bike', aggfunc='sum'),
    daily_dispatch_abs=pd.NamedAgg(column='dispatch_bike', aggfunc='sum')
).reset_index()
# Save
agg_by_date_by_hour.to_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_date.csv',
    index=False
)

# Agg weekday type dispatch bikes
daily_agg_dispatch['is_dispatch'] = daily_agg_dispatch['daily_dispatch_abs'] > 0
weekday_agg_dispatch = daily_agg_dispatch.groupby(
    ['stop_id', 'weekday']
).agg(
    stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
    is_dispatch_day=pd.NamedAgg(column='is_dispatch', aggfunc='sum'),
    mean_daily_dispatch_in=pd.NamedAgg(column='daily_dispatch_in', aggfunc='mean'),
    mean_daily_dispatch_out=pd.NamedAgg(column='daily_dispatch_out', aggfunc='mean'),
    mean_daily_dispatch_abs=pd.NamedAgg(column='daily_dispatch_abs', aggfunc='mean')
).reset_index()
# Save
weekday_agg_dispatch.to_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_weekday.csv',
    index=False
)

# agg weekday type dispatch
weekday_agg_dispatch['weekday_type'] = weekday_agg_dispatch['weekday'].apply(
    lambda x: 'weekend' if x > 5 else 'weekday'
)
weekdaytype_agg_dispatch = weekday_agg_dispatch.groupby(
    ['stop_id', 'weekday_type']
).agg(
    stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
    is_dispatch_day=pd.NamedAgg(column='is_dispatch_day', aggfunc='sum'),
    mean_daily_dispatch_in=pd.NamedAgg(column='mean_daily_dispatch_in', aggfunc='mean'),
    mean_daily_dispatch_out=pd.NamedAgg(column='mean_daily_dispatch_out', aggfunc='mean'),
    mean_daily_dispatch_abs=pd.NamedAgg(column='mean_daily_dispatch_abs', aggfunc='mean')
).reset_index()
# Save
weekdaytype_agg_dispatch.to_csv(
    f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_weekday_type.csv',
    index=False
)
