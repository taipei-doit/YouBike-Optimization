import pandas as pd

# Constants
ROOT_PATH = 'D:/iima/ubike分析'
YM = '202403'
EXCLUDE_REGION = ['維護所']
NTU_REGION = ['ZB1', 'ZB2', 'ZB3']


# Load
# dispatch region
region = pd.read_csv(f'{ROOT_PATH}/DIM/dispatch_region_{YM}.csv')
# dispatch card
dispatch = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/dispatch/dispatch_card_agg_by_stop_by_date.csv')
# transaction
txn = pd.read_csv(f'{ROOT_PATH}/DM/{YM}/prepared_data/txn/aggregate_by_date_by_hour.csv')

# Transform
# Agg by day
# dispatch
daily_dispatch = dispatch.groupby(['stop_id', 'date']).agg(
    stop_name=pd.NamedAgg(column='stop_name', aggfunc='first'),
    weekday=pd.NamedAgg(column='weekday', aggfunc='first'),
    dispatch_in=pd.NamedAgg(column='in_bike', aggfunc='sum'),
    dispatch_out=pd.NamedAgg(column='out_bike', aggfunc='sum')
).reset_index()
daily_dispatch['dispatch_abs'] = daily_dispatch['dispatch_in'] + daily_dispatch['dispatch_out']
# txn
daily_txn = txn.groupby(['stop_id', 'date']).agg(
    stop_name=pd.NamedAgg(column='stop', aggfunc='first'),
    weekday=pd.NamedAgg(column='weekday', aggfunc='first'),
    txn_rent=pd.NamedAgg(column='rent', aggfunc='sum'),
    txn_return=pd.NamedAgg(column='return', aggfunc='sum'),
).reset_index()
daily_txn['txn_abs'] = daily_txn['txn_rent'] + daily_txn['txn_return']
# merge
daily_agg = daily_txn.merge(
    daily_dispatch,
    on=['stop_id', 'date', 'weekday'],
    how='left'
)
daily_agg = daily_agg.merge(region[['stop_id', 'region']], on='stop_id', how='left')
daily_agg['weekday_type'] = daily_agg['weekday'].apply(lambda x: 'weekday' if x <= 5 else 'weekend')
# cleansing
daily_agg = daily_agg.drop(columns=['stop_name_y'])
daily_agg = daily_agg.rename(columns={'stop_name_x': 'stop_name'})
daily_agg = daily_agg.loc[~daily_agg['region'].isna()]
daily_agg = daily_agg.loc[~daily_agg['region'].isin(EXCLUDE_REGION)]

# Analyse
weekday_daily_agg = daily_agg.loc[daily_agg['weekday_type'] == 'weekday']
# taipei city
total_dispatch = weekday_daily_agg['dispatch_abs'].sum()
total_rent = weekday_daily_agg['txn_rent'].sum()
(total_dispatch / total_rent)
# NTU
ntu_weekday_daily_agg = weekday_daily_agg.loc[weekday_daily_agg['region'].isin(NTU_REGION)]
ntu_total_dispatch = ntu_weekday_daily_agg['dispatch_abs'].sum()
ntu_total_rent = weekday_daily_agg['txn_rent'].sum()
(ntu_total_dispatch / ntu_total_rent)
