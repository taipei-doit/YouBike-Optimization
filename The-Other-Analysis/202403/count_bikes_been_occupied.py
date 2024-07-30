import pandas as pd
import pickle

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
OUTPUT_PATH =  f'{ROOT_PATH}/DM/202403/bike_status'

# Read time table
with open(f'{OUTPUT_PATH}/bike_status.pkl', 'rb') as f:
    bike_status = pickle.load(f)

# Count the number of bikes been occupied
occupied_summary = {
    'date': [], 'used_bikes': [], 'max_occupied_bikes': [], 'max_occupied_time': []
}
for date, daily_status in bike_status.items():
    # make sure the status is boolean, 0: not occupied, 1: occupied
    # cause there are some 2 in the status(returned but not rented in the same day)
    daily_status = daily_status==1

    # bikes have been occupied
    occupied_bikes = daily_status.sum(axis=0)
    # find max number of bikes that have been occupied and the time
    occupied_summary['date'].append(date)
    occupied_summary['used_bikes'].append(daily_status.shape[0])
    occupied_summary['max_occupied_bikes'].append(occupied_bikes.max())
    occupied_summary['max_occupied_time'].append(occupied_bikes.idxmax())
occupied_summary = pd.DataFrame(occupied_summary)
occupied_summary['occupied_rate'] = occupied_summary['max_occupied_bikes'] / occupied_summary['used_bikes']

# Save to csv
occupied_summary.to_csv(f'{OUTPUT_PATH}/occupied_summary.csv')