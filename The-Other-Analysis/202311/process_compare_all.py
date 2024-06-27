import pandas as pd

# Config
root_dir = 'D:/iima/ubike分析'

# Load data
dispatch = pd.read_csv(root_dir+'/DM/202311/compare_all.csv')

# Calculate the new columns
dispatch['diff_0907'] = dispatch['num_avg_rent_202309'] - dispatch['num_avg_rent_202307']
dispatch['diff_0904'] = dispatch['num_avg_rent_202309'] - dispatch['num_avg_rent_202304']
dispatch['diff_0911'] = dispatch['num_avg_rent_202309'] - dispatch['num_avg_rent_202211']
dispatch['established_time'] = pd.to_datetime(dispatch['established_time'])
dispatch['is_202303_new'] = (dispatch['established_time'].dt.month == 3)
dispatch['is_202304_new'] = (dispatch['established_time'].dt.month == 4)
dispatch['is_202307_new'] = (dispatch['established_time'].dt.month == 7)
dispatch['is_202309_new'] = (dispatch['established_time'].dt.month == 9)

# Save data
dispatch.to_csv(root_dir+'/DM/202311/compare_all_plus_test.csv', index=False)

