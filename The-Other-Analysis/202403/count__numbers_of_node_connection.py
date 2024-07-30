import pandas as pd

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH =  f'{ROOT_PATH}/DM/202403/journey'
LOW_BOUND = 5

# Read od table
df = pd.read_csv(INPUT_PATH+'/journey_graph_weekday_.csv')

# Transform txn times to node connection
bool_df = df.iloc[:, 1:] > LOW_BOUND

# Count numbers of node connection
count = bool_df.sum(axis=1)
count.index = df.iloc[:, 0]
count.sort_values(ascending=False, inplace=True)

# Save to csv
count.to_csv(INPUT_PATH+'/count_numbers_of_node_connection.csv')