import pandas as pd
import networkx as nx
import time

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH =  f'{ROOT_PATH}/DM/202403/journey'

# Read graph
graph = nx.read_gpickle(INPUT_PATH+'/journey_graph_weekday_.gpickle')

# Convert graph to DataFrame, from i to j
df = nx.to_pandas_adjacency(graph, dtype=int)

# Save to csv
df.to_csv(INPUT_PATH+'/journey_graph_weekday_.csv')