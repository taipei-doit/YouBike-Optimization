import pandas as pd
import networkx as nx
import time

# Init config
ROOT_PATH = r'D:\iima\ubike分析'
INPUT_PATH =  f'{ROOT_PATH}/DM/202403/journey'

# Read graph
DG = nx.read_gpickle(INPUT_PATH+'/journey_graph_weekday_.gpickle')

# 給定的節點集合
start_nodes = ['U101001']

# 找到所有相連的節點
connected_nodes = set(start_nodes)
for start_node in start_nodes:
    if start_node in DG:
        # 使用BFS找到從start_node開始的所有節點
        successors = nx.bfs_successors(DG, start_node)
        # 添加到connected_nodes集合中
        for nodes in successors.values():
            connected_nodes.update(nodes)
