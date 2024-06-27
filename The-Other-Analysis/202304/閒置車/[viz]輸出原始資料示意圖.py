# -*- coding: utf-8 -*-
"""
Created on Wed May 10 18:50:48 2023

@author: rz3881
"""

joined_txn = combine_status_and_txn(target_txn, target_status, is_return_view=True)

joined_dispatch = combine_status_and_dispatch(target_dispatch, target_status, is_return_view=True)

joined_table = joined_txn.merge(joined_dispatch, how='outer', on='time')
joined_table = joined_table.drop(columns='status_y')
joined_table.columns = ['time', '站點回傳', '交易', '調度類型', '調度車數']

joined_table.loc[joined_table['站點回傳']=='api', '站點回傳'] = '回傳'
joined_table.loc[joined_table['交易']=='on', '交易'] = '借'
joined_table.loc[joined_table['交易']=='off', '交易'] = '還'
joined_table.loc[joined_table['調度類型']=='tie', '調度類型'] = '綁'
joined_table.loc[joined_table['調度類型']=='untie', '調度類型'] = '解綁'
joined_table.loc[joined_table['調度類型']=='load', '調度類型'] = '載走'
joined_table.loc[joined_table['調度類型']=='unload', '調度類型'] = '載來'