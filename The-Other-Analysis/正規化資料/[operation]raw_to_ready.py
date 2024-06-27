import os
import pandas as pd
import xlrd

ym = '202403'
DIS_PATH = f'D:/iima/ubike分析/DW/raw_operation/{ym}'
OUTPUT_PATH = f'D:/iima/ubike分析/DM/{ym}/prepared_data/dispatch'
files = os.listdir(DIS_PATH)

# concat dispatch files
operation = []
for file in files:
    workbook = xlrd.open_workbook(
        os.path.join(DIS_PATH, file),
        ignore_workbook_corruption=True
    )
    temp = pd.read_excel(workbook)
    operation.append(temp)
operation = pd.concat(operation)
operation['場站代號'] = operation['場站代號'].astype(str)
is_start_with_500 = operation['場站代號'].str.startswith('500')
operation.loc[is_start_with_500, '場站代號'] = (
    'U' + operation.loc[is_start_with_500, '場站代號'].str.slice(3, )
)

operation.to_csv(
    OUTPUT_PATH+'/dispatch_operation_log.csv', index=False, encoding='UTF-8'
)
