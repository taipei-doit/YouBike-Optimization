"""
This script represents a pipeline for data preprocessing, model training, and inference.

It initializes with a list of dates for processing and executes the following steps:
1. Data Preprocessing:
   - Data preprocessing is performed using the 'Preprocess' class.
   - The processing time for data preprocessing is measured and printed.

2. Model Training:
   - Machine learning models are trained using different algorithms (LightGBM, XGBoost, CATBoost).
   - The script calculates and prints the processing time for modeling.

3. Inference:
   - Inference is performed using the 'Inference' class.
   - The processing time for inference is measured and printed.

Please ensure that the 'Preprocess,' 'Train,' and 'Inference' classes are correctly implemented
in separate Python files.
"""

import datetime

import inference
import preprocess
import train

# Initialize with a list of date strings
date_list = [
    '20230305', '20230311', '20230317', '20230322'
]

# Data Preprocessing
p = preprocess.Preprocess(date_list=date_list)

# Measure and print the start time of data preprocessing
start_time = datetime.datetime.now()
print(f'Data Preprocessing Start at {start_time}!\n')

# Run the data preprocessing step
p.run()

# Measure and print the end time of data preprocessing
end_time = datetime.datetime.now()
print(f'\nData Preprocessing Finished at {end_time}!')

# Calculate and print the processing time for data preprocessing
processing_time = end_time - start_time
print(f'\nProcessing Time on Data Preprocessing is {processing_time}!')

# Model Training
train_lgbm = train.Train(date_list=date_list, model_name='LightGBM')
train_xgb = train.Train(date_list=date_list, model_name='XGBoost')
train_catb = train.Train(date_list=date_list, model_name='CATBoost')

# Measure and print the start time of modeling (training)
start_time = datetime.datetime.now()
print(f'Modeling Start at {start_time}!\n')

# Run the model training steps
train_lgbm.run()
train_xgb.run()
train_catb.run()

# Measure and print the end time of modeling (training)
end_time = datetime.datetime.now()
print(f'\nModeling Finished at {end_time}!')

# Calculate and print the processing time for modeling
processing_time = end_time - start_time
print(f'\nProcessing Time on Modeling is {processing_time}!')

print('- ' * 40)

# Inference
i = inference.Inference()

# Measure and print the start time of inference
start_time = datetime.datetime.now()
print(f'Inference Start at {start_time}!\n')

# Run the inference step
i.run()

# Measure and print the end time of inference
end_time = datetime.datetime.now()
print(f'\nInference Finished at {end_time}!')

# Calculate and print the processing time for inference
processing_time = end_time - start_time
print(f'\nProcessing Time on Inference is {processing_time}!')
