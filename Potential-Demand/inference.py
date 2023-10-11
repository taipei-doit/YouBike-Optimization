"""
Module: inference

This module performs inference using a machine learning model trained for predicting
certain target values based on preprocessed data.

"""

import configparser
import pickle
from ast import literal_eval

import pandas as pd
from sklearn import preprocessing


class Inference:
    """
    Class: Inference

    This class defines an inference process using a trained machine learning model
    to predict target values based on input data.

    Attributes:
        not_features (list): A list of features that are not considered as input features.
        target (str): The target variable ('on' or 'off') to specify which prediction to make.
        performance_file_path (str): The file path of the performance data.
        infer_df_file_path (str): The file path of the inference data.
        categorical_features (list): A list of categorical features that require encoding.

    Methods:
        __init__(self): Constructor method to initialize the Inference object.
        run(self): Execute the inference process, including data preprocessing and prediction.

    Usage:
        inference = Inference()
        inference.run()
    """

    def __init__(self):
        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        self.not_features = literal_eval(ini['not_features'])
        self.target = literal_eval(ini['target'])

        # file path
        if self.target == 'on':
            self.performance_file_path = 'output_ON/performance/performanceDF.csv'
            self.infer_df_file_path = 'output_ON/inference_data.csv'

        elif self.target == 'off':
            self.performance_file_path = 'output_OFF/performance/performanceDF.csv'
            self.infer_df_file_path = 'output_OFF/inference_data.csv'

        # categorical features
        self.categorical_features = ['Date', 'Hour', 'IsWeekend', 'SlopeClass']

    def run(self):
        # read
        perf_df = pd.read_csv(self.performance_file_path)
        infer_df = pd.read_csv(self.infer_df_file_path)
        infer_df.set_index(self.not_features, inplace=True)

        # preprocess
        drop_cols = ['OnCounts', 'OffCounts', 'NetCounts']
        infer_df.drop(
            drop_cols, axis=1, inplace=True
        )

        # get best model
        row_index = perf_df['Testing RMSE'] == perf_df['Testing RMSE'].min()
        model_id = perf_df.loc[row_index, 'ID'].tolist()[0]
        model_name = perf_df.loc[row_index, 'Model'].tolist()[0]
        print(f'Use {model_name} (id : {model_id}) to predict inference data!')

        # deal with categorical features
        if model_name == 'LightGBM':
            for c in self.categorical_features:
                infer_df[c] = infer_df[c].astype('category')

        elif model_name == 'XGBoost':
            lbl = preprocessing.LabelEncoder()
            for c in self.categorical_features:
                infer_df[c] = lbl.fit_transform(infer_df[c].astype(str))

        elif model_name == 'RandomRorest':
            for c in self.categorical_features:
                dummy_df = pd.get_dummies(infer_df[c])
                dummy_df.columns = [f'{c}_{x}' for x in dummy_df.columns]
                infer_df = pd.concat(
                    [infer_df, dummy_df], axis=1
                )
                infer_df.drop(
                    [c], axis=1, inplace=True
                )

        elif model_name == 'CATBoost':
            pass

        # file path
        if self.target == 'on':
            model_file_path = f'output_ON/model/{model_id}.pkl'
            pred_infer_df_file_path = f'output_ON/predictionInfer/{model_id}.csv'

        elif self.target == 'off':
            model_file_path = f'output_OFF/model/{model_id}.pkl'
            pred_infer_df_file_path = f'output_OFF/predictionInfer/{model_id}.csv'

        # read best model
        with open(model_file_path, 'rb') as bm:
            best_model = pickle.load(bm)

        # predict
        infer_pred = best_model.predict(infer_df).round()
        infer_df['pred'] = infer_pred
        infer_df.reset_index(inplace=True)

        # save
        infer_df.to_csv(
            pred_infer_df_file_path, index=False
        )


if __name__ == '__main__':
    I = Inference()
    I.run()
