"""
This module defines a base class 'Base' that contains methods for getting
model parameters, models, performance metrics, and predictions.

"""
import os
import pickle

import pandas as pd


class Base:
    """
    Class: Base

    This class provides methods for managing machine learning model parameters,
    saving and loading models, recording performance metrics, and saving predictions.

    """
    def __init__(self):
        pass

    def params(
            self, hyperparameter_file_path, save_id, trials, best_params
        ):
        df_file_path = f'{hyperparameter_file_path}/paramsDF.csv'
        d = {
            'ID': save_id,
            'Trials': trials
        }

        if 'paramsDF.csv' not in os.listdir(hyperparameter_file_path):
            d.update(
                {
                    k:[v]
                    for k, v in best_params.items()
                }
            )
            params_df = pd.DataFrame(d)
        else:
            params_df = pd.read_csv(df_file_path)
            d.update(best_params)
            params_df.loc[len(params_df)] = d

        params_df.to_csv(
            df_file_path, index=False
        )

    def model(self, best_model, model_file_path, save_id):
        with open(f'{model_file_path}/{save_id}.pkl', 'wb') as mf:
            pickle.dump(
                best_model, mf
            )

        # # Load Model
        # pickled_model = pickle.load(open('model.pkl', 'rb'))
        # pickled_model.predict(x_test)

    def performance(
            self, perf_file_path, save_id, model_name, trials,
            test_mae_score, test_rmse_score, test_r2_score
        ):
        performance_file_path = f'{perf_file_path}/performanceDF.csv'
        if 'performanceDF.csv' not in os.listdir(perf_file_path):
            performance_df = pd.DataFrame(
                {},
                columns=[
                    'ID', 'Model', 'Trials',
                    'Testing MAE', 'Testing RMSE', 'Testing R2'
                ]
            )
        else:
            performance_df = pd.read_csv(performance_file_path)

        # add new row data
        performance_df.loc[len(performance_df)] = [
            save_id, model_name, trials,
            test_mae_score, test_rmse_score, test_r2_score
        ]

        # set column type
        performance_df['ID'] = performance_df['ID'].astype(int)
        performance_df['Model'] = performance_df['Model'].astype(str)
        performance_df['Trials'] = performance_df['Trials'].astype(int)
        performance_df['Testing MAE'] = performance_df['Testing MAE'].astype(float)
        performance_df['Testing RMSE'] = performance_df['Testing RMSE'].astype(float)
        performance_df['Testing R2'] = performance_df['Testing R2'].astype(float)

        # save
        performance_df.sort_values(
            by=['Model', 'Trials'], inplace=True
        )
        performance_df.to_csv(
            performance_file_path, index=False
        )

    def prediction(
            self, pred_file_path, save_id,
            x_test, y_test, test_pred
        ):
        prediction_file_path = f'{pred_file_path}/{save_id}.csv'
        df = x_test
        df['y_test'] = y_test
        df['y_pred'] = test_pred.tolist()
        df['error'] = df['y_test'] - df['y_pred']
        df = df.reindex(
            df.error.abs().sort_values(ascending=False).index
        )
        df.index = range(len(df))
        df.to_csv(
            prediction_file_path, index=False
        )
