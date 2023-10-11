"""
This module provides functionality for training machine learning models for a specific task.

It includes classes for data preprocessing and training with various machine learning models
like LightGBM, XGBoost, RandomForest, and CATBoost.

Usage:
- Import this module and create instances of the 'Train' class to train machine learning models.

Example:
    import train

    # Initialize a list of dates and model names
    date_list = ['20230305', '20230311', '20230317', '20230322']

    # Train a LightGBM model
    train.Train(date_list=date_list, model_name='LightGBM').run()

    # Train an XGBoost model
    train.Train(date_list=date_list, model_name='XGBoost').run()

    # Train a CATBoost model
    train.Train(date_list=date_list, model_name='CATBoost').run()

"""

import configparser
import os
import pickle
import warnings
from ast import literal_eval

import geopandas as gpd
import model.catboost
import model.lightgbm
import model.randomforest
import model.xgboost
import pandas as pd
import src.detect.abnormal
from shapely.geometry import Point
from sklearn import preprocessing
from sklearn.model_selection import train_test_split

warnings.filterwarnings('ignore')

class Train:
    """
    A class for training machine learning models using data preprocessing and various models.

    This class allows you to train machine learning models such as LightGBM, XGBoost, RandomForest,
    and CATBoost using the specified date list and model name.

    Usage:
    - Create an instance of this class with the 'date_list' and 'model_name' parameters.
    - Call the 'run' method to perform data preprocessing and train the selected model.

    Parameters:
        date_list (list): A list of date strings specifying the data to be used for training.
        model_name (str): The name of the machine learning model to be trained
        ('LightGBM', 'XGBoost', 'RandomForest', 'CATBoost').
    """

    def __init__(self, date_list, model_name):
        # date & modelname
        self.date_list = date_list
        self.hyphen_date_list = [
            x[0:4] + '-' + x[4:6] + '-' + x[6:8] for x in self.date_list
        ]
        self.model = model_name

        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        self.target = literal_eval(ini['target'])
        self.seed = literal_eval(ini['seed'])

        # file path
        file_path = config['FILEPATH']
        self.transaction_file_path = literal_eval(file_path['transaction'])
        self.population_file_path = literal_eval(file_path['population'])
        self.done_file_path = 'output_ON' if self.target == 'on' else 'output_OFF'

        # categorical features
        self.categorical_features = [
            'Date', 'Hour', 'IsWeekend', 'SlopeClass'
        ]

        # model
        self.abn = src.detect.abnormal.Base()
        self.lgbm = model.lightgbm.LightGBM()
        self.rf = model.randomforest.RandomForest()
        self.xgb = model.xgboost.XGBoost()
        self.catb = model.catboost.CATBoost()

    def get_train_infer_info(self):
        # get the GridID, which doesn't have any YouBike stops
        # read stop data
        stop_df = pd.read_csv(
            f'{self.transaction_file_path}/86ec099baa2d36c22ab3a87350b718de_export.csv'
        )
        stop_df = stop_df[['sno', 'lat', 'lng']]
        stop_df['sno'] = stop_df['sno'].astype(str)
        stop_df['sno'] = 'U' + stop_df['sno'].str[3:]
        stop_df['geometry'] = stop_df.apply(
            lambda x: Point((x.lng, x.lat)), axis=1
        )
        stop_df = gpd.GeoDataFrame(stop_df, crs=4326)
        stop_df = stop_df.to_crs(3826)
        stop_point = stop_df[['sno', 'geometry']]

        # read Grid data
        grid_df = gpd.read_file(
            f'{self.population_file_path}/FET_2023_grid_97.geojson'
        )
        grid_poly = grid_df[['gridid', 'geometry']]

        # overlay between stop_point and grid_poly
        overlay_df = gpd.overlay(
            stop_point, grid_poly, how='union').explode().reset_index()

        # the GridiD would be viewed as inference data
        all_grid_id = set(grid_df.gridid)
        covered_grid_id = {
            int(x)
            for x in list(set(overlay_df.gridid))
            if str(x) != 'nan'
        }
        uncovered_grid_id = all_grid_id.difference(covered_grid_id)
        with open(f'{self.done_file_path}/NoStopGridID.pkl', 'wb') as nsgi:   #Pickling
            pickle.dump(uncovered_grid_id, nsgi)

        ## get "Empty Date Hour StopID" and "Full Date Hour StopID"
        # empty
        if 'EmptyDateHourStop.pkl' in os.listdir(f'{self.done_file_path}'):
            with open(f'{self.done_file_path}/EmptyDateHourStop.pkl', 'rb') as fp:   # Unpickling
                empty_date_hour_stop = pickle.load(fp)
        else:
            stop_time_df = pd.read_csv(
                f'{self.transaction_file_path}/status_return_time_merge_txn_and_dispatch.csv'
            )
            stop_time_df = stop_time_df[stop_time_df.date_m6h.isin(self.hyphen_date_list)]
            empty_date_hour_stop = []
            for (date_m6h, stop_id), df in stop_time_df.groupby(['date_m6h', 'stop_id']):
                empty_date_hour_stop = self.abn.find_date_hour_stop_id(
                    df, issue='empty', tolerate_num=1, tolerate_time=15,
                    date_m6h=date_m6h, stop_id=stop_id,
                    result=empty_date_hour_stop
                )
            with open(f'{self.done_file_path}/EmptyDateHourStop.pkl', 'wb') as fp:   #Pickling
                pickle.dump(empty_date_hour_stop, fp)

        # full
        if 'FullDateHourStop.pkl' in os.listdir(f'{self.done_file_path}'):
            with open(f'{self.done_file_path}/FullDateHourStop.pkl', 'rb') as fp:   # Unpickling
                full_date_hour_stop = pickle.load(fp)
        else:
            stop_time_df = pd.read_csv(
                f'{self.transaction_file_path}/status_return_time_merge_txn_and_dispatch.csv'
            )
            stop_time_df = stop_time_df[stop_time_df.date_m6h.isin(self.hyphen_date_list)]
            full_date_hour_stop = []
            for (date_m6h, stop_id), df in stop_time_df.groupby(['date_m6h', 'stop_id']):
                full_date_hour_stop = self.abn.find_date_hour_stop_id(
                    df, issue='full', tolerate_num=1, tolerate_time=15,
                    date_m6h=date_m6h, stop_id=stop_id,
                    result=full_date_hour_stop
                )
            with open(f'{self.done_file_path}/FullDateHourStop.pkl', 'wb') as fp:   #Pickling
                pickle.dump(full_date_hour_stop, fp)

        # empty
        empty_df = pd.DataFrame(
            empty_date_hour_stop, columns=['Date', 'Hour', 'sno']
        )
        # print('# of empty data points', len(empty_df))
        empty_stop_point = empty_df.merge(
            stop_point, how='inner', on='sno'
        )[['sno', 'geometry']]
        empty_stop_point.drop_duplicates(
            inplace=True, ignore_index=True
        )
        empty_stop_point = gpd.GeoDataFrame(empty_stop_point)
        overlay_empty_df = gpd.overlay(
            empty_stop_point, grid_poly, how='union'
        ).explode().reset_index()
        empty_df = empty_df.merge(
            overlay_empty_df, how='left', on='sno'
        )
        empty_df.dropna(
            inplace=True, ignore_index=True
        )
        empty_df['Date'] = empty_df['Date'].str.split('-').str.join('').astype(int)
        empty_df['GridID'] = empty_df['gridid'].astype(int)

        # full
        full_df = pd.DataFrame(
            full_date_hour_stop, columns=['Date', 'Hour', 'sno']
        )
        # print('# of full data points', len(full_df))
        full_stop_point = full_df.merge(
            stop_point, how='inner', on='sno'
        )[['sno', 'geometry']]
        full_stop_point.drop_duplicates(
            inplace=True, ignore_index=True
        )
        full_stop_point = gpd.GeoDataFrame(full_stop_point)
        overlay_full_df = gpd.overlay(
            full_stop_point, grid_poly, how='union'
        ).explode().reset_index()
        full_df = full_df.merge(
            overlay_full_df, how='left', on='sno'
        )
        full_df.dropna(
            inplace=True, ignore_index=True
        )
        full_df['Date'] = full_df['Date'].str.split('-').str.join('').astype(int)
        full_df['GridID'] = full_df['gridid'].astype(int)

        # get inference data info
        self.inference_grid_id = list(uncovered_grid_id)
        self.inference_empty = empty_df[['Date', 'Hour', 'GridID']]
        self.inference_full = full_df[['Date', 'Hour', 'GridID']]

        # some stopID located in the same GridID, which will be 'drop_duplicates'
        self.inference_empty.drop_duplicates(
            inplace=True, ignore_index=True
        )
        self.inference_full.drop_duplicates(
            inplace=True, ignore_index=True
        )

    def get_train_val_test(self):
        # read Full Data
        df = pd.read_csv(
            f'{self.done_file_path}/DF.csv'
        )

        # append uncovered GridID to inference index
        all_index = list(df.index)
        inference_index = list(
            df[df.GridID.isin(self.inference_grid_id)].index
        )

        # append Empty to inference index
        tem_empty_df = pd.merge(
            df.reset_index(), self.inference_empty, how='inner', on=['Date', 'Hour', 'GridID']
        ).set_index('index')
        inference_index = inference_index + list(tem_empty_df.index)

        # append Full to inference index
        tem_full_df = pd.merge(
            df.reset_index(), self.inference_full, how='inner', on=['Date', 'Hour', 'GridID']
        ).set_index('index')
        inference_index = inference_index + list(tem_full_df.index)

        # happen uncovered, empty and full simultaneously
        inference_index = list(set(inference_index))
        inference_index.sort()

        # get Training Index
        training_index = list(set(all_index).difference(set(inference_index)))
        training_index.sort()

        modeling_data = df.loc[training_index]
        inference_data = df.loc[inference_index]

        # save
        modeling_data.to_csv(
            f'{self.done_file_path}/modeling_data.csv', index=False
        )
        inference_data.to_csv(
            f'{self.done_file_path}/inference_data.csv', index=False
        )

        # deal with categorical features
        if self.model == 'LightGBM':
            for c in self.categorical_features:
                modeling_data[c] = modeling_data[c].astype('category')

        elif self.model == 'XGBoost':
            lbl = preprocessing.LabelEncoder()
            for c in self.categorical_features:
                modeling_data[c] = lbl.fit_transform(
                    modeling_data[c].astype(str)
                )

        elif self.model == 'RandomRorest':
            for c in self.categorical_features:
                dummy_df = pd.get_dummies(modeling_data[c])
                dummy_df.columns = [
                    f'{c}_{x}'
                    for x in dummy_df.columns
                ]
                modeling_data = pd.concat(
                    [modeling_data, dummy_df], axis=1
                )
                modeling_data.drop(
                    [c], inplace=True, axis=1
                )

        elif self.model == 'CATBoost':
            pass

        # train, validation and test dataset with the proportion 8:1:1
        data = modeling_data.drop(
            ['OnCounts', 'OffCounts', 'NetCounts'], axis=1
        )
        target = modeling_data['OnCounts'] if self.target == 'on' else modeling_data['OffCounts']
        x_train_val, self.x_test, y_train_val, self.y_test = train_test_split(
            data, target, test_size=0.1, random_state=self.seed
        )
        self.x_train, self.x_val, self.y_train, self.y_val = train_test_split(
            x_train_val, y_train_val, test_size=0.11, random_state=self.seed
        )

    def run(self):
        self.get_train_infer_info()
        self.get_train_val_test()

        if self.model == 'RandomForest':
            self.rf.train(
                self.x_train, self.x_test, self.y_train, self.y_test
            )

        elif self.model == 'LightGBM':
            self.lgbm.train(
                self.x_train, self.x_val, self.x_test,
                self.y_train, self.y_val, self.y_test,
                self.categorical_features
            )

        elif self.model == 'XGBoost':
            self.xgb.train(
                self.x_train, self.x_val, self.x_test,
                self.y_train, self.y_val, self.y_test
            )

        elif self.model == 'CATBoost':
            self.catb.train(
                self.x_train, self.x_val, self.x_test,
                self.y_train, self.y_val, self.y_test,
                self.categorical_features
            )

if __name__ == '__main__':
    Train(
        date_list=['20230305', '20230311', '20230317', '20230322'], model_name='LightGBM'
    ).run()

    Train(
        date_list=['20230305', '20230311', '20230317', '20230322'], model_name='XGBoost'
    ).run()

    Train(
        date_list=['20230305', '20230311', '20230317', '20230322'], model_name='CATBoost'
    ).run()
