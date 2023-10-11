"""
This module provides a class for training a CatBoost machine learning model with
hyperparameter optimization using Optuna. It reads configuration parameters from
'params.ini' and saves the trained model and related information.

"""

import configparser
import datetime
from ast import literal_eval

import numpy as np
import optuna
import pandas as pd
import src.evaluate.metric
import src.save.savecatb
from catboost import CatBoostRegressor, Pool
from optuna.samplers import TPESampler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


class CATBoost:
    """
    This class reads configuration parameters from 'params.ini' to set up the model
    and hyperparameter optimization. It provides methods for training the model,
    optimizing hyperparameters, and saving model-related information.

    Methods:
        train(x_train, x_val, x_test, y_train, y_val, y_test, categorical_features)
            Train the CatBoost model with hyperparameter optimization using Optuna.
    """
    def __init__(self):
        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        catb_default = config['CATB.DEFAULT']
        catb_fixed_params = config['CATB.FIXED.PARAMS']
        catb_searched_params = config['CATB.SEARCHED.PARAMS']

        self.seed = literal_eval(ini['seed'])
        self.not_features = literal_eval(ini['not_features'])
        self.catb_trials = literal_eval(catb_default['trials'])
        self.catb_fixed_params = {
            k: literal_eval(v)
            for k, v in catb_fixed_params.items()
        }
        self.catb_fixed_params['random_seed'] = self.seed
        self.catb_searched_params = {
            k: literal_eval(v)
            for k, v in catb_searched_params.items()
        }
        curr_time = str(datetime.datetime.now()).split('.', maxsplit=1)[0]
        self.save_id = curr_time.replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

    def train(
            self, x_train, x_val, x_test,
            y_train, y_val, y_test, categorical_features
        ):

        def objective(trial):

            self.catb_fixed_params['cat_features'] = categorical_features

            searched_params = {
                'learning_rate': trial.suggest_float(
                    'learning_rate',
                    self.catb_searched_params['learning_rate_min'],
                    self.catb_searched_params['learning_rate_max']
                ),
                'max_depth': trial.suggest_int(
                    'max_depth',
                    self.catb_searched_params['max_depth_min'],
                    self.catb_searched_params['max_depth_max']
                ),
                'subsample': trial.suggest_float(
                    'subsample',
                    self.catb_searched_params['subsample_min'],
                    self.catb_searched_params['subsample_max']
                ),
                'colsample_bylevel': trial.suggest_float(
                    'colsample_bylevel',
                    self.catb_searched_params['colsample_bylevel_min'],
                    self.catb_searched_params['colsample_bylevel_max']
                ),
                'min_data_in_leaf': trial.suggest_int(
                    'min_data_in_leaf',
                    self.catb_searched_params['min_data_in_leaf_min'],
                    self.catb_searched_params['min_data_in_leaf_max']
                ),
            }

            # set hyperparameters
            params = {
                **self.catb_fixed_params, **searched_params
            }

            # train
            model = CatBoostRegressor(**params)
            model.fit(x_train.drop(columns=self.not_features, axis=1),
                      y_train,
                      eval_set=[(x_val.drop(columns=self.not_features, axis=1),y_val)],
                      early_stopping_rounds=50
            )

            # validate
            val_pred = model.predict(
                x_val.drop(columns=self.not_features, axis=1)).round()
            val_rmse = mean_squared_error(y_val, val_pred) ** (1/2)

            return val_rmse


        # create study
        study = optuna.create_study(
            direction='minimize', sampler=TPESampler(seed=self.seed)
        )
        study.optimize(
            objective, n_trials=self.catb_trials
        )

        print('- '* 40)

        print(f'Number of finished trials: {len(study.trials)}')

        print('- '* 40)


        best_params = {
            **self.catb_fixed_params, **study.best_trial.params
        }
        self.best_model = CatBoostRegressor(**best_params)
        self.best_model.fit(x_train.drop(columns=self.not_features, axis=1),
                           y_train,
                           eval_set=[(x_val.drop(columns=self.not_features, axis=1),y_val)],
                           early_stopping_rounds=50
        )

        print('- '* 40)

        print(f'The best parameters are: {study.best_trial.params}')


        train_pred = self.best_model.predict(
            x_train.drop(columns=self.not_features, axis=1)).round()
        train_mae_score = mean_absolute_error(y_train, train_pred)
        train_rmse_score = mean_squared_error(y_train, train_pred) ** (1/2)
        train_r2_score = r2_score(y_train, train_pred)

        test_pred = self.best_model.predict(
            x_test.drop(columns=self.not_features, axis=1)).round()
        test_mae_score = mean_absolute_error(y_test, test_pred)
        test_rmse_score = mean_squared_error(y_test, test_pred) ** (1/2)
        test_r2_score = r2_score(y_test, test_pred)

        print('- '* 40)

        print(f'MAE on training dataset = {train_mae_score}')
        print(f'RMSE on training dataset = {train_rmse_score}')
        print(f'R2 Score on training dataset = {train_r2_score}')

        print('- '* 40)

        print(f'MAE on test dataset = {test_mae_score}')
        print(f'RMSE on test dataset = {test_rmse_score}')
        print(f'R2 Score on test dataset = {test_r2_score}')

        print('- '* 40)

        # make feature importance DF
        importance = self.best_model.get_feature_importance(
            Pool(
                data=x_train.drop(columns=self.not_features, axis=1),
                label=y_train,
                cat_features=categorical_features
            )
        )
        names = list(
            x_train.drop(columns=self.not_features, axis=1).columns
        )
        feature_importance = np.array(importance)
        feature_names = np.array(names)

        #Create a DataFrame using a Dictionary
        fi_df = pd.DataFrame(
            {
                'feature_names':feature_names,
                'feature_importance':feature_importance
            }
        )
        fi_df.sort_values(
            by=['feature_importance'], ascending=False, inplace=True
        )

        # save
        self.save = src.save.savecatb.Base(
            self.save_id, self.best_model, self.catb_trials, best_params
        )
        self.save.save_feature_importance(fi_df)
        self.save.save_params()
        self.save.save_model()
        self.save.save_performance(test_mae_score, test_rmse_score, test_r2_score)
        self.save.save_prediction(x_test, y_test, test_pred)
