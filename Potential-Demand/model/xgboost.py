"""
This module provides a class for training an XGBoost machine learning model with
hyperparameter optimization using Optuna. It reads configuration parameters from
'params.ini' and saves the trained model and related information.

"""

import configparser
import datetime
from ast import literal_eval

import optuna
import src.save.savexgb
import xgboost as xgb
from optuna.samplers import TPESampler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


class XGBoost:
    """
    This class reads configuration parameters from 'params.ini' to set up the model
    and hyperparameter optimization. It provides methods for training the model,
    optimizing hyperparameters, and saving model-related information.

    Methods:
        train(x_train, x_val, x_test, y_train, y_val, y_test)
            Train the XGBoost model with hyperparameter optimization using Optuna.
    """
    def __init__(self):
        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        xgb_default = config['XGB.DEFAULT']
        xgb_fixed_params = config['XGB.FIXED.PARAMS']
        xgb_searched_params = config['XGB.SEARCHED.PARAMS']

        self.seed = literal_eval(ini['seed'])
        self.not_features = literal_eval(ini['not_features'])
        self.xgb_trials = literal_eval(xgb_default['trials'])
        self.xgb_fixed_params = {
            k: literal_eval(v)
            for k, v in xgb_fixed_params.items()
        }
        self.xgb_fixed_params['seed'] = self.seed
        self.xgb_searched_params = {
            k: literal_eval(v)
            for k, v in xgb_searched_params.items()
        }
        curr_time = str(datetime.datetime.now()).split('.', maxsplit=1)[0]
        self.save_id = curr_time.replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

    def train(self, x_train, x_val, x_test, y_train, y_val, y_test):

        def objective(trial):

            searched_params = {
                'eta': trial.suggest_float(
                    'eta',
                    self.xgb_searched_params['eta_min'],
                    self.xgb_searched_params['eta_max']
                ),
                'gamma': trial.suggest_float(
                    'gamma',
                    self.xgb_searched_params['gamma_min'],
                    self.xgb_searched_params['gamma_max']
                ),
                'max_depth': trial.suggest_int(
                    'max_depth',
                    self.xgb_searched_params['max_depth_min'],
                    self.xgb_searched_params['max_depth_max']
                ),
                'min_child_weight': trial.suggest_float(
                    'min_child_weight',
                    self.xgb_searched_params['min_child_weight_min'],
                    self.xgb_searched_params['min_child_weight_max']
                ),
                'max_delta_step': trial.suggest_int(
                    'max_delta_step',
                    self.xgb_searched_params['max_delta_step_min'],
                    self.xgb_searched_params['max_delta_step_max']
                ),
                'subsample': trial.suggest_float(
                    'subsample',
                    self.xgb_searched_params['subsample_min'],
                    self.xgb_searched_params['subsample_max']
                ),
            }

            # set hyperparameters
            params = {
                **self.xgb_fixed_params, **searched_params
            }

            # train
            model = xgb.XGBRegressor(**params)
            model.fit(
                x_train.drop(columns=self.not_features, axis=1),
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
        study.optimize(objective, n_trials=self.xgb_trials)

        print('- '* 40)

        print(f'Number of finished trials: {len(study.trials)}')

        print('- '* 40)


        best_params = {
            **self.xgb_fixed_params, **study.best_trial.params
        }
        self.best_model = xgb.XGBRegressor(**best_params)
        self.best_model.fit(
            x_train.drop(columns=self.not_features, axis=1),
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

        # save
        self.save = src.save.savexgb.Base(
            self.save_id, self.best_model, self.xgb_trials, best_params
        )
        self.save.save_feature_importance()
        self.save.save_params()
        self.save.save_model()
        self.save.save_performance(test_mae_score, test_rmse_score, test_r2_score)
        self.save.save_prediction(x_test, y_test, test_pred)
