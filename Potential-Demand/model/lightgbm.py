"""
This module provides a class for training a LightGBM machine learning model with
hyperparameter optimization using Optuna. It reads configuration parameters from
'params.ini' and saves the trained model and related
information.

"""

import configparser
import datetime
from ast import literal_eval

import lightgbm as lgb
import optuna
import src.save.savelgbm
from lightgbm import early_stopping
from optuna.samplers import TPESampler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


class LightGBM:
    """
    This class reads configuration parameters from 'params.ini' to set up the model
    and hyperparameter optimization. It provides methods for training the model,
    optimizing hyperparameters, and saving model-related information.


    Methods:
        train(x_train, x_val, x_test, y_train, y_val, y_test, categorical_features)
            Train the LightGBM model with hyperparameter optimization using Optuna.
    """

    def __init__(self):
        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        lgbm_default = config['LGBM.DEFAULT']
        lgbm_fixed_params = config['LGBM.FIXED.PARAMS']
        lgbm_searched_params = config['LGBM.SEARCHED.PARAMS']

        self.seed = literal_eval(ini['seed'])
        self.not_features = literal_eval(ini['not_features'])
        self.lgbm_trials = literal_eval(lgbm_default['trials'])
        self.lgbm_fixed_params = {
            k: literal_eval(v)
            for k, v in lgbm_fixed_params.items()
        }
        self.lgbm_fixed_params['seed'] = self.seed
        self.lgbm_searched_params = {
            k: literal_eval(v)
            for k, v in lgbm_searched_params.items()
        }
        curr_time = str(datetime.datetime.now()).split('.', maxsplit=1)[0]
        self.save_id = curr_time.replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

    def train(
            self, x_train, x_val, x_test,
            y_train, y_val, y_test, categorical_features
        ):

        def objective(trial):
            # create dataset
            dtrain = lgb.Dataset(
                x_train.drop(
                    columns=self.not_features, axis=1
                ),
                label=y_train,
                categorical_feature=categorical_features,
                free_raw_data=False
            )
            dval = lgb.Dataset(
                x_val.drop(
                    columns=self.not_features, axis=1
                ),
                label=y_val,
                reference=dtrain,
                categorical_feature=categorical_features,
                free_raw_data=False
            )

            searched_params = {
                'lambda_l1': trial.suggest_float(
                    'lambda_l1',
                    self.lgbm_searched_params['lambda_l1_min'],
                    self.lgbm_searched_params['lambda_l1_max'],
                    log=True
                ),
                'lambda_l2': trial.suggest_float(
                    'lambda_l2',
                    self.lgbm_searched_params['lambda_l2_min'],
                    self.lgbm_searched_params['lambda_l2_max'],
                    log=True
                ),
                'learning_rate': trial.suggest_float(
                    'learning_rate',
                    self.lgbm_searched_params['learning_rate_min'],
                    self.lgbm_searched_params['learning_rate_max']
                ),
                'min_data_in_leaf': trial.suggest_int(
                    'min_data_in_leaf',
                    self.lgbm_searched_params['min_data_in_leaf_min'],
                    self.lgbm_searched_params['min_data_in_leaf_max']
                ),
                'path_smooth': trial.suggest_float(
                    'path_smooth',
                    self.lgbm_searched_params['path_smooth_min'],
                    self.lgbm_searched_params['path_smooth_max']
                ),
                'feature_fraction': trial.suggest_float(
                    'feature_fraction',
                    self.lgbm_searched_params['feature_fraction_min'],
                    self.lgbm_searched_params['feature_fraction_max']
                ),
                'bagging_fraction': trial.suggest_float(
                    'bagging_fraction',
                    self.lgbm_searched_params['bagging_fraction_min'],
                    self.lgbm_searched_params['bagging_fraction_max']
                ),
                'bagging_freq': trial.suggest_int(
                    'bagging_freq',
                    self.lgbm_searched_params['bagging_freq_min'],
                    self.lgbm_searched_params['bagging_freq_max']
                ),
                'min_child_samples': trial.suggest_int(
                    'min_child_samples',
                    self.lgbm_searched_params['min_child_samples_min'],
                    self.lgbm_searched_params['min_child_samples_max']
                ),
            }

            # set hyperparameters
            params = {**self.lgbm_fixed_params, **searched_params}

            # train
            model = lgb.train(
                params, dtrain, categorical_feature=categorical_features,
                valid_sets=[dtrain, dval], callbacks=[early_stopping(50)],
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
            objective, n_trials=self.lgbm_trials
        )

        print('- '* 40)

        print(f'Number of finished trials: {len(study.trials)}')

        print('- '* 40)


        dtrain = lgb.Dataset(
            x_train.drop(
                columns=self.not_features, axis=1
            ),
            label=y_train,
            categorical_feature=categorical_features,
            free_raw_data=False
        )
        dval = lgb.Dataset(
            x_val.drop(
                columns=self.not_features, axis=1
            ),
            label=y_val,
            reference=dtrain,
            categorical_feature=categorical_features,
            free_raw_data=False
        )
        best_params = {
            **self.lgbm_fixed_params, **study.best_trial.params
        }
        self.best_model = lgb.train(
            best_params, dtrain, categorical_feature=categorical_features,
            valid_sets=[dtrain, dval], callbacks=[early_stopping(50)],
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
        self.save = src.save.savelgbm.Base(
            self.save_id, self.best_model, self.lgbm_trials, best_params
        )
        self.save.save_feature_importance()
        self.save.save_params()
        self.save.save_model()
        self.save.save_performance(test_mae_score, test_rmse_score, test_r2_score)
        self.save.save_prediction(x_test, y_test, test_pred)
