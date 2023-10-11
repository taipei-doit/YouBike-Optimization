"""
This module defines a base class 'Base' that contains methods for saving
model parameters, models, performance metrics, and predictions.

"""
import configparser
from ast import literal_eval

import src.save.save
from matplotlib import pyplot as plt


class Base:
    """
    Class: Base

    This class provides methods for managing machine learning model parameters,
    saving and loading models, recording performance metrics, and saving predictions.

    """
    def __init__(self, save_id, best_model, catb_trials, best_params):
        # config
        config = configparser.ConfigParser()
        config.read('input/params.ini')
        ini = config['INI']
        self.target = literal_eval(ini['target'])

        # file path
        self.file_path = 'output_ON' if self.target == 'on' else 'output_OFF'
        self.model_name = 'CATBoost'
        self.model_file_path = f'{self.file_path}/model'
        self.hyperparameter_file_path = f'{self.file_path}/hyperparameter/{self.model_name}'
        self.feature_importance_file_path = f'{self.file_path}/feature_importance'
        self.performance_file_path = f'{self.file_path}/performance'
        self.prediction_file_path = f'{self.file_path}/prediction'

        # content
        self.save_id = save_id
        self.best_model = best_model
        self.trials = catb_trials
        self.best_params = best_params

    def save_feature_importance(self, fi_df):
        max_num_features = 20
        fi_df = fi_df.iloc[:max_num_features,]

        fig, ax = plt.subplots(figsize =(20, 10))
        ax.barh(
            list(fi_df['feature_names']),
            list(fi_df['feature_importance']),
            height=0.2
        )

        # # Add x, y gridlines
        # ax.grid(b = True, color ='grey',
        #         linestyle ='-.', linewidth = 0.5,
        #         alpha = 0.2)

        ax.invert_yaxis()

        # Add Plot Title
        ax.set_title(
            'Feature Importance', loc ='center'
        )

        # Add annotation to bars
        for i in ax.patches:
            plt.text(
                i.get_width()+0.2, i.get_y()+0.5,
                str(round((i.get_width()), 2)),
                fontsize=10,
                fontweight='bold',
                color='grey'
            )

        fig.figure.savefig(
            f'{self.feature_importance_file_path}/{self.save_id}.png'
        )

    def save_params(self):
        src.save.save.Base().params(
            self.hyperparameter_file_path, self.save_id, self.trials, self.best_params
        )

    def save_model(self):
        src.save.save.Base().model(
            self.best_model, self.model_file_path, self.save_id
        )

    def save_performance(self, test_mae_score, test_rmse_score, test_r2_score):
        src.save.save.Base().performance(
            self.performance_file_path, self.save_id, self.model_name, self.trials,
            test_mae_score, test_rmse_score, test_r2_score
        )

    def save_prediction(self, x_test, y_test, test_pred):
        src.save.save.Base().prediction(
            self.prediction_file_path, self.save_id,
            x_test, y_test, test_pred
        )
