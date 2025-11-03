"""Module for managing Machine Learning tasks."""
import os
import numpy as np
import sys
from log_handler import Logger


class MLManager():
    """Class for handling all Machine Learning related functions and data."""
    def __init__(self):
        self.logger = Logger(name="ml_manager", filename=os.path.join(self.HOME, "database_handler.log"))


    def linear_regression(self, ):
        """Linear Regression model.
        y = mx + b
        m = slope
        b = y-intercept
        x = input feature
        y = output feature
        """
        self.m = 0
        self.b = 0
        self.x = np.array([1, 2, 3, 4, 5])
        self.learning_rate = 0.01
        self.y = self.m * self.x + self.b
        self.y_true = np.array([3, 5, 7, 9, 11])
        self.n = len(self.x)


    def capture_data_from_user(self, ):
        """Capture data from user.
        """


    def choose_model_parameters(self, dataset: str = None, dataset_path: str = None):
        """Choose model parameters.

        Args:
            dataset (str, optional): The name of the dataset to train the model on. Defaults to None.
            dataset_path (str, optional): The path of the dataset. Defaults to None.
        """
        credentials = self.get_credentials()
        dataset_path = credentials['default_download_folder']
        dataset_path = self.download_kaggle_dataset(dataset=dataset, dataset_path=dataset_path)
        for file in os.listdir(dataset_path[200]):
            sys.stdout.write(file)


    def train_machine_learning_model(self, dataset: str = None, dataset_path: str = None):
        """Train a machine learning modelf from a list of machine learning models.

        Args:
            dataset (str, optional): The name of the dataset to train the model on. Defaults to None.
            dataset_path (str, optional): The path of the dataset. Defaults to None.
        """
        pass