"""Module for handling and downloading Kaggle datasets."""
import os
import sys
import boto3
import yaml
import argparse
from typing import List, Dict, Optional, Tuple
from configparser import ConfigParser
from kaggle.api.kaggle_api_extended import KaggleApi
from pprint import pprint
from log_handler import Logger


class KaggleHandler():
    """Class for handling all Kaggle related functions and data."""
    def __repr__(self):
        return f"KaggleHandler(username = <username>)"


    def __str__(self):
        return "KaggleHandler class that browses Kaggle and downloads datasets from https://www.kaggle.com/datasets/ "


    def __init__(self, username: str = None):
        self.logger = Logger(name="kaggle_handler", filename="database_handler.log")
        self.HOME = os.path.expanduser('~')
        self.USERNAME = username.lower().replace(" ", "_") or input("Enter your username: ").replace(" ", "_").lower()
        self.logger = Logger(name="database_handler", filename=os.path.join(self.HOME, "database_handler.log"))
        self.logger.log("Logger initialized for database_handler")
        self.args = self.get_args()
        if os.path.exists(os.path.join(self.HOME, ".aws/credentials")):
            self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
            self.SESSION = boto3.Session(profile_name=self.USERNAME)
            self.CONFIG = self.SESSION.get_credentials()[1]
        elif os.path.exists(os.path.join(self.HOME, f".database_credentials.yaml")):
            self.CONFIG_PATH = os.path.join(self.HOME, f".database_credentials.yaml")
            with open(self.CONFIG_PATH, "r") as file:
                self.CONFIG = yaml.safe_load(file)
        else:
            self.CONFIG_PATH = None
        if self.CONFIG_PATH is None:
            self.CONFIG_PATH, self.CONFIG = self.create_credentials_file()


    def get_credentials(self) -> Tuple[str, dict]:
            """
            Read the YAML credentials file and return its contents.

            :return: A dictionary containing the contents of the YAML file.
            """
            if self.CONFIG_PATH is None:
                self.create_credentials_file()
            elif ".aws" in self.CONFIG_PATH:
                config = ConfigParser()
                config.read(self.CONFIG_PATH)
                self.logger.log(f"ConfigParser file loaded from {self.CONFIG_PATH}")
                return self.CONFIG_PATH, config
            else:
                with open(self.CONFIG_PATH, "r") as file:
                    yaml_file = yaml.safe_load(file)
                    self.logger.log(f"YAML file loaded from {self.CONFIG_PATH}")
                    return self.CONFIG_PATH, yaml_file


    def get_args(self):
        arg_parse = argparse.ArgumentParser()
        arg_parse.add_argument("-a", "--all_datasets", help='Download all datasets', action="store_true")
        arg_parse.add_argument("-n", "--dataset_name", help="Name of the dataset to download", type=str)
        return arg_parse.parse_args()


    def ensure_api(self) -> None:
        """Authenticate and return a KaggleApi instance.

        Args:
            None.

        Returns:
            None
        """
        home = os.path.expanduser("~")
        cred_dir = os.path.join(home, ".kaggle")
        cred_file = os.path.join(cred_dir, "kaggle.json")
        if not (os.path.isdir(cred_dir) and os.path.isfile(cred_file)):
            raise FileNotFoundError(
                "kaggle.json not found. Create the file and and place it in ~/.kaggle/kaggle.json"
            )
        api = KaggleApi()
        api.authenticate()
        return api


    def list_dataset_files(self, dataset: str) -> List:
        """List files in a Kaggle dataset.

        Args:
            dataset (str): The name of the Kaggle dataset.

        Returns:
            List: A list of files in the dataset.
        """
        api = self.ensure_api()
        files = api.dataset_list_files(dataset=dataset)
        return files.dataset_files


    def search_kaggle_datasets(self, dataset: str, user: Optional[str] = None, max_results: int = 50, list_files: bool = False) -> List[Dict]:
        """
        Search Kaggle datasets.

        Returns a list of lightweight dicts:
        {"ref": "owner/dataset", "title": "...", "size": ..., "downloadCount": ..., "lastUpdated": "..."}
        """
        api = self.ensure_api()
        results = api.dataset_list(search=dataset, user=user, max_size=None)
        out = {}
        count = 1
        for d in results[:max_results]:
            if list_files:
                out[count] = {
                    "ref": d.ref,
                    "title": d.title,
                    "size": getattr(d, "size", None),
                    "downloadCount": d.download_count,
                    "lastUpdated": d.last_updated,
                    "ownerName": d.creator_name,
                    "subtitle": d.subtitle,
                    "url": d.url,
                    "files": [file.name for file in self.list_dataset_files(dataset=d.url.split("/datasets/")[-1])]
                }
                count += 1
            else:
                out[count] = {
                    "ref": d.ref,
                    "title": d.title,
                    "size": getattr(d, "size", None),
                    "downloadCount": d.download_count,
                    "lastUpdated": d.last_updated,
                    "ownerName": d.creator_name,
                    "subtitle": d.subtitle,
                    "url": d.url,
                }
                count += 1
        return out


    def search_kaggle_datasets_with_keyword(self, keyword: str, max_results: int = 100, max_size: int = None) -> List[Dict]:
        """Search Kaggle datasets with a specific keyword.

        Args:
            keyword (str): The keyword to search for.
            max_results (int, optional): The max number of results to retrieve. Defaults to 100.
            max_size (int, optional): Max size of the dataset to return. Defaults to None.

        Returns:
            List[Dict]: The list of datasets matching the keyword.
        """
        api = self.ensure_api()

        results = api.dataset_list(search=keyword, max_size = max_size)

        datasets = []
        for d in results[:max_results]:
            datasets.append({
                "ref": d.ref,
                "title": d.title,
                "size": getattr(d, "size", None),
                "downloadCount": d.download_count,
                "lastUpdated": d.last_updated,
                "ownerName": d.creator_name,
                "subtitle": d.subtitle,
                "url": d.url,
            })
        return datasets


    def download_kaggle_dataset(self, dataset: str = None, dataset_path: str = None, dataset_link: str = None) -> dict:
        """
        Download a Kaggle dataset.

        Args:
            dataset (str): The name of the Kaggle dataset.
            path (str, optional): The path to save the downloaded files. Defaults to None.

        Returns:
            bool: True if the dataset is downloaded successfully, False otherwise.
        """

        api = self.ensure_api()
        if dataset_path is None:
            dataset_path = self.get_credentials()[1].get('credentials').get('default_download_folder')
        if dataset_link and not self.args.dataset_name:
            dataset_ = dataset_link.split("/")[-1]
            if len(dataset_link.split("/")) != 2:
                dataset_link = dataset_link.split("/datasets/")[-1]
            dataset_path = os.path.join(self.get_credentials()[1].get('credentials').get('default_download_folder'), dataset_link)
            dataset_to_download = self.search_kaggle_datasets(dataset=dataset_)[1].get('ref')
            api.dataset_download_files(dataset=dataset_to_download, path=dataset_path, unzip=True)
            return f"Dataset {dataset_to_download} Downloaded to {dataset_path}"
        elif not self.args.dataset_name:
            if not dataset:
                raise ValueError("Please enter the name of the dataset")
        else:
            dataset = self.args.dataset_name
        if not os.path.exists(os.path.expanduser('~') + "/.kaggle") or not os.path.isfile(os.path.expanduser('~') + "/.kaggle/kaggle.json"):
            sys.stdout.write("kaggle.json not found!\n")
            self.logger.error("kaggle.json not found!")
            return False

        self.logger.log(f"Authorized {self.USERNAME}! Kaggle API authenticated successfully.")
        sys.stdout.write(f"Authorized!\n\n")
        if len(dataset.split("/")) != 2:
            dataset = dataset.split("/datasets/")[-1]
        sys.stdout.write(f"Searching for {dataset}" + "...\n")
        dataset = self.search_kaggle_datasets(dataset=dataset)
        if len(dataset) == 0:
            return "No datasets found!"
        pprint(dataset)
        choice: str = input("Enter the dataset index to download: ")
        if not os.path.exists(dataset_path):
            os.makedirs(dataset_path)
        if choice == "all" or choice == "a":
            for key in dataset:
                dataset_: str = dataset[int(key)].get('ref')
                if not os.path.exists(dataset_path + f"/{dataset_}"):
                    api.dataset_download_files(dataset_, path=dataset_path + f"/{dataset_}", unzip=True)
            return {200: f"All Datasets downloaded! to {dataset_path}"}
        else:
            for index in choice.split(", "):
                dataset_ = dataset[int(index)].get('ref')
                print(dataset_)
                if not os.path.exists(dataset_path + f"/{dataset_}"):
                    api.dataset_download_files(dataset_, path=dataset_path + f"/{dataset_}", unzip=True)
            return {200: f"Datasets downloaded to {dataset_path + f'/{dataset_}'}"}


if __name__ == "__main__":
    kaggle_handler = KaggleHandler(username='vishal_nadig')
    dataset_path = kaggle_handler.download_kaggle_dataset(dataset_link="https://www.kaggle.com/datasets/itsmonir31/hydroponics-datasets")