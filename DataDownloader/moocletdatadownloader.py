from DataDownloader.datadownloader import DataDownloader
from Exceptions.DataDownloaderException import *
from APIConfigs import secure
import requests
import yaml
import pandas as pd
from typing import Dict, Optional, List
pd.options.mode.chained_assignment = None


class MoocletDataDownloader(DataDownloader):
    """
    This is a concrete class that is designed to download all the data from the Mooclet Engine database.
    """

    def __init__(self, from_API:bool=True, filters=None):
        """Initializes a MoocletDataDownloader object.
        """
        if from_API:
            self.token = secure.MOOCLET_API_TOKEN
            self.load_configs()
        self.filter = filters
        self.data = {}

    def load_configs(self):
        """Loads Mooclet API configs, including all the base url and the endpoints.
        """
        self.configs = yaml.safe_load(open("../APIConfigs/mooclet.yaml"))

    def download_data(self, input_factors: Optional[Dict[str, List[str]]] = None) -> Dict[str, pd.DataFrame]:
        """Downloads all the data from the Mooclet Engine database.

        :param input_factors: The list of tables that we are going to extract data from
        if it is not None, otherwise, all data will be extracted
        :type input_factors: Dict[str,  List[str]], optional
        :return: a dictionary of dataframes that contains all the downloaded data
        :rtype: Dict[str, pd.DataFrame]
        """
        if not self.configs:
            raise ConfigsNotSetException
            return
        if not input_factors:
            input_factors = {key: [] for key in self.configs["endpoints"]}
        for key in input_factors.keys():
            url = self.configs["baseURL"]+ key
            objects = requests.get(url, headers={'Authorization': f'Token {self.token}'})
            count = objects.json()["count"]
            if objects.status_code != 200:
                print("unable to request endpoint: ", str(key))
                raise RequestFailureException
            else:
                rows = 0
                page = 1
                list_dfs = []
                while rows < count:
                    objects = requests.get(url+f"?page={page}", headers={'Authorization': f'Token {self.token}'})
                    data = objects.json()["results"]
                    list_dfs.append(pd.DataFrame.from_records(data))
                    page += 1
                    rows += len(data)
                self.data[key] = pd.concat(list_dfs)
                if input_factors[key]:
                    self.data[key] = self.data[key][input_factors[key]]
        return self.data


if __name__ == "__main__":
    moocletDataDownloader = MoocletDataDownloader()
    data = moocletDataDownloader.download_data()
    values = data["value"]
    print(values.shape)
    values = values[values["mooclet"].isin([10, 11, 12])]
    print(values.shape)
    values["timestamp_real"] = pd.to_datetime(values["timestamp"])
    values = values.sort_values(by=["learner", "timestamp_real"])
    # # divide timestamps into timestamps groups of 5 seconds
    values["timestamp_rounded"] = values["timestamp_real"].dt.round('60s')
    values = values.drop_duplicates(subset=["learner", "timestamp_rounded", "variable", "mooclet"])
    values = values.set_index("timestamp_rounded")
    values.to_csv("../sample_files/mooclet_test_values.csv")
