from datadownloader import DataDownloader
from Exceptions.DataDownloaderException import *
from APIConfigs import secure
import requests
import yaml
import pandas as pd
from typing import Dict, Optional, List


class MoocletDataDownloader(DataDownloader):
    """
    This is a concrete class that is designed to download all the data from the Mooclet Engine database.
    """

    def __init__(self, from_API:bool=True):
        """Initializes a MoocletDataDownloader object.
        """
        if from_API:
            self.token = secure.MOOCLET_API_TOKEN
            self.load_configs()
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
            if objects.status_code != 200:
                print("unable to request endpoint: ", str(key))
                raise RequestFailureException
            else:
                if input_factors[key] == []:
                        self.data[key] = pd.DataFrame.from_dict(objects.json()["results"])
                print(self.data[key].head(5))
        return self.data

# if __name__ == "__main__":
#     moocletDataDownloader = MoocletDataDownloader()
#     moocletDataDownloader.download_data()
