from DataDownloader.datadownloader import DataDownloader
from Exceptions.DataDownloaderException import *
from APIConfigs import secure
import requests
import yaml
import pandas as pd
from typing import Dict, Optional, List


class MHADataDownloader(DataDownloader):
    """
        This is a concrete class that is designed to download all the data from the Mooclet Engine database.
        """

    def __init__(self, from_API: bool = True):
        """Initializes a MoocletDataDownloader object.
        """
        if from_API:
            self.token = secure.MHA_API_TOKEN
            self.load_configs()
        self.data = {}

    def load_configs(self):
        """Loads Mooclet API configs, including all the base url and the endpoints.
        """
        self.configs = yaml.safe_load(open("../APIConfigs/mha_controller.yaml"))

    def download_data(self,
                      input_factors: Optional[Dict[str, List[str]]] = None) -> \
    Dict[str, pd.DataFrame]:
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
            url = self.configs["baseURL"] + key
            objects = requests.get(url, headers={
                'Authorization': f'Token {self.token}'})
            if objects.status_code != 200:
                print("unable to request endpoint: ", str(key))
                raise RequestFailureException
            else:
                self.data[key] = pd.DataFrame.from_dict(objects.json())
                if input_factors[key]:
                    self.data[key] = self.data[key][input_factors[key]]
        return self.data


if __name__ == "__main__":
    mhadatadownloader = MHADataDownloader()
    data = mhadatadownloader.download_data()
    data_dialgoues = data["dialogues"][["user", "url", "bandit_sequence"]]
    data_bot_messages = data["botmessages"][["bot_text", "time_stamp", "sent", "dialogue_id"]]
    data_user_messages = data["usermessages"][["user_text", "time_stamp", "dialogue_id", "sent", "invalid", "dummy"]]
    df = data_dialgoues.join(data_bot_messages.set_index("dialogue_id"), on="url")
    #df.to_csv("../sample_files/mha_test_data.csv")
    df = data_dialgoues.join(data_user_messages.set_index("dialogue_id"), on="url")
    df.to_csv("../sample_files/mha_test_data.csv")


