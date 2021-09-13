from abc import ABC, abstractmethod
from typing import Optional, Dict

class DataDownloader(ABC):
    @abstractmethod
    def load_configs():
        """
        Load API configs.
        """
        raise NotImplementedError

    @abstractmethod
    def download_data(input_factors= Optional[Dict[str, str]]):
        """
        Download data from url specified in loaded configs, and from table with their names listed in
        input_factors.
        
        :param input_factors: The list of tables that we are going to extract data from
        if it is not None, otherwise, all data will be extracted
        :type input_factors: Dict[str, str], optional
        """
        raise NotImplementedError
