from abc import ABC, abstractmethod
from typing import List, Optional


class DataSummarizer(ABC):
    @abstractmethod
    def construct_summarized_df(self, csv_path: str, groups: List[str]):
        raise NotImplementedError

    @abstractmethod
    def save_data(self, path: Optional[str]=None):
        raise NotImplementedError
