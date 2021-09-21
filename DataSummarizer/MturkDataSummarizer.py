import pandas as pd
from DataSummarizer.DataSummarizer import DataSummarizer
import pandas as pd


class MTurkDataSummarizer(DataSummarizer):

    def __init__(self):
        self.output_data = None

    def construct_summarized_df(self, csv_file_path, groups):
        df = pd.read_csv(csv_file_path)
        learner_count = df.groupby(by=groups[:-1])["learner"].shape[0]
        df = df.groupby(by=groups).agg({'reward': ['mean', 'min', 'max','std','sem'], 'learner' : ['count']})
        df[("learner", "probAssigned")] = df[("learner", "count")]/learner_count
        self.output_data = df
        return df

    def save_data(self, path):
        if not path:
            path = "../output_files/mturk_data_summarised.csv"
        df.to_csv(path)


if __name__ == "__main__":
    summarizer = MTurkDataSummarizer()
    df = summarizer.construct_summarized_df("../output_files/mturk_datapipeline_2021-09-20 17:31:11.426449.csv", groups=["policy", "arm"])
    summarizer.save_data("../output_files/mturk_data_deployment_2_summarised.csv")
