from DataSummarizers.DataSummarizer import DataSummarizer
import pandas as pd


class MTurkDataSummarizer(DataSummarizer):

    def __init__(self):
        self.output_data = None

    def construct_summarized_df(self, groups, csv_file_path=None, dataframe=None):
        if csv_file_path:
            df = pd.read_csv(csv_file_path)
        else:
            df = dataframe
        if df is None:
            return
        if "batch_group_updated" in df.columns:
            df = df[df["batch_group_updated"].notna()]
        learner_count = df.groupby(by=groups[:-1])["learner"].count()
        df = df.groupby(by=groups).agg({'reward': ['mean', 'min', 'max','std','sem'], 'learner' : ['count']})
        df[("learner", "probAssigned")] = df[("learner", "count")]/learner_count
        self.output_data = df
        return df

    def save_data(self, path):
        if not path:
            path = "../output_files/mturk_data_summarised.csv"
        self.output_data.to_csv(path)


if __name__ == "__main__":
    summarizer = MTurkDataSummarizer()
    df = summarizer.construct_summarized_df("../output_files/mturk_datapipeline_2021-09-20 17:31:11.426449.csv", groups=["policy", "arm"])
    summarizer.save_data("../output_files/mturk_data_deployment_2_summarised.csv")
