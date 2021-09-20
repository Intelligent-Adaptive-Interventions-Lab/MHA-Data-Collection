from DataDownloader.moocletdatadownloader import MoocletDataDownloader
from Exceptions.DataPipelineException import *
from DataPipelines.utils import get_valid_parameter_set, get_reward_in_timewindow, get_learner_next_assign_t, get_policy_by_policy_id
import pandas as pd
import numpy as np
import datetime
import os
pd.options.mode.chained_assignment = None


class MturkDataPipeline:

    def __init__(self, mooclet_ids):
        self.configs = ""
        self.mooclet_data = None
        self.intermediate_data = {}
        self.output_data = None
        self.mooclet_ids = mooclet_ids

    def step_0_initialize_data_downloaders(self):
        self.mooclet_data = MoocletDataDownloader().download_data()

    def step_1_obtain_processed_data(self, var_names):
        if not self.mooclet_data:
            raise EmptyDataFrameException
            return
        mooclets = self.mooclet_data["mooclet"]
        mooclets = mooclets[mooclets["id"].isin(self.mooclet_data)][["id", "name"]]
        values = self.mooclet_data["value"]
        values = values[values["mooclet"].isin(self.mooclet_ids)]
        values = values.sort_values(by=["learner", "mooclet", "timestamp"])
        # get arms
        versions = values[values["variable"] == "version"]
        rewards = values[values["variable"] == var_names["reward"]]
        print(rewards)
        # rewards = {}
        # for r in var_names["reward"]:
        #     rewards[r] = values[values["variable"] == r]
        parameters = self.mooclet_data["policyparameters"]
        parametershistory = self.mooclet_data["policyparametershistory"]
        parameters = parameters[parameters["mooclet"].isin(self.mooclet_ids)]
        parameters = parameters.sort_values(by=["mooclet"])
        parametershistory = parametershistory[parametershistory["mooclet"].isin(self.mooclet_ids)]
        parametershistory = parametershistory.sort_values(by=["mooclet", "creation_time"])
        policies = self.mooclet_data["policy"]
        learners = sorted(values["learner"].unique())
        self.intermediate_data = {
            "mooclets": mooclets,
            "versions": versions,
            "rewards": rewards,
            "parameters": parameters,
            "parameterhistory": parametershistory,
            "learners": learners,
            "policies": policies
        }

    def step_2_combine_data(self):
        # columns:
        # 2) learner
        # 3) timestamp
        # 4) reward
        # 5) version
        # 7) parameters
        rewards = self.intermediate_data["rewards"]
        versions = self.intermediate_data["versions"]
        versions = versions.sort_values(by=["learner", "timestamp"])
        parameters = self.intermediate_data["parameters"]
        parametershistory = self.intermediate_data["parameterhistory"]
        parametershistory = parametershistory.sort_values(by=["creation_time"])
        rows = []
        for learner in self.intermediate_data["learners"]:
            version_l = versions[versions["learner"] == learner]
            rewards_l = rewards[rewards["learner"] == learner]
            for v_id in version_l.index:
                version = version_l.loc[[v_id]][["text", "timestamp", "policy"]]
                row_dict = {}
                row_dict["version"] = version["text"][v_id]
                row_dict["learner"] = learner
                row_dict["assign_t"] = version["timestamp"][v_id]
                row_dict["next_assign_t"] = get_learner_next_assign_t(version_l, version["timestamp"][v_id])
                reward = get_reward_in_timewindow(rewards_l, row_dict["assign_t"], row_dict["next_assign_t"])
                if reward is not None:
                    row_dict["reward"] = reward["value"].values[0]
                    row_dict["reward_time"] = reward["timestamp"].values[0]
                else:
                    row_dict["reward"] = np.NaN
                    row_dict["reward_time"] = np.NaN
                row_dict["parameters"] = get_valid_parameter_set(parametershistory, parameters, row_dict["assign_t"])
                row_dict["policy"] = get_policy_by_policy_id(self.intermediate_data["policies"], version["policy"][v_id])
                rows.append(row_dict)
        combined_df = pd.DataFrame.from_records(rows)
        combined_df = combined_df.sort_values(by=["assign_t"])
        combined_df.reset_index(inplace=True, drop=True)
        self.output_data = combined_df

    def step_3_add_timestamp_filters(self, start_time=None, end_time=None):
        if start_time:
            self.output_data = self.output_data[self.output_data["timestamp"] > start_time]
        if end_time:
            self.output_data = self.output_data[self.output_data["timestamp"] < end_time]
        return self.output_data

    def step_4_save_output_data(self, name=None):
        now = datetime.datetime.now()
        if not os.path.isdir("../output_files/"):
            os.mkdir("../output_files/")
        if not name:
            workbook_name = f"../output_files/mturk_datapipeline_{now}.csv"
        else:
            workbook_name = f"../output_files/{name}.csv"
        self.output_data.to_csv(workbook_name)

    def __call__(self, var_names):
        self.step_0_initialize_data_downloaders()
        self.step_1_obtain_processed_data(var_names)
        self.step_2_combine_data()
        self.step_3_add_timestamp_filters()
        self.step_4_save_output_data()


if __name__ == "__main__":
    mooclet_id = [20]
    var_names = {
        "reward": "mturk_ts_reward_round_3"
    }
    mturk_datapipeline = MturkDataPipeline(mooclet_id)
    mturk_datapipeline(var_names)






