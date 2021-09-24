from DataDownloader.moocletdatadownloader import MoocletDataDownloader
from DataSummarizers.MturkDataSummarizer import MTurkDataSummarizer
from Exceptions.DataPipelineException import *
from DataPipelines.utils import get_valid_parameter_set, get_reward_in_timewindow, get_learner_next_assign_t, get_policy_by_policy_id
import pandas as pd
import numpy as np
import datetime
import os
from DataPipelines.utils import *
pd.options.mode.chained_assignment = None


class MturkDataPipeline:

    def __init__(self, mooclet_ids, summarized:bool):
        self.configs = ""
        self.mooclet_data = None
        self.intermediate_data = {}
        self.output_data = None
        self.mooclet_ids = mooclet_ids
        self.summarized = summarized

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
        values = values.drop_duplicates(subset=["learner", "timestamp"])
        # get arms
        versions = values[values["variable"] == "version"]
        rewards = values[values["variable"] == var_names["reward"]]
        # rewards = {}
        # for r in var_names["reward"]:
        #     rewards[r] = values[values["variable"] == r]
        parameters = self.mooclet_data["policyparameters"]
        parametershistory = self.mooclet_data["policyparametershistory"]
        parameters = parameters[parameters["mooclet"].isin(self.mooclet_ids)]
        if parameters.shape[0] > 1:
            parameters = parameters[parameters["policy"] == var_names["parameterpolicy"]]
        parameters = parameters.sort_values(by=["mooclet"])
        parametershistory = parametershistory[parametershistory["mooclet"].isin(self.mooclet_ids)]
        parametershistory = parametershistory.sort_values(by=["mooclet", "creation_time"])
        policies = self.mooclet_data["policy"]
        learners = sorted(values["learner"].unique())
        learners_ = self.mooclet_data["learner"]
        learners = learners_[learners_["name"].isin(learners)][["id", "name"]]
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
        learners = self.intermediate_data["learners"]
        for l_id in learners.index:
            _learner = learners.loc[[l_id]][["id", "name"]]
            learner = _learner["name"].values[0]
            id_learner =_learner["id"].values[0]
            version_l = versions[versions["learner"] == learner]
            rewards_l = rewards[rewards["learner"] == learner]
            for v_id in version_l.index:
                version = version_l.loc[[v_id]][["text", "timestamp", "policy"]]
                row_dict = {}
                row_dict["version"] = version["text"][v_id]
                arm_strs = row_dict["version"].lower().split(" ")
                arm = arm_strs[arm_strs.index("arm"): arm_strs.index("arm")+2]
                row_dict["arm"] = " ".join(arm)
                row_dict["learner_id"] = id_learner
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
        arm_early_no = sorted(list(combined_df["arm"].unique()))[0]
        combined_df[f"is_arm{arm_early_no.split(' ')[-1]}"] = (combined_df["arm"] == arm_early_no)
        combined_df[f"is_arm{arm_early_no.split(' ')[-1]}"] = combined_df[f"is_arm{arm_early_no.split(' ')[-1]}"].astype(int)
        self.intermediate_data["version_json"] = f"is_arm{arm_early_no.split(' ')[-1]}"
        combined_df = combined_df.sort_values(by=["assign_t"])
        combined_df.reset_index(inplace=True, drop=True)
        combined_df = pd.concat([combined_df.drop(['parameters'], axis=1), combined_df['parameters'].apply(pd.Series)], axis=1)
        batch_groups = combined_df.groupby("variance_a").obj.set_index(
            "variance_a")
        batch_groups_list = list(batch_groups.index.unique())
        combined_df["batch_group"] = combined_df["variance_a"].apply(
            batch_groups_list.index)
        self.output_data = combined_df
        self.output_data = combined_df.drop_duplicates(subset=["learner_id", "assign_t"])
        print(self.output_data.shape)

    def step_3_add_updated_parameters(self):
        parametershistory = self.intermediate_data["parameterhistory"]
        df = self.output_data
        df = df.reset_index()
        group_count = 0
        list_rids_assigned = {}
        rows = []
        for p_h in parametershistory.index:
            history = parametershistory.loc[[p_h]][["parameters"]]
            update_record = history["parameters"][p_h]["update_record"]
            for dict_ in update_record:
                keys = list(dict_.keys())
                record = df[df["learner_id"] == dict_[keys[0]]]
                if dict_[keys[0]] not in list_rids_assigned:
                    list_rids_assigned[dict_[keys[0]]] = []
                record = record[record[self.intermediate_data["version_json"]] == dict_[keys[1]]]
                record = record[record["reward"] == dict_[keys[2]]]
                if record.shape[0] > 1:
                    record = record.head(1)
                    if df.index.get_loc(record.iloc[0].name) in list_rids_assigned[dict_[keys[0]]]:
                        record = record.head(len(list_rids_assigned[dict_[keys[0]]])+ 1).tail(1)
                if not record.empty:
                    parameters = history["parameters"][p_h]
                    parameters = dict(("{}_updated".format(k), v) for k, v in
                                      parameters.items())
                    r_i = record["index"].values[0]
                    list_rids_assigned[dict_[keys[0]]].append(r_i)
                    row = {}
                    row["parameters_update"] = parameters
                    row["update_batch_group"] = group_count
                    row["index"] = r_i
                    rows.append(row)
            group_count += 1
        new_df = pd.DataFrame(rows)
        df = df.set_index("index").join(new_df.set_index("index"))
        df = pd.concat([df.drop(['parameters_update'], axis=1),
                                 df['parameters_update'].apply(pd.Series)],
                                axis=1)
        df = df.replace("NA", np.NaN)
        self.output_data = df.drop_duplicates(subset=["learner_id", "assign_t"])

    def step_4_add_timestamp_filters(self, start_time=None, end_time=None):
        if start_time:
            self.output_data = self.output_data[self.output_data["timestamp"] > start_time]
        if end_time:
            self.output_data = self.output_data[self.output_data["timestamp"] < end_time]
        return self.output_data

    def step_5_save_output_data(self, name=None):
        now = datetime.datetime.now()
        if not os.path.isdir("../output_files/"):
            os.mkdir("../output_files/")
        if not name:
            workbook_name = f"../output_files/mturk_datapipeline_{now}.csv"
        else:
            workbook_name = f"../output_files/{name}.csv"
        self.output_data.to_csv(workbook_name)

    def step_6_get_summarized_data(self, groups, name=None):
        if not self.summarized:
            return
        else:
            now = datetime.datetime.now()
            if not os.path.isdir("../output_files/"):
                os.mkdir("../output_files/")
            if not name:
                workbook_name = f"../output_files/mturk_datapipeline_summarized_{now}.csv"
            else:
                workbook_name = f"../output_files/{name}.csv"
            mturk_summarizer = MTurkDataSummarizer()
            mturk_summarizer.construct_summarized_df(groups=groups, dataframe=self.output_data)
            mturk_summarizer.save_data(workbook_name)

    def __call__(self, var_names):
        self.step_0_initialize_data_downloaders()
        self.step_1_obtain_processed_data(var_names)
        self.step_2_combine_data()
        self.step_3_add_updated_parameters()
        self.step_4_add_timestamp_filters()
        self.step_5_save_output_data()
        self.step_6_get_summarized_data(groups=["policy", "arm"])


if __name__ == "__main__":
    mooclet_id = [52]
    var_names = {
        "reward": "mturk_ts_reward_round_24",
        "parameterpolicy": 6
    }
    mturk_datapipeline = MturkDataPipeline(mooclet_id, True)
    mturk_datapipeline(var_names)






