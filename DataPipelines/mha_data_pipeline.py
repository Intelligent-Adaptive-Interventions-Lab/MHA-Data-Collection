from DataDownloader.mhadatadownloader import MHADataDownloader
from DataDownloader.moocletdatadownloader import MoocletDataDownloader
from Exceptions.DataPipelineException import *
from typing import List, Optional
import pandas as pd
import numpy as np
import datetime
from DataPipelines.utils import *
pd.options.mode.chained_assignment = None
import collections


def _get_timewindow_list(start_time, end_time, increment):
    start_time = int(start_time.split(":")[0])
    end_time = int(end_time.split(":")[0])
    window_list = []
    time = start_time
    while time < end_time:
        window_list.append(time)
        time += increment
    return window_list


def _get_hours_int(times):
    hours = []
    for t in times:
        hours.append(t.split(" ")[-1].split(":")[0])
    return hours


class MHADataPipeline:
    def __init__(self, mooclet_ids):
        self.configs = ""
        self.mooclet_data = None
        self.mha_data = None
        # the mooclet objects that we are going to extract data from
        self.mooclet_ids = mooclet_ids
        self.intermediate_data = {}
        self.output_data = {}

    def step_0_initialize_data_downloaders(self):
        self.mooclet_data = {
            "value": MoocletDataDownloader().get_objects_by_mooclet_ids("value", self.mooclet_ids),
            "policy": MoocletDataDownloader().get_policies(),
            "policyparametershistory": MoocletDataDownloader().get_objects_by_mooclet_ids("policyparametershistory", self.mooclet_ids),
            "policyparameters": MoocletDataDownloader().get_objects_by_mooclet_ids("policyparameters", self.mooclet_ids),
            "version-name": MoocletDataDownloader().get_objects_by_mooclet_ids("version-name", self.mooclet_ids)
        }
        self.mha_data = MHADataDownloader().download_data()

    def step_1_obtain_processed_data(self, var_names):
        if not self.mooclet_data or not self.mha_data:
            raise EmptyDataFrameException
            return
        base_values = self.mooclet_data["value"]
        base_values = base_values.loc[base_values["mooclet"].isin(self.mooclet_ids)]
        for m_id in self.mooclet_ids:
            values = base_values[base_values["mooclet"] == m_id]
            versions = values[values["variable"] == "version"]
            versions = versions.reset_index()
            contextual = {}
            for c in var_names["contextuals"]:
                if not values.loc[values["variable"] == c].empty:
                    contextual[c] = values[values["variable"] == c]
            rewards = None
            for r in var_names["rewards"]:
                if not values[values["variable"] == r].empty:
                    rewards = values[values["variable"] == r]
            policies = self.mooclet_data["policy"]
            parameters = self.mooclet_data["policyparameters"]
            parameters_history = self.mooclet_data["policyparametershistory"]
            parameters = parameters.loc[parameters["mooclet"] == m_id]
            parameters_history = parameters_history.loc[parameters_history["mooclet"] == m_id]
            versions_names = self.mooclet_data["version-name"]
            versions_names = versions_names.loc[versions_names["mooclet"] == m_id]
            intermediate_data = {
                "rewards": rewards,
                "contextual": contextual,
                "policies": policies,
                "parameters": parameters,
                "parameters_history": parameters_history,
                "versions": versions,
                "versions_name": versions_names,
            }
            data = {}
            for g in var_names["group_var"]:
                if not values[values["variable"] == g].empty:
                    data[g] = values[values["variable"] == g]
            intermediate_data["groups"] = data
            self.intermediate_data[m_id] = intermediate_data

    def step_2_combine_data(self):
        output_data = {}
        count = 1
        for m_id in list(self.intermediate_data.keys()):
            rows = []
            rewards = self.intermediate_data.get(m_id).get("rewards")
            contextual = self.intermediate_data.get(m_id).get("contextual")
            #print(f"contextual:{contextual}")
            policies = self.intermediate_data.get(m_id).get("policies")
            parameters = self.intermediate_data.get(m_id).get("parameters")
            parameters_history = self.intermediate_data.get(m_id).get("parameters_history")
            versions = self.intermediate_data.get(m_id).get("versions")
            versions_name = self.intermediate_data.get(m_id).get("versions_name")
            versions = versions.sort_values(by=["timestamp"])
            for v_id, row_data in versions.iterrows():
                row_dict = collections.OrderedDict()
                version = versions.loc[[v_id]][["learner", "timestamp", "policy", "version"]]
                if version is None or version["learner"][v_id] is None:
                    continue
                #print(F"VERSION{version}")
                row_dict["study"] = f"Study {count}"
                row_dict["learner"] = version["learner"][v_id]
                row_dict["timestamp"] = version["timestamp"][v_id]
                #print(f"Policy ID: {version['policy'][v_id]}")
                row_dict["policy"] = get_policy_by_policy_id(policies, version["policy"][v_id])
                row_dict["arm"] = get_version_by_version_id(versions_name, version["version"][v_id])
                row_dict["contextual"] = get_valid_contextual_values(contextual, row_dict["timestamp"])
                #print(versions[versions["learner"] == row_dict["learner"]])
                next_version_timestamp = get_learner_next_assign_t(versions[versions["learner"] == row_dict["learner"]], row_dict["timestamp"])
                row_dict["next_assign_t"] = next_version_timestamp
                row_dict["reward_name"],row_dict["reward"], row_dict["user respond time"] = None, None, None
                reward = get_reward_in_timewindow(rewards[rewards["learner"] == row_dict["learner"]], row_dict["timestamp"], next_version_timestamp)
                if reward is not None:
                    row_dict["reward_name"] = reward["variable"].values[0]
                    row_dict["reward"] = reward["value"].values[0]
                    row_dict["user respond time"] = reward["timestamp"].values[0]
                row_dict["parameters"] = get_valid_parameter_set(
                    parameters_history, parameters, row_dict["timestamp"])
                row_dict["group"] = None
                # if row_dict["policy"] != "uniform_random":
                #     if "groups" in self.intermediate_data[m_id]:
                #         row_dict["group"] = get_valid_groups(self.intermediate_data.get(m_id).get("groups"), pd.Timestamp(row_dict["timestamp"]), version["version"][v_id], row_dict["learner"])
                rows.append(row_dict)
            combined_df = pd.DataFrame.from_records(rows)
            combined_df = combined_df.sort_values(by=["timestamp"])
            combined_df.reset_index(inplace=True, drop=True)
            combined_df = pd.concat([combined_df.drop(['parameters'], axis=1),
                                     combined_df['parameters'].apply(
                                         pd.Series)], axis=1)
            combined_df = pd.concat([combined_df.drop(['contextual'], axis=1),
                                     combined_df['contextual'].apply(
                                         pd.Series)], axis=1)
            if "variance_a" in combined_df.columns:
                batch_groups = combined_df.groupby("variance_a").obj.set_index(
                    "variance_a")
                batch_groups_list = list(batch_groups.index.unique())
                combined_df["batch_group"] = combined_df["variance_a"].apply(
                    batch_groups_list.index)
            output_data[m_id] = combined_df
            count += 1
        dfs = list(output_data.values())
        big_df = pd.concat(dfs, sort=False)
        big_df = big_df.sort_values(by=["timestamp"])
        big_df.reset_index(inplace=True, drop=True)
        self.output_data = big_df

    def step_3_add_time_filters(self, timeconstraints):
        if timeconstraints:
            start_time, end_time = timeconstraints[0], timeconstraints[1]
            if not end_time:
                end_time = sorted(list(self.output_data["timestamp"].unique()))[-1]
            if not start_time:
                start_time = sorted(list(self.output_data["timestamp"].unique()))[0]
            filtered_df = self.output_data[self.output_data["timestamp"] > start_time and self.output_data["Time the first message is sent"] < end_time]
            self.output_data = filtered_df
            return filtered_df
        return

    def step_4_save_output_data(self, name: Optional[str]=None):
        now = datetime.datetime.now()
        if not name:
            workbook_name = f"../output_files/mha_datapipeline_{now}.csv"
        else:
            workbook_name = f"../output_files/{name}.csv"
        self.output_data.to_csv(workbook_name)

    def __call__(self, var_names, time_constraints=None):
        self.step_0_initialize_data_downloaders()
        self.step_1_obtain_processed_data(var_names)
        self.step_2_combine_data()
        self.step_3_add_time_filters(time_constraints)
        self.step_4_save_output_data()


if __name__ == "__main__":
    var_names = {
        "rewards": ["mha_link_reward_wave_3", "mha_rationale_reward_wave_3", "mha_interaction_reward_wave_3"],
        "contextuals": ["activelast48_wave_3", "weekdayorweekend_wave_3"],
        "group_var": ["UR_or_TS", "UR_or_TSCONTEXTUAL"]
    }
    mooclet_ids = [89,90 ,92 ]
    mha_data_pipeline = MHADataPipeline(mooclet_ids)
    mha_data_pipeline(var_names)

