from DataDownloader.mhadatadownloader import MHADataDownloader
from DataDownloader.moocletdatadownloader import MoocletDataDownloader
from Exceptions.DataPipelineException import *
from typing import List, Optional
import pandas as pd
import numpy as np
import datetime


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
        self.mooclet_data = MoocletDataDownloader().download_data()
        self.mha_data = MHADataDownloader().download_data()

    def step_1_obtain_processed_data(self, var_names):
        if not self.mooclet_data or not self.mha_data:
            raise EmptyDataFrameException
            return
        values = self.mooclet_data["value"]
        values = values.loc[values["mooclet"].isin(self.mooclet_ids)]
        # all values
        values["timestamp"] = pd.to_datetime(values["timestamp"])
        values = values.sort_values(by=["timestamp"])
        # divide timestamps into timestamps groups of 5 seconds
        values["timestamp_round"] = values["timestamp"].dt.round('120s')
        values = values.drop_duplicates(
            subset=["learner", "timestamp_rounded", "variable", "mooclet"])
        # contextual - contextual values
        contextual = {}
        for c in var_names["contextuals"]:
            contextual[c] = values[values["variable"] == c]
            # drop duplicates for easier/faster manipulation
            contextual[c] = contextual[c].drop_duplicates(subset=["timestamp_round", "learner"])
        # TODO: rewards - user respond time and reward values - change it to df instead of dict
        rewards = {}
        for r in var_names["rewards"]:
            rewards[r] = values[values["variable"] == r]
            rewards[r] = rewards[r].drop_duplicates(subset=["timestamp_round", "learner"])
        # groups - which algorithm does is the bandit assignment assigned
        groups = {}
        # (g, m_id) should be arranged in the order of study 1, 2, 3
        for i in range(len(var_names["group"])):
            g, m_id = groups[i], self.mooclet_ids[i]
            groups[m_id] = values[values["variable"] == g and values["mooclet"] == m_id]
        # sanity check
        if len(groups) != 3:
            raise Exception
        # versions (assignment)
        versions = values[values["variable"] == "version"]
        # policy parameters and policy parameters history
        current_parameters = self.mooclet_data["policyparameters"]
        current_parameters = current_parameters[current_parameters["mooclet"].isin(self.mooclet_ids)]
        policyparameterhistory = self.mooclet_data["policyparametershistory"]
        policyparameterhistory = policyparameterhistory[policyparameterhistory["mooclet"].isin(self.mooclet_ids)]
        # keep parameter, mooclet, policy and creation time
        parameters = {}
        for m_id in self.mooclet_ids:
            parameter = policyparameterhistory[policyparameterhistory["mooclet"] == m_id]
            parameter = parameter.sort_values(by=["creation_time"])
            curr_param = current_parameters[current_parameters["mooclet"] == m_id]
            curr_param["creation_time"] = np.NaN
            parameter = parameter[["mooclet", "parameters", "policy", "creation_time"]]
            curr_param = curr_param[["mooclet", "parameters", "policy", "creation_time"]]
            parameter = pd.concat(parameter, curr_param)
            parameters[m_id] = parameter
        users= self.mha_data["users"]
        timeslotset = self.mha_data["timeslotsets"]
        users = users.join(timeslotset.set_index("url"), on="timeslotset")[["annoy_id", "start_time", "end_time", "increment"]]

        self.intermediate_data = {
            "contexutals": contextual,
            "rewards": rewards,
            "groups": groups,
            "versions": versions,
            "parameters": parameters,
            "users": users
        }

    def step_2_combine_data(self, version_ids):
        rows = []
        versions = self.intermediate_data["versions"]
        learners = sorted(versions["learner"].unique())
        groups = self.intermediate_data["groups"]
        contextuals = self.intermediate_data["contextuals"]
        rewards = self.intermediate_data["rewards"]
        parameters = self.intermediate_data["parameters"]
        timeslotsets = self.intermediate_data["users"]
        for learner in learners:
            timeslotset_info = timeslotsets[timeslotsets["annoy_id"] == learner]
            if timeslotset_info.shape > 1:
                raise Exception
            start_time,end_time, increment =tuple(zip(timeslotset_info.start_time, timeslotset_info.end_time, timeslotset_info.increment))
            window_set = _get_timewindow_list(start_time, end_time, increment)
            assignment_times = sorted(
                versions[versions["learner"] == learner]["timestamps_round"].unique())
            for t in window_set:
                row_dict = {}
                row_dict["Participant"] = learner
                if t in _get_hours_int(assignment_times):
                    i = _get_hours_int(assignment_times).index(t)
                    a_t = assignment_times[i]
                    try:
                        # should be next_a_t for user, code below is wrong
                        next_a_t = assignment_times[i+1]
                    except IndexError:
                        next_a_t = None
                    row_dict["send or not"] = 1
                    row_dict["Time the first message is sent"] = a_t
                    version = versions[versions["learner"] == learner and versions["timestamps_round"] == a_t]
                    # sanity check: check whether length of version is greater than no of studies
                    if len(version) != 3:
                        raise Exception
                    row_dict["Rationale or not"] = version[version["version"].isin(version_ids["rationale"])]["text"]
                    row_dict["Link or not"] = version[version["version"].isin(version_ids["link"])]["text"]
                    row_dict["InteractionType"] = version[version["version"].isin(version_ids["interaction"])]["text"]
                    # find the contextuals with timestamps that is greatest but less than assignment time
                    activelast48 = contextuals["activelast48"]
                    row_dict["RecentEngagement"] = activelast48[activelast48["learner"] == learner and activelast48["timestamp_round"] < a_t].tail()["value"]
                    weekdayVSweekend = contextuals["weekdayorweekend"]
                    row_dict["WeekdayOrWeekEnd"] = weekdayVSweekend[weekdayVSweekend["learner"] == learner and weekdayVSweekend["timestamp_round"] < a_t].tail()["value"]
                    # rewards should be latest record before next assignement time
                    rewards = rewards[rewards["learner"] == learner]
                    if next_a_t:
                        reward = rewards[rewards["timestamp_round"] > a_t and rewards["timestamp_round"] > next_a_t].tail(1)
                    else:
                        # check if there are rewards later than assignment time
                        reward = rewards[rewards["timestamp_round"] > a_t]
                    if reward:
                        row_dict["Reward"] = reward.head(1)["value"]
                        row_dict["user respond time"] = reward.head(1)["timestamp_round"]
                    else:
                        row_dict["Reward"], row_dict["user respond time"] = np.NaN, np.NaN
                    # groups and parameters
                    count = 1
                    for m_id in self.mooclet_ids:
                        df_group = groups[m_id][
                            groups[m_id]["learner"] == learner and
                            groups[m_id]["timestamp_round"] == a_t]
                        if df_group.shape[0] > 1:
                            raise Exception
                        row_dict[f"Group {count}"] = df_group.tail()["text"]
                        # parameters - should get the first set of param_history later than a_t
                        parameter = parameters[m_id]
                        param = parameter[parameter["creation_time"] > a_t].head(1)
                        if param:
                            row_dict[f"parameters {count}"] = param["parameters"]
                        else:
                            # get the current policy
                            row_dict[f"parameters {count}"] = parameter[parameter["creation_time"].isnull()]
                        count += 1
                    rows.append(row_dict)
                else:
                    row_dict["send or not"] = 0
                    rows.append(row_dict)

        combined_df = pd.DataFrame(columns=[
            "Participant",
            "Time the first message is sent",
            "sent or not",
            "user respond time"
            "Reward",
            "Group 1",
            "Rationale or not",
            "parameters 1",
            "Group 2",
            "Link or not",
            "RecentEngagement",
            "WeekdayOrWeekEnd",
            "parameters 2",
            "Group 3",
            "InteractionType",
            "parameters 3"
        ], data=rows)
        self.output_data = combined_df.sort_values(by=["Time the first message is sent"])
        return combined_df

    def step_3_add_time_filters(self, start_time=None, end_time=None):
        if start_time or end_time:
            if not end_time:
                end_time = sorted(list(self.output_data["Time the first message is sent"].unique()))[-1]
            if not start_time:
                start_time = sorted(list(self.output_data["Time the first message is sent"].unique()))[0]
            filtered_df = self.output_data[self.output_data["Time the first message is sent"] > start_time and self.output_data["Time the first message is sent"] < end_time]
            self.output_data = filtered_df

        return filtered_df

    def step_4_save_output_data(self, name: Optional[str]=None):
        now = datetime.datetime.now()
        if not name:
            workbook_name = f"../output_files/mha_datapipeline_{now}.csv"
        else:
            workbook_name = f"../output_files/{name}.csv"
        self.output_data.to_csv(workbook_name)

    def __call__(self, var_names, version_ids, time_constraints):
        self.step_0_initialize_data_downloaders()
        self.step_1_combine_data(var_names)
        self.step_2_combine_data(version_ids)
        self.step_3_add_time_filters(*time_constraints)
        self.step_4_save_output_data()


    # def step_2


    # def __call__(self):
    #     step_1()
    #     step_2()
    #     ...


    # ###: https://www.geeksforgeeks.org/__call__-in-python/
    # datapipeline = mha_data_pipeline()
    # datapipeline()
