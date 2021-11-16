import datetime
import numpy as np
import pandas as pd
import requests
from APIConfigs.secure import *


def get_reward_in_timewindow(rewards, start_time, end_time):
    if rewards is not None:
        rewards = rewards.sort_values(by=["timestamp"])
        if start_time:
            rewards = rewards[rewards["timestamp"] > start_time]
        if end_time:
            rewards = rewards[rewards["timestamp"] < end_time]
        if rewards.shape[0] == 0:
            return None
        if rewards.shape[0] > 1:
            print(f"WARNING: {rewards.shape[0]}More than one reward found")
        # get last record of filtered reward
        return rewards.tail(1)
    return None


def get_key_not_empty(rewards):
    for key in rewards:
        if rewards[key] is not None and len(rewards) > 0:
            return {key: rewards[key]}
    return None


def get_learner_next_assign_t(versions, a_t, mooclet_id=None,timestamp_key="timestamp"):
    if mooclet_id:
        versions = versions[versions["mooclet"] == mooclet_id]
    assignment_times = sorted(versions[timestamp_key].unique().tolist())
    i = assignment_times.index(a_t)
    try:
        next = assignment_times[i+1]
        return next
    except IndexError:
        return None


def build_var_names_dict():
    pass


def get_valid_parameter_set(parameterhistory, parameter, a_t):
    parameterhistory = parameterhistory[parameterhistory["creation_time"] > a_t]
    if parameterhistory.shape[0] >= 1:
        return parameterhistory.head(1)["parameters"].values[0]
    else:
        return parameter["parameters"].values[0]


def get_valid_draws_set(draws, a_t, timestamp="timestamp"):
    return draws[draws[timestamp] < a_t].tail(1)


def get_policy_by_policy_id(policies, p_id):
    return policies[policies["id"] == p_id]["name"].values[0]


def get_version_by_version_id(versions, v_id):
    try:
        return versions[versions["id"] == v_id]["name"].values[0]
    except:
        return None


def get_learner_name_by_id(learners, learner_id):
    url = f"https://mooclet.canadacentral.cloudapp.azure.com/engine/api/v1/learner/{learner_id}"
    objects = requests.get(url, headers={'Authorization': f'Token {MOOCLET_API_TOKEN}'})
    return objects.json()["name"]


def get_valid_contextual_values(contextuals, timestamp):
    if contextuals is None or len(contextuals) == 0:
        return None
    else:
        result = {}
        for k , v in contextuals.items():
            v = v.sort_values(by=["timestamp"])
            result_df = v[(v["variable"] == k) & (v["timestamp"] < timestamp)].tail()
            if not result_df.empty:
                result[k] = result_df["value"].values[0]
        return result


def get_valid_groups(groups, timestamp, v_id):
    def _check_delta_time(timestamp, input_timestamp):
        return (timestamp - input_timestamp).seconds/60 < 3
    dfs = []
    for key in groups:
        value = groups[key]
        filtered = value[value["version"] == v_id]
        sorted = filtered.sort_values(by=["timestamp"])
        dfs.append(sorted)
    if len(dfs) != 0:
        groups = pd.concat(dfs)
        # try:
        print(f"DIFF: {timestamp - pd.to_datetime(groups['timestamp'])}")
        groups["diff"] = (timestamp - pd.to_datetime(groups["timestamp"])).seconds / 60
        print(list(groups["diff"]))
        return groups[(timestamp - groups["timestamp"]).seconds/60 < 3].tail()["text"].values[0]
    return None
