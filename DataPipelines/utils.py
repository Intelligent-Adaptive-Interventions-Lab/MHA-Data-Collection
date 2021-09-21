import datetime
import numpy as np


def get_reward_in_timewindow(rewards, start_time, end_time):
    rewards = rewards.sort_values(by=["timestamp"])
    if start_time:
        rewards = rewards[rewards["timestamp"] > start_time]
    if end_time:
        rewards = rewards[rewards["timestamp"] < end_time]
    if rewards.shape[0] == 0:
        return None
    if rewards.shape[0] > 1:
        print("More than one reward found")
    # get last record of filtered reward
    return rewards.tail(1)


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


def get_policy_by_policy_id(policies, p_id):
    return policies[policies["id"] == p_id]["name"].values[0]


