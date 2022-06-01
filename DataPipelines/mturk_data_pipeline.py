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
        self.mooclet_data = {
            "value": MoocletDataDownloader().get_objects_by_mooclet_ids("value", self.mooclet_ids),
            "policy": MoocletDataDownloader().get_policies(),
            "policyparametershistory": MoocletDataDownloader().get_objects_by_mooclet_ids("policyparametershistory", self.mooclet_ids),
            "policyparameters": MoocletDataDownloader().get_objects_by_mooclet_ids("policyparameters", self.mooclet_ids),
            "version-name": MoocletDataDownloader().get_objects_by_mooclet_ids("version-name", self.mooclet_ids),
        }

    def step_1_obtain_processed_data(self, var_names):
        if not self.mooclet_data:
            raise EmptyDataFrameException
            return

        mooclets = self.mooclet_ids
        values = self.mooclet_data["value"]
        values = values[values["mooclet"].isin(self.mooclet_ids)]
        values = values.sort_values(by=["learner", "mooclet", "timestamp"])
        values = values.drop_duplicates(subset=["learner", "timestamp"])
        # get arms
        versions = values[values["variable"] == "version"]
        rewards = values[values["variable"] == var_names["reward"]]
        parameters = self.mooclet_data["policyparameters"]
        parametershistory = self.mooclet_data["policyparametershistory"]
        parameters = parameters[parameters["mooclet"].isin(self.mooclet_ids)]
        if parameters.shape[0] > 1:
            parameters = parameters[parameters["policy"] == var_names["parameterpolicy"]]
        parameters = parameters.sort_values(by=["mooclet"])
        if parametershistory.shape[0] > 1:
            parametershistory = parametershistory[parametershistory["mooclet"].isin(self.mooclet_ids)]
            parametershistory = parametershistory.sort_values(by=["mooclet", "creation_time"])
        policies = self.mooclet_data["policy"]
        learners = values["learner"].dropna()
        learners = sorted(learners.unique())
        draws = {}
        for _draw in ["precesion_draw", "coef_draw"]:
                draws[_draw] = values[values["variable"] == _draw]
        contextual = {}
        for c in var_names["contextuals"]:
            if not values.loc[values["variable"] == c].empty:
                contextual[c] = values[values["variable"] == c]
        self.intermediate_data = {
            "mooclets": mooclets,
            "versions": versions,
            "rewards": rewards,
            "contextual": contextual,
            "parameters": parameters,
            "parameterhistory": parametershistory,
            "learners": learners,
            "policies": policies,
            "draws": draws,
        }
        print(self.intermediate_data)

    def step_2_combine_data(self):
        # columns:
        # 2) learner
        # 3) timestamp
        # 4) reward
        # 5) version
        # 7) parameters
        # 6) contextuals
        rewards = self.intermediate_data["rewards"]
        versions = self.intermediate_data["versions"]
        versions = versions.sort_values(by=["learner", "timestamp"])
        contextual = self.intermediate_data["contextual"]
        parameters = self.intermediate_data["parameters"]
        all_draws = self.intermediate_data["draws"]
        prec_draws = all_draws["precesion_draw"]
        coef_draws = all_draws["coef_draw"]
        parametershistory = self.intermediate_data["parameterhistory"]
        if parametershistory.shape[0] > 1:
            parametershistory = parametershistory.sort_values(by=["creation_time"])
        rows = []
        for learner in self.intermediate_data["learners"]:
            version_l = versions[versions["learner"] == learner]
            rewards_l = rewards[rewards["learner"] == learner]
            for v_id in version_l.index:
                version = version_l.loc[[v_id]][["text", "timestamp", "policy", "version"]]
                version_id = version["version"][v_id]
                row_dict = {}
                row_dict["version"] = version["text"][v_id]
                # arm_strs = row_dict["version"].lower().split(" ")
                # arm = arm_strs[arm_strs.index("arm"): arm_strs.index("arm")+2]
                # row_dict["arm"] = " ".join(arm)
                row_dict["learner"] = learner
                row_dict["assign_t"] = version["timestamp"][v_id]
                row_dict["next_assign_t"] = get_learner_next_assign_t(version_l, version["timestamp"][v_id])
                reward = get_reward_in_timewindow(rewards_l, row_dict["assign_t"], row_dict["next_assign_t"])
                row_dict["contextual"] = get_valid_contextual_values(contextual,row_dict["assign_t"])
                if reward is not None:
                    row_dict["reward"] = reward["value"].values[0]
                    row_dict["reward_time"] = reward["timestamp"].values[0]
                else:
                    row_dict["reward"] = np.NaN
                    row_dict["reward_time"] = np.NaN
                row_dict["parameters"] = get_valid_parameter_set(parametershistory, parameters, row_dict["assign_t"])
                row_dict["policy"] = get_policy_by_policy_id(self.intermediate_data["policies"], version["policy"][v_id])
                prec_draw = prec_draws[prec_draws["learner"] == learner]
                coef_draw = coef_draws[coef_draws["learner"] == learner]
                prec_draw = prec_draw[prec_draw["version"] == version_id]
                coef_draw = coef_draw[coef_draw["version"] == version_id]
                prec_draw = get_valid_draws_set(prec_draw, row_dict["assign_t"])
                coef_draw = get_valid_draws_set(coef_draw, row_dict["assign_t"])
                if prec_draw.size > 0 and coef_draw.size > 0:
                    prev_draw = prec_draw["text"].values[0]
                    coef_draw = coef_draw["text"].values[0]
                    row_dict["precision_draw_data"] = np.fromstring(prev_draw[1:-1], sep=" ")
                    row_dict["coef_draw_data"] = np.fromstring(coef_draw[1:-1], sep=" ")
                rows.append(row_dict)
        combined_df = pd.DataFrame.from_records(rows)
        # arm_early_no = sorted(list(combined_df["arm"].unique()))[0]
        # combined_df[f"is_arm{arm_early_no.split(' ')[-1]}"] = (
        #             combined_df["arm"] == arm_early_no)
        # combined_df[f"is_arm{arm_early_no.split(' ')[-1]}"] = combined_df[
        #     f"is_arm{arm_early_no.split(' ')[-1]}"].astype(int)
        # self.intermediate_data["version_json"] = f"is_arm{arm_early_no.split(' ')[-1]}"

        combined_df = combined_df.sort_values(by=["assign_t"])
        combined_df.reset_index(inplace=True, drop=True)
        combined_df = pd.concat([combined_df.drop(['parameters'], axis=1), combined_df['parameters'].apply(pd.Series)], axis=1)
        batch_groups = combined_df.groupby("variance_a").obj.set_index(
            "variance_a")
        batch_groups_list = list(batch_groups.index.unique())
        combined_df["batch_group"] = combined_df["variance_a"].apply(
            batch_groups_list.index)
        self.output_data = combined_df

    def step_3_add_parameters_updates(self, var_names):
        df = self.output_data
        df["index"] = df.index
        df["coef_mean_updated"] = repr(0)
        df["coef_cov_updated"] = repr(0)
        df["variance_a_updated"] = np.NaN
        df["variance_b_updated"] = np.NaN
        df["update_size_updated"] = np.NaN
        df["batch_group_updated"] = np.NaN
        learners = self.intermediate_data["learners"]
        obs_record = {}
        for i in df["batch_group"].unique():
            obs = df[df["batch_group"] == i]
            obs = obs.head(1)
            mean = obs["coef_mean"].values[0]
            cov = obs["coef_cov"].values[0]
            var_a = obs["variance_a"].values[0]
            var_b = obs["variance_b"].values[0]
            obs_record[var_a] = len(obs_record)
            if "update_size" not in obs:
                update_size = np.NaN
            else:
                update_size = obs["update_size"].values[0]
            if "update_record" not in obs:
                continue
            for dict in obs["update_record"].values[0]:
                round_no = var_names["reward"].split("_")[-1]
                #arm_text = self.intermediate_data["version_json"] + f"_round_{round_no}"
                # df  -> df_learner -> df_learner_arm_value -> df_reward_value
                record = df[df["batch_group"] == i]
                record = record[record["learner"] == get_learner_name_by_id(learners, int(dict["user_id"]))]
                # record = record[
                #     record[self.intermediate_data["version_json"]] == dict[
                #         arm_text]]
                record = record[record["reward"] == dict[var_names["reward"]]]
                if record.shape[0] > 1:
                    record = record.head(1)
                if record.size > 0:
                    r_id = record["index"].values[0]
                    df.loc[r_id, "coef_mean_updated"] = repr(mean)
                    df.loc[r_id, "coef_cov_updated"] = repr(cov)
                    df.loc[r_id, "variance_a_updated"] = float(var_a)
                    df.loc[r_id, "variance_b_updated"] = float(var_b)
                    df.loc[r_id, "batch_group_updated"] = obs_record[var_a]
                    df.loc[r_id, "update_size_updated"] = update_size
        df["coef_mean_updated"] = df["coef_mean_updated"].apply(eval)
        df["coef_cov_updated"] = df["coef_cov_updated"].apply(eval)
        df["coef_cov_updated"] = df["coef_cov_updated"].replace(0, np.NaN)
        df["coef_mean_updated"] = df["coef_mean_updated"].replace(0, np.NaN)
        df = df.set_index("index")
        if "coef_draw" in list(df.columns) and "precesion_draw" in list(df.columns):
            self.output_data = df.drop(columns=["coef_draw", "precesion_draw"])
        else:
            self.output_data = df

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
        print("__step__0")
        self.step_1_obtain_processed_data(var_names)
        print("__step__1")
        self.step_2_combine_data()
        print("__step__2")
        self.step_3_add_parameters_updates(var_names)
        print("__step__3")
        self.step_4_add_timestamp_filters()
        print("__step__4")
        self.step_5_save_output_data()
        self.step_6_get_summarized_data(groups=["policy", "arm"])


if __name__ == "__main__":

    mooclet_id = [315]
    var_names = {
        "reward": "modular_link_mha_prototype_linkrating",
        "parameterpolicy": 6,
        "contextuals": [
            "modular_link_mha_prototype_hadrecentactivitylast48hours",
            "modular_link_mha_prototype_isweekend",
            "modular_link_mha_prototype_timeofday",
            "modular_link_mha_prototype_highmood",
            "modular_link_mha_prototype_highenergy",
            "modular_link_mha_prototype_studyday",
            "modular_link_mha_prototype_k10",
            "modular_link_mha_prototype_averageresponsiveness",
            "modular_link_mha_prototype_averagecontentratingsoverall"
        ],
    }
    mturk_datapipeline = MturkDataPipeline(mooclet_id,False)
    mturk_datapipeline(var_names)






