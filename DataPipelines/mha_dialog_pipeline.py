from typing import List, Optional
from DataDownloader.mhadatadownloader import MHADataDownloader
import pandas as pd
import datetime
pd.options.mode.chained_assignment = None
import os

class MHADialogPipeline:
    def __init__(self, users: Optional[List[str]] = None):
        self.users = users
        self.mha_data = None
        self.intermediate_data = {}
        self.output_data = {}

    def step_0_initialize_data_downloaders(self):
        self.mha_data = MHADataDownloader().download_data()

    def step_1_get_users_urls(self):
        if not self.users:
            self.users = list(self.mha_data["users"]["url"])
        else:
            users = self.mha_data["users"]
            users = users[users['annoy_id'].isin(self.users)]
            self.users = users["url"]

    def step_2_get_dialogues_for_all_users(self):
        data_dialgoues = self.mha_data["dialogues"][
            ["user", "url", "bandit_sequence"]]
        data_bot_messages = self.mha_data["botmessages"][
            ["bot_text", "time_stamp", "sent", "dialogue_id"]]
        data_user_messages = self.mha_data["usermessages"][
            ["user_text", "time_stamp", "dialogue_id", "user", "sent",
             "invalid", "dummy"]]
        if self.users:
            data_dialogues = data_dialgoues[data_dialgoues["user"].isin(self.users)]
            data_user_messages = data_user_messages[data_user_messages["user"].isin(self.users)]
        self.users = list(set(data_dialogues["user"]))
        self.intermediate_data["dialogueXbotmessage"] = data_dialogues.join(data_bot_messages.set_index("dialogue_id"), on="url")
        data_user_messages = data_user_messages.drop(columns=["user"])
        self.intermediate_data["dialogueXusermessage"] = data_dialogues.join(data_user_messages.set_index("dialogue_id"), on="url")

    def step_3_separate_by_users(self):
        def _get_id(str):
            return str.split("/")[-2]

        for user in self.users:
            botmessages = self.intermediate_data["dialogueXbotmessage"][["user", "url", "bot_text", "time_stamp", "sent"]]
            usermessages = self.intermediate_data["dialogueXusermessage"][["user", "url", "user_text", "time_stamp", "sent"]]
            botmessages = botmessages[botmessages["user"] == user]
            botmessages["fromBot"] = True
            botmessages["time_stamp"] = pd.to_datetime(botmessages["time_stamp"])
            botmessages["user_id"] = botmessages['user'].apply(lambda x: _get_id(x))
            botmessages["dialogue_id"] = botmessages["url"].apply(lambda x: _get_id(x))
            usermessages = usermessages[usermessages["user"] == user]
            usermessages["time_stamp"] = pd.to_datetime(
                usermessages["time_stamp"])
            usermessages["fromBot"] = False
            usermessages["user_id"] = usermessages["user"].apply(lambda x: _get_id(x))
            usermessages["dialogue_id"] = usermessages["url"].apply(lambda x: _get_id(x))
            botmessages = botmessages.rename(columns={"url": "dialogue", "bot_text": "text"})
            usermessages = usermessages.rename(columns={"url": "dialogue", "user_text": "text"})
            df = pd.concat([botmessages, usermessages], sort=True)
            self.output_data[user] = df[["user_id", "dialogue_id", "text", "fromBot", "time_stamp"]]

    def step_4_sort_by_timestamps(self):
        for user in self.users:
            self.output_data[user] = self.output_data[user].sort_values(by=["time_stamp"])

    def step_5_save_output_file(self, name: Optional[str]=None):
        now = datetime.datetime.now()
        if not name:
            workbook_name = f"../output_files/mha_dialogs_{now}.xlsx"
        else:
            workbook_name = f"../output_files/{name}.xlsx"
        if not os.path.isdir("../output_files/"):
            os.mkdir("../output_files/")
        writer = pd.ExcelWriter(workbook_name)
        for user in self.users:
            df_user = self.output_data[user]
            user_id = user.split("/")[-2]
            df_user["time_stamp"] = df_user['time_stamp'].dt.tz_localize(None)
            df_user.to_excel(writer, sheet_name=f"User_{user_id}")
        writer.save()

    def __call__(self):
        self.step_0_initialize_data_downloaders()
        self.step_1_get_users_urls()
        self.step_2_get_dialogues_for_all_users()
        self.step_3_separate_by_users()
        self.step_4_sort_by_timestamps()
        self.step_5_save_output_file()


if __name__ == "__main__":
    mha_dialog_pipeline = MHADialogPipeline()
    mha_dialog_pipeline()
