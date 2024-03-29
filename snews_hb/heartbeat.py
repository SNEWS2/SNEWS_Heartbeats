"""
This a module to handle all heartbeat related work

"""

import os, json, click

import pandas as pd
from hop import Stream
from datetime import datetime, timedelta
import numpy as np
from . import hb_utils
from . import analyse_beats

class HeartBeat:
    """ Class to handle heartbeat message stream

        """
    def __init__(self, logs_folder=None, store=None):
        """

        :param logs_folder: `str` where the logs will be saved (default is cwd)
        :param store: `str` one of ['csv','json','both'] what format to store
        """
        # maybe hard code this and get rid of the env file?
        config = hb_utils.get_config()
        self.store = config['params']['store'] if store is None else store
        self.stash_time = float(config['params']['stash_time']) # hours
        self.delete_after = float(config['params']['delete_after']) # days
        self.heartbeat_topic = config['topics']['heartbeat']
        self.times = hb_utils.TimeStuff()
        self.hr = self.times.get_hour()
        self.date = self.times.get_date()
        self.column_names = ["Received Times", "Detector", "Stamped Times", "Latency", "Time After Last"]
        self.cache_df = pd.DataFrame(columns=self.column_names)
        if logs_folder is None:
            logs_folder = os.getcwd()
        self.logsdir = os.path.join(logs_folder, "daily_logs")
        os.makedirs(self.logsdir, exist_ok=True)
        click.secho("Storing logs in " + os.path.join(logs_folder, "daily_logs") + " folder", fg='bright_blue')


    def make_entry(self, message):
        """ Make an entry in the cache df using new message
        """
        msg = {"Received Times": message["Received Times"], "Detector": message["detector_name"]}
        stamped_time_obj = self.times.str_to_datetime(message["sent_time"], fmt="%y/%m/%d %H:%M:%S:%f")
        msg["Stamped Times"] = stamped_time_obj
        msg["Latency"] = msg["Received Times"] - msg["Stamped Times"]
        # check the last message of given detector
        detector_df = self.cache_df[self.cache_df["Detector"]==msg['Detector']]
        if len(detector_df):
            msg["Time After Last"] = msg["Received Times"] - detector_df["Received Times"].max()
        else:
            msg["Time After Last"] = timedelta(0)
        a = pd.DataFrame([msg])
        self.cache_df = pd.concat([self.cache_df, a], ignore_index = True)

    def store_beats(self):
        """ log the heartbeats, and save locally

        """
        # for now store one master csv in any case
        self.complete_csv()
        if self.store=='both':
            self.dump_csv()
            self.dump_JSON()
        elif self.store=='csv':
            self.dump_csv()
        elif self.store=='json':
            self.dump_JSON()
        else:
            raise KeyError(f"Can store either 'csv', 'json' or both, got {self.store}")

    def drop_old_messages(self):
        """ Keep the heartbeats for 24 hours
            Drop the earlier-than-24hours messages from cache

        """
        # first store a csv/JSON before dumping anything
        self.store_beats()
        curr_time = datetime.utcnow()
        existing_times = self.cache_df["Received Times"]
        del_t = (curr_time - existing_times).dt.total_seconds() /60/60
        locs = np.where(del_t < self.stash_time)[0]
        self.cache_df = self.cache_df.reset_index(drop=True).loc[locs]
        self.cache_df.sort_values(by=['Received Times'], inplace=True)

    def dump_csv(self):
        """ dump a local csv file once a day
            and keep appending the messages within that day
            into the same csv file

        """
        today = datetime.utcnow()
        today_str = datetime.strftime(today, "%y-%m-%d")
        output_csv_name = os.path.join(self.logsdir, f"{today_str}_heartbeat_log.csv")
        if os.path.exists(output_csv_name):
            self.cache_df.to_csv(output_csv_name, mode='a', header=True)
        else:
            self.cache_df.to_csv(output_csv_name, mode='w', header=True)

    def dump_JSON(self):
        """ dump a local JSON file once a day
            and keep appending the messages within that day
            into the same csv file

            Notes:
                It is risky at the moment as I overwrite each time instead
                Ideally we should compare the existing json file with the new one
                and append the new ones. If the script is re-run in the same day,
                current version would ignore the previous logs and overwrite a new one

        """
        curr_data = self.cache_df.to_json(orient='columns')
        today = datetime.utcnow()
        today_str = datetime.strftime(today, "%y-%m-%d")
        output_json_name = os.path.join(self.logsdir, f"{today_str}_heartbeat_log.json")
        # if os.path.exists(output_json_name):
        with open(output_json_name, 'w') as file:
        #     file_data = json.load(file)
        #     # append missing keys?
        # OVERWRITE INSTEAD
            file.seek(0)
            json.dump(curr_data, file, indent=4)

    def complete_csv(self):
        """ For now, also keep a csv that doesn't distinguish dates
            Append and save everything

        """
        master_csv = os.path.join(self.logsdir, f"complete_heartbeat_log.csv")
        if os.path.exists(master_csv):
            self.cache_df.to_csv(master_csv, mode='a', header=True)
        else:
            self.cache_df.to_csv(master_csv, mode='w', header=True)

    def burn_logs(self):
        """ Remove the logs after pre-decided time

        """
        today_fulldate = datetime.utcnow()
        today_str = datetime.strftime(today_fulldate, "%y-%m-%d")
        today = datetime.strptime(today_str, "%y-%m-%d")
        existing_logs = os.listdir(self.logsdir)
        if self.store == 'both':
            existing_logs = np.array([x for x in existing_logs if x.endswith('json') or x.endswith('csv')])
        else:
            existing_logs = np.array([x for x in existing_logs if x.endswith(self.store)])

        # take only dates
        dates_str = [i.split('/')[-1].split("_heartbeat")[0] for i in existing_logs]
        dates, files = [], []
        for d_str, logfile in zip(dates_str, existing_logs):
            try:
                dates.append(datetime.strptime(d_str, "%y-%m-%d"))
                files.append(logfile)
            except:
                continue

        time_differences = np.array([(date - today).days for date in dates])
        older_than_limit = np.where(np.abs(time_differences) > self.delete_after)
        files = np.array(files)
        print(f"Things will be removed; {files[older_than_limit[0]]}")

    def display_table(self):
        print("Current cache \n", self.cache_df.to_markdown())

    def sanity_checks(self):
        """ Check  the following
            - detector frequencies are reasonable
            - latencies are reasonable
            - At least one detector is operational

        :return:
        """
        analyse_beats.Analyze(self.cache_df)
        return None

    def subscribe(self):
        """ Subscribe and listen heartbeats
        """
        # Initiate hop_stream
        stream = Stream(until_eos=False)
        try:
            with stream.open(self.heartbeat_topic, "r") as s:
                print("wait please")
                for message in s:
                    print(message)
                    if '_id' not in message.keys():
                        click.secho(f"Attempted to submit a message that does not follow "
                                    f"snews_pt convention. \nThis is not supported now", fg='red')
                        continue

                    if len(message['_id'].split('_')) < 2:
                        print(f"> Received an unknown id {message['_id']}\n\t Ignoring...")
                        pass
                    if message['_id'].split('_')[1] == 'Heartbeat':
                        message["Received Times"] = datetime.utcnow()
                        print(message)
                        self.make_entry(message)
                        self.drop_old_messages()
                        self.sanity_checks()
                        self.display_table()
                        self.burn_logs()

                    else:
                        # handle only the heartbeat messages
                        continue
        except KeyboardInterrupt:
            click.secho('Done', fg='green')
