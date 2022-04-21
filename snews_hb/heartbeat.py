"""
This a module to handle all heartbeat related work

"""

import os, json, click

import pandas as pd
from hop import Stream
from datetime import datetime
import numpy as np
import dateutil.parser
from . import hb_utils

class HeartBeat:
    """ Class to handle heartbeat message stream

        """
    def __init__(self):
        # maybe hard code this and get rid of the env file?
        self.heartbeat_topic = "kafka://kafka.scimma.org/snews.experiments-test"
        self.times = hb_utils.TimeStuff()
        self.hr = self.times.get_hour()
        self.date = self.times.get_date()
        self.stash_time = 24 # hours
        self.column_names = ["Received Times", "Detector", "Stamped Times", "Latency", "Time After Last"]
        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.cache_df.set_index("Received Times", inplace=True)

    def make_entry(self, message):
        """ Make an entry in the cache df using new message
        """
        msg = {"Received Times": message["Received Times"], "Detector": message["detector_name"]}
        stamped_time_obj = self.times.str_to_datetime(message["sent_time"], fmt="%y/%m/%d %H:%M:%S")
        msg["Stamped Times"] = stamped_time_obj
        msg["Latency"] = msg["Received Times"] - msg["Stamped Times"]
        # check the last message of given detector
        detector_df = self.cache_df[self.cache_df["Detector"]==msg['Detector']]
        if len(detector_df):
            msg["Time After Last"] = msg["Received Times"] - detector_df["Received Times"].max()
        else:
            msg["Time After Last"] = 0
        a = pd.DataFrame([msg])
        self.cache_df = pd.concat([self.cache_df, a], ignore_index = True)
        # print(f"******* \n{self.cache_df.to_markdown()}")

    def store_beats(self):
        """ log the heartbeats, and save locally

        """
        raise NotImplementedError

    def check_store(self):
        """ Check the cache and stored messages
            append and save,
        """
        self.store_beats()
        raise NotImplementedError

    def drop_old_messages(self):
        """ Keep the heartbeats for 24 hours
            Store (all?) statistics and remove earlier messages

        """
        curr_time = datetime.utcnow()
        existing_times = self.cache_df["Received Times"]

        del_t = (curr_time - existing_times).total_seconds() /60/60
        locs = np.where(del_t >= self.stash_time)[0]
        self.cache_df = self.cache_df[~locs]
        # return True if del_t >= self.stash_time else False

    def dump_csv(self):
        """ dump a local csv file once a day
            and keep appending the messages within that day
            into the same csv file

        """
        today = datetime.utcnow()
        today_str = datetime.strftime(today, "%y-%m-%d")
        output_csv_name = f"./daily_logs/{today_str}_heartbeat_log.csv"
        if os.path.exists(output_csv_name):
            self.cache_df.to_csv(output_csv_name, mode='a', header=True)
        else:
            self.cache_df.to_csv(output_csv_name, mode='w', header=True)

    def subscribe(self):
        """ Subscribe and listen heartbeats
        """
        # Initiate hop_stream
        stream = Stream(until_eos=False)
        try:
            with stream.open(self.heartbeat_topic, "r") as s:
                print("wait please")
                for message in s:
                    if '_id' not in message.keys():
                        click.secho(f"Attempted to submit a message that does not follow "
                                    f"snews_pt convention. \nThis is not supported now", fg='red')
                        continue

                    if message['_id'].split('_')[0] != 'Heartbeat':
                        message["Received Times"] = datetime.utcnow() #.strftime("%y/%m/%d %H:%M:%S:%f")
                        self.make_entry(message)
                        self.dump_csv()
                        self.drop_old_messages()

                    else:
                        # handle only the heartbeat messages
                        continue
        except KeyboardInterrupt:
            click.secho('Done', fg='green')
