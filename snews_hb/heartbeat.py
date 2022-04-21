"""
This a module to handle all heartbeat related work

"""

import os, json, click

import pandas as pd
from hop import Stream
from datetime import datetime
import dateutil.parser
from . import hb_utils

class HeartBeat:
    """ Class to handle heartbeat message stream
        Parameters
        ----------
        env_path : `str`
            path for the environment file.
            Use default settings if not given

        """
    def __init__(self):
        # maybe hard code this and get rid of the env file?
        self.heartbeat_topic = "kafka://kafka.scimma.org/snews.experiments-test"
        self.times = hb_utils.TimeStuff()
        self.hr = self.times.get_hour()
        self.date = self.times.get_date()
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
        detector_df = self.cache_df[self.cache_df["Detector"]==msg['Detector']] # .query(f"Detector=={msg['Detector']}")
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
                        # self.check_store()

                    else:
                        # handle only the heartbeat messages
                        continue
        except KeyboardInterrupt:
            click.secho('Done', fg='green')
