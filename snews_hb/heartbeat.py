"""
This a module to handle all heartbeat related work

"""

import os, json, click

import pandas as pd
from hop import Stream
from datetime import datetime
from . import hb_utils

class HeartBeat:
    """ Class to handle heartbeat message stream
        Parameters
        ----------
        env_path : `str`
            path for the environment file.
            Use default settings if not given

        """
    def __init__(self, env_path=None):
        # maybe hard code this and get rid of the env file?
        cs_utils.set_env(env_path)
        self.heartbeat_topic = os.getenv("OBSERVATION_TOPIC")
        self.times = cs_utils.TimeStuff()
        self.hr = self.times.get_hour()
        self.date = self.times.get_date()
        self.column_names = ["Received Times", "Detector", "Stamped Times", "Latency", "Time After Last"]
        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.cache_df.set_index("Received Times", in_place=True)

    def make_entry(self, message):
        """ Make an entry in the cache df using new message
        """
        msg = {}
        msg["Detector"] = message["detector_name"]
        stamped_time_obj = self.times.str_to_datetime(message["sent_time"], fmt='%y/%m/%d %H:%M:%S:%f')
        msg["Stamped Times"] = stamped_time_obj
        msg["Latency"] = message["Received Times"] - msg["Stamped Times"]
        # check the last message of given detector
        detector_df = self.cache_df.query(f"Detector=={msg['Detector']}")
        if len(detector_df):
            msg["Time After Last"] = msg["Received Times"] - detector_df["Received Times"].max()
        else:
            msg["Time After Last"] = 0
        self.cache_df = self.cache_df.append(msg, ignore_index=True).sort_index(in_place=True)


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
                for message in s:
                    if '_id' not in message.keys():
                        click.secho(f"Attempted to submit a message that does not follow "
                                    f"snews_pt convention. \nThis is not supported now", fg='red')
                        continue

                    if message['_id'].split('_')[0] != 'heartbeat':
                        message["Received Times"] = datetime.utcnow().strftime("%y/%m/%d %H:%M:%S:%f")
                        self.make_entry(message)
                        self.check_store()

                    else:
                        # handle only the heartbeat messages
                        continue
        except KeyboardInterrupt:
            click.secho('Done', fg='green')
