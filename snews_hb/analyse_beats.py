"""
    Script to analyze collected heartbeats
    If needed, take actions e.g. send warnings
"""

import pandas as pd
import numpy as np

class Analyze:
    def __init__(self, data):
        self.df = data
        self.columns = data.columns.to_list()

    def control_operational(self):
        """ Check how many detectors are operational
            Send back a warning to all if number is < 1

        :return:
        """
        raise NotImplementedError

    def check_consistencies(self):
        """ Check the last N heartbeats from each detector
            compare the heartbeat intervals
            sent time - machine time deviations
            sent time - received time deviations

        :return:
        """
        raise NotImplementedError

    def check_detector_schedules(self):
        """ Track scheduled downtimes
            communicate with other detectors

        :return:
        """