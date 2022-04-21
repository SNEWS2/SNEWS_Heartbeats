"""Test publishing heartbeat tier messages."""
from snews_hb.heartbeat import HeartBeat
from SNEWS_PT.snews_pub import SNEWSTiersPublisher

import os, json

def test_coincidence_expected():
    """Test with example of expected message type."""
    # Create heartbeat tier message.
    with open("example_heartbeat.json") as json_file:
        data = json.load(json_file)

    SNEWSTiersPublisher.from_json("example_heartbeat.json")
    coin = HeartBeat(detector_name='KamLAND', neutrino_time='12/06/09 15:31:08:1098', p_value=0.4)
    # Check that message has expected structure.
    assert coin.tiernames == ['CoincidenceTier']