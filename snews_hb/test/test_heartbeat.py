"""Test publishing heartbeat tier messages."""
from snews_hb.heartbeat import HeartBeat
from SNEWS_PT.snews_pub import SNEWSTiersPublisher

def test_heartbeat_message():
    """Test with example of expected message type."""
    # Create heartbeat tier message.
    hb = SNEWSTiersPublisher.from_json("example_heartbeat.json")
    assert hb.tiernames == ['Heartbeat'], f"Expected 'Heartbeat' Tier got {hb.tiernames[0]}"
    input_hb = {"detector_name": "TEST", "machine_time":"30/01/01 12:34:48:678999", "detector_status":"ON"}
    for k,v in input_hb.items():
        assert hb.messages[0][k] == v

    try:
        hb.send_to_snews(firedrill_mode=False)
    except Exception as exc:
        print('SNEWSTiersPublisher.send_to_snews() test failed!\n')
        assert False, f"Exception raised:\n {exc}"

def test_heartbeat_server():
    pass