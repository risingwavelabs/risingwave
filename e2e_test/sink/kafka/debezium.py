import sys
import json

expected_output_file = sys.argv[1]
test_output_file = sys.argv[2]
expected_data = []
test_data = []

with open(expected_output_file) as file:
    for line in file:
        data = json.loads(line)
        # The `ts_ms` field may vary, so we delete it from the json object 
        # and assert the remaining fields equal.
        del data["payload"]["ts_ms"]
        expected_data.append(data)

with open(test_output_file) as file:
    for line in file:
        data = json.loads(line)
        # Assert `ts_ms` is an integer here.
        assert isinstance(data["payload"]["ts_ms"], int)
        del data["payload"]["ts_ms"]
        test_data.append(data)

assert expected_data == test_data
