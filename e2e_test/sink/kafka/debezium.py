import sys
import json

expected_output_file = sys.argv[1]
test_output_file = sys.argv[2]
expected_data = []
test_data = []

with open(expected_output_file) as file:
    for line in file:
        # debezium sink sends k/v pair
        kv = line.split()
        key = json.loads(kv[0])
        value = json.loads(kv[1])
        # The `ts_ms` field may vary, so we delete it from the json object 
        # and assert the remaining fields equal.
        del value["payload"]["ts_ms"]
        expected_data.append(key)
        expected_data.append(value)

with open(test_output_file) as file:
    for line in file:
        kv = line.split()
        key = json.loads(kv[0])
        value = json.loads(kv[1])
        # Assert `ts_ms` is an integer here.
        assert isinstance(value["payload"]["ts_ms"], int)
        del value["payload"]["ts_ms"]
        test_data.append(key)
        test_data.append(value)

assert expected_data == test_data
