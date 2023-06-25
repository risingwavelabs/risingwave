import sys
import json

expected_output_file = sys.argv[1]
test_output_file = sys.argv[2]
expected_shard = []
test_shard = []

with open(expected_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    expected_shard = file["hits"]

with open(test_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    test_shard = file["hits"]

assert sorted(expected_shard.items()) == sorted(test_shard.items())
