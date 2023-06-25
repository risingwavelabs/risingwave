import sys
import json

expected_output_file = sys.argv[1]
test_output_file = sys.argv[2]
expected_shard = []
test_shard = []

with open(expected_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    expected_shard = sorted(file["hits"].items())

with open(test_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    test_shard = sorted(file["hits"].items())

if test_shard != expected_shard:
    print(test_shard)
    print(expected_shard)
assert test_shard == expected_shard
