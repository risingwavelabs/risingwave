import sys
import json
from operator import itemgetter

expected_output_file = sys.argv[1]
test_output_file = sys.argv[2]
expected_shard = []
test_shard = []

# The result of a es request is of the form
# {took:, timeout:, _shards:, hits:}, and we only care about the result
# in hits. We also need to sort the hits since its order is random.
with open(expected_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    expected_shard = sorted(file["hits"]["hits"], key=itemgetter("_id"))

with open(test_output_file) as file:
    line = file.readline()
    file = json.loads(line)
    test_shard = sorted(file["hits"]["hits"], key=itemgetter("_id"))

if test_shard != expected_shard:
    print(test_shard)
    print(expected_shard)
assert test_shard == expected_shard
