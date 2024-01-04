import json
import subprocess
import sys

relations = ['test']

failed_cases = []
versions = ['7', '8']
for rel in relations:
    query = f'curl -XGET -u elastic:risingwave "http://localhost:9200/{rel}/_count"  -H "Content-Type: application/json"'
    for v in versions:
        es = 'elasticsearch{}'.format(v)
        print(f"Running Query: {query} on {es}")
        counts = subprocess.check_output(["docker", "compose", "exec", es, "bash", "-c", query])
        counts = json.loads(counts)['count']
        print("{} counts in {}_{}".format(counts, es, rel))
        if counts < 1:
            failed_cases.append(es + '_' + rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
