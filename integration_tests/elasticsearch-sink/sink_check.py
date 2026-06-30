import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, report_failures

relations = ['test', 'test_types']
versions = ['7', '8']

failed_cases = []
for rel in relations:
    query = f'curl -XGET -u elastic:risingwave "http://localhost:9200/{rel}/_count"  -H "Content-Type: application/json"'
    for v in versions:
        es = f'elasticsearch{v}'
        print(f"Running Query: {query} on {es}")
        output = docker_compose_exec(es, query)
        counts = json.loads(output)['count']
        print(f"{counts} counts in {es}_{rel}")
        if counts < 1:
            failed_cases.append(f"{es}_{rel}")

report_failures(failed_cases)
