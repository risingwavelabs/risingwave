import subprocess
import sys
import json

relations = ["orders"]

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) as count FROM {rel}'
    print(f"Running SQL: {sql} on Pinot")
    command = f'{{"sql":"{sql}"}}'
    rows = subprocess.check_output(["docker", "compose", "exec", "pinot-broker", "curl", "-H", "Content-Type: application/json", "-X", "POST", "-d", command, "http://localhost:8099/query/sql"])
    rows = json.loads(rows.decode('utf-8'))['resultTable']['rows'][0][0]
    print(rows)
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

# update data
subprocess.run(["docker", "compose", "exec", "postgres", "bash", "-c", "psql -h risingwave-standalone -p 4566 -d dev -U root -f update.sql"])

sql = f'SELECT status FROM orders WHERE id = 1'
command = f'{{"sql":"{sql}"}}'
output = subprocess.check_output(["docker", "compose", "exec", "pinot-broker", "curl", "-H", "Content-Type: application/json", "-X", "POST", "-d", command, "http://localhost:8099/query/sql"])
output = json.loads(output.decode('utf-8'))['resultTable']['rows'][0][0]
if output != "PROCESSING":
    failed_cases.append(f"expected PROCESSING, get {output}")

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
