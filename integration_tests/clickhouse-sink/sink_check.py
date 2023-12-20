import subprocess
import sys
from time import sleep

sleep(30)

relations = ['default.demo_test']

failed_cases = []
for rel in relations:
    sql = f"SELECT COUNT(*) FROM {rel};"
    print(f"Running SQL: {sql} ON ClickHouse")
    command = f'clickhouse-client -q "{sql}"'
    rows = subprocess.check_output(["docker", "compose", "exec", "clickhouse-server", "bash", "-c", command])
    rows = int(rows.decode('utf-8').strip())
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
