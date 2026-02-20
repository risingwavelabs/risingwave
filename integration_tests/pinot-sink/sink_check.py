import subprocess
import sys
import json
import time

relations = ["orders"]


def run_pinot_sql(sql: str):
    command = f'{{"sql":"{sql}"}}'
    output = subprocess.check_output([
        "docker",
        "compose",
        "exec",
        "-T",
        "pinot-broker",
        "curl",
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "-d",
        command,
        "http://localhost:8099/query/sql",
    ])
    return json.loads(output.decode("utf-8"))


failed_cases = []
for rel in relations:
    sql = f"SELECT COUNT(*) as count FROM {rel}"
    print(f"Running SQL: {sql} on Pinot")
    rows = run_pinot_sql(sql)["resultTable"]["rows"][0][0]
    print(rows)
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

# update data
subprocess.run(
    [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "bash",
        "-c",
        "psql -h risingwave-standalone -p 4566 -d dev -U root -f update.sql",
    ],
    check=True,
)

last_status = "<empty>"
for _ in range(30):
    sql = "SELECT status, updated_at FROM orders WHERE id = 1 ORDER BY updated_at DESC LIMIT 1"
    result = run_pinot_sql(sql)
    rows = result.get("resultTable", {}).get("rows", [])
    if rows:
        last_status = rows[0][0]
        print(f"Latest status in Pinot for id=1: {rows[0]}")
        if last_status == "PROCESSING":
            break
    time.sleep(2)

if last_status != "PROCESSING":
    failed_cases.append(f"expected PROCESSING, get {last_status}")

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
