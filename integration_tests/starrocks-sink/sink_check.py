import subprocess
import sys

relations = ['demo.demo_primary_table','demo.demo_duplicate_table','demo.demo_aggregate_table','demo.demo_unique_table', 'demo.upsert_table']

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) FROM {rel};'
    print(f"Running SQL: {sql} ON Starrocks")
    command = f'mysql -uroot -P9030 -h127.0.0.1 -e "{sql}"'
    output = subprocess.check_output(
        ["docker", "compose", "exec", "starrocks-fe", "bash", "-c", command])
    # output:
    # COUNT(*)
    # 0
    rows = int(output.decode('utf-8').split('\n')[1])
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

# update data
subprocess.run(["docker", "compose", "exec", "postgres", "bash", "-c", "psql -h risingwave-standalone -p 4566 -d dev -U root -f update_delete.sql"], check=True)

# delete
sql = f"SELECT COUNT(*) FROM demo.upsert_table;"
command = f'mysql -uroot -P9030 -h127.0.0.1 -e "{sql}"'
output = subprocess.check_output(
    ["docker", "compose", "exec", "starrocks-fe", "bash", "-c", command])
rows = int(output.decode('utf-8').split('\n')[1])
print(f"{rows} rows in demo.upsert_table")
if rows != 3:
    print(f"rows expected 3, get {rows}")
    failed_cases.append("delete demo.upsert_table")

# update
sql = f"SELECT target_id FROM demo.upsert_table WHERE user_id = 3;"
command = f'mysql -uroot -P9030 -h127.0.0.1 -e "{sql}"'
output = subprocess.check_output(
    ["docker", "compose", "exec", "starrocks-fe", "bash", "-c", command])
id = int(output.decode('utf-8').split('\n')[1])
if id != 30:
    print(f"target_id expected 30, get {id}")
    failed_cases.append("update demo.upsert_table")

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
