import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def doris_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("mysql", f'mysql -uroot -P9030 -hfe -e "{sql}"')
    return int(output.split('\n')[1])


failed = check_row_counts(['demo.demo_bhv_table', 'demo.upsert_table'], doris_count, "Doris")

# update data
subprocess.run(["docker", "compose", "exec", "postgres", "bash", "-c",
                 "psql -h risingwave-standalone -p 4566 -d dev -U root -f update_delete.sql"], check=True)

# delete check
rows = doris_count("demo.upsert_table")
if rows != 3:
    print(f"rows expected 3, get {rows}")
    failed.append("delete demo.upsert_table")

# update check
sql = "SELECT target_id FROM demo.upsert_table WHERE user_id = 3;"
output = docker_compose_exec("mysql", f'mysql -uroot -P9030 -hfe -e "{sql}"')
target_id = int(output.split('\n')[1])
if target_id != 30:
    print(f"target_id expected 30, get {target_id}")
    failed.append("update demo.upsert_table")

report_failures(failed)
