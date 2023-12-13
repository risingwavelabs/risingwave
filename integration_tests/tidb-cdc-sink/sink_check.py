import sys
import subprocess

relations = ['hot_hashtags']

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) FROM {rel};'
    command = f'mysql --password= -h tidb --port 4000 -u root test -e "{sql}"'
    output = subprocess.check_output(
        ["docker", "compose", "exec", "mysql", "bash", "-c", command])
    # output:
    # COUNT(*)
    # 0
    rows = int(output.decode('utf-8').split('\n')[1])
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
