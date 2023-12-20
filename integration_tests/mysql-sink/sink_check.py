import subprocess
import sys

relations = ['target_count', 'data_types']

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) FROM {rel};'
    print(f"Running SQL: {sql} ON MYSQL")
    command = f'mysql -p123456 mydb -e "{sql}"'
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
