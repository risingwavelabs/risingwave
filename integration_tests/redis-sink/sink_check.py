import sys
import subprocess

relations = ['user_id', 'UserID', 'types_id', 'TYPESID']

failed_cases = []
for rel in relations:
    query = f"*{rel}*"
    print(f"Running query: scan {query} on Redis")
    output = subprocess.Popen(["docker", "compose", "exec", "redis", "redis-cli", "--scan", "--pattern", query],
                              stdout=subprocess.PIPE)
    rows = subprocess.check_output(["wc", "-l"], stdin=output.stdout)
    output.stdout.close()
    output.wait()
    rows = int(rows.decode('utf8').strip())
    print(f"{rows} keys in '*{rel}*'")
    if rows < 1:
        failed_cases.append(rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
