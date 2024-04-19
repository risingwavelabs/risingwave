import subprocess
import sys

failed_cases = []
for client_image in ['presto-client', 'trino-client']:
    output = subprocess.check_output(
        ["docker", "compose", "run", client_image,
            "--execute", "select * from data_types"],
    )
    rows_cnt = len(output.splitlines())
    if rows_cnt < 1:
        failed_cases.append(client_image)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
